import 'dart:async';
import 'dart:convert';
import 'package:http/http.dart' as http;
import 'types.dart';

typedef AllexBearerTokenProvider = FutureOr<String?> Function();
typedef AllexBearerTokenRefresher =
    FutureOr<bool> Function(AllexAuthFailure failure);

class AllexAuthFailure {
  final Uri uri;
  final String module;
  final String method;
  final int statusCode;

  const AllexAuthFailure({
    required this.uri,
    required this.module,
    required this.method,
    required this.statusCode,
  });
}

/// Low-level transport for the allex RPC wire protocol.
///
/// The allex wire encoding is a double-wrapped JSON-over-bytes scheme:
///
/// Request body  = `jsonEncode(utf8.encode(jsonEncode([module, [method, utf8.encode(jsonEncode(args))]])))`.
/// Response body = same scheme in reverse: outer JSON list of ints → utf8
///                 decode → inner JSON → deserialise as the result type.
///
/// Streaming responses are newline-delimited; each frame is a JSON object
/// with exactly one of the keys `"Progress"`, `"Success"`, or `"Error"`,
/// whose value is a double-wrapped payload following the same byte encoding.
class AllexTransport {
  final Uri baseUri;
  final AllexBearerTokenProvider? bearerToken;
  final AllexBearerTokenRefresher? refreshBearerToken;
  final http.Client _client;

  AllexTransport({
    required this.baseUri,
    this.bearerToken,
    this.refreshBearerToken,
    http.Client? client,
  }) : _client = client ?? http.Client();

  void dispose() => _client.close();

  // -------------------------------------------------------------------------
  // Unary call
  // -------------------------------------------------------------------------

  Future<AllexResult<T, E>> callUnary<T, E>({
    required String module,
    required String method,
    required dynamic request,
    required T Function(dynamic json) decode,
    required E Function(dynamic json) decodeError,
  }) async {
    final body = _encodeRpcCall(module, method, request);
    final response = await _postWithAuthRetry(
      module: module,
      method: method,
      body: body,
    );

    if (response.statusCode != 200) {
      throw AllexError(
        'HTTP ${response.statusCode} for $module/$method',
        httpStatus: response.statusCode,
      );
    }

    final decoded = _decodePayload(response.body);
    if (decoded is Map) {
      if (decoded.containsKey('Ok')) {
        return Ok(decode(decoded['Ok']));
      }
      if (decoded.containsKey('Err')) {
        return Err(decodeError(decoded['Err']));
      }
    }
    throw AllexError('Unexpected response shape for $module/$method: $decoded');
  }

  // -------------------------------------------------------------------------
  // Streaming call
  // -------------------------------------------------------------------------

  Stream<AllexStep<P, S, E>> callStreaming<P, S, E>({
    required String module,
    required String method,
    required dynamic request,
    required P Function(dynamic json) decodeProgress,
    required S Function(dynamic json) decodeSuccess,
    required E Function(dynamic json) decodeError,
  }) {
    return Stream.multi((controller) async {
      var isCanceled = false;
      controller.onCancel = () {
        isCanceled = true;
      };

      while (!isCanceled) {
        try {
          final streamed = await _sendWithAuthRetry(
            module: module,
            method: method,
            body: _encodeRpcCall(module, method, request),
          );

          if (streamed.statusCode != 200) {
            throw AllexError(
              'HTTP ${streamed.statusCode} for $module/$method',
              httpStatus: streamed.statusCode,
            );
          }

          final buffer = StringBuffer();
          await for (final chunk in streamed.stream.transform(utf8.decoder)) {
            buffer.write(chunk);
            final text = buffer.toString();
            final lines = text.split('\n');
            // All complete lines except the last (which may be partial).
            for (final line in lines.sublist(0, lines.length - 1)) {
              final trimmed = line.trim();
              if (trimmed.isEmpty) continue;
              final frame = jsonDecode(trimmed) as Map<String, dynamic>;
              if (frame.containsKey('Progress')) {
                final payload = _decodePayload(jsonEncode(frame['Progress']));
                controller.add(AllexProgress(decodeProgress(payload)));
              } else if (frame.containsKey('Success')) {
                final payload = _decodePayload(jsonEncode(frame['Success']));
                controller.add(AllexSuccess(decodeSuccess(payload)));
                return; // stream ended cleanly
              } else if (frame.containsKey('Error')) {
                final payload = _decodePayload(jsonEncode(frame['Error']));
                controller.add(AllexFailed(decodeError(payload)));
                return;
              }
            }
            // Keep the incomplete last line in the buffer.
            buffer
              ..clear()
              ..write(lines.last);
          }
          // Server closed the connection without a Success/Error frame.
          // Fall through to reconnect.
        } on AllexError {
          rethrow;
        } catch (e) {
          if (isCanceled) return;
          // Brief pause before reconnect to avoid hammering a failing server.
          await Future<void>.delayed(const Duration(seconds: 2));
        }
        if (isCanceled) return;
        // Brief pause between reconnect attempts.
        await Future<void>.delayed(const Duration(seconds: 1));
      }
    });
  }

  // -------------------------------------------------------------------------
  // Wire encoding helpers
  // -------------------------------------------------------------------------

  /// Encodes an allex RPC call body.
  ///
  /// Wire format:
  ///   outer = jsonEncode(bytes)
  ///   where bytes = utf8.encode(inner)
  ///   inner = jsonEncode([module, [method, argBytes]])
  ///   argBytes = utf8.encode(jsonEncode(args))
  String _encodeRpcCall(String module, String method, dynamic args) {
    final argBytes = utf8.encode(jsonEncode(args));
    final inner = jsonEncode([module, [method, argBytes]]);
    final outerBytes = utf8.encode(inner);
    return jsonEncode(outerBytes);
  }

  /// Decodes a double-wrapped allex response payload.
  dynamic _decodePayload(String raw) {
    final outer = jsonDecode(raw);
    if (outer is List<dynamic>) {
      final bytes = List<int>.from(outer);
      return jsonDecode(utf8.decode(bytes));
    }
    // Fallback: server returned plain JSON (should not happen in normal operation).
    return outer;
  }

  Future<http.Response> _postWithAuthRetry({
    required String module,
    required String method,
    required String body,
  }) async {
    final response = await _post(body);
    if (!await _shouldRetryAuth(module, method, response.statusCode)) {
      return response;
    }

    return _post(body);
  }

  Future<http.StreamedResponse> _sendWithAuthRetry({
    required String module,
    required String method,
    required String body,
  }) async {
    final response = await _send(body);
    if (!await _shouldRetryAuth(module, method, response.statusCode)) {
      return response;
    }

    return _send(body);
  }

  Future<http.Response> _post(String body) async {
    return _client.post(
      baseUri,
      headers: await _headers(),
      body: body,
    );
  }

  Future<http.StreamedResponse> _send(String body) async {
    final httpRequest = http.Request('POST', baseUri);
    httpRequest.headers.addAll(await _headers());
    httpRequest.body = body;
    return _client.send(httpRequest);
  }

  Future<bool> _shouldRetryAuth(
    String module,
    String method,
    int statusCode,
  ) async {
    if (refreshBearerToken == null) return false;
    if (statusCode != 401 && statusCode != 403) return false;

    return await refreshBearerToken!(
      AllexAuthFailure(
        uri: baseUri,
        module: module,
        method: method,
        statusCode: statusCode,
      ),
    );
  }

  Future<Map<String, String>> _headers() async {
    final token = await bearerToken?.call();
    return {
      'Content-Type': 'application/json',
      if (token != null && token.isNotEmpty) 'Authorization': 'Bearer $token',
    };
  }
}
