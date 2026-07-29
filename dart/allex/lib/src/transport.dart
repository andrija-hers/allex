import 'dart:async';
import 'dart:convert';
import 'package:http/http.dart' as http;
import 'types.dart';

typedef AllexBearerTokenProvider = FutureOr<String?> Function();
typedef AllexBearerTokenRefresher = FutureOr<bool> Function(
    AllexAuthFailure failure);

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
/// The allex wire encoding is a double-wrapped JSON-in-JSON scheme:
///
/// Request body  = `jsonEncode(jsonEncode([module, [method, jsonEncode(args)]]))`.
/// Response body = same scheme in reverse: outer JSON string → inner JSON →
///                 deserialise as the result type.
///
/// Streaming responses are newline-delimited; each frame is a JSON object
/// with exactly one of the keys `"Progress"`, `"Success"`, or `"Error"`,
/// whose value is a double-wrapped payload following the same scheme.
///
/// Payloads used to travel as JSON arrays of byte values — about four
/// characters per byte, and one array element per byte for the receiver to
/// walk. Both ends still READ that form, so a new client talks to an old
/// server and vice versa; neither writes it any more.
/// Called with the server's clock reading (UTC) parsed from an HTTP `Date`
/// response header, on every response. Lets a consumer correct for device-clock
/// skew so time-based UI decisions match the server. Optional and additive —
/// when null the transport behaves exactly as before.
typedef AllexServerTimeObserver = void Function(DateTime serverUtc);

class AllexTransport {
  final Uri baseUri;
  final AllexBearerTokenProvider? bearerToken;
  final AllexBearerTokenRefresher? refreshBearerToken;
  final AllexServerTimeObserver? onServerTime;
  final http.Client _client;

  AllexTransport({
    required this.baseUri,
    this.bearerToken,
    this.refreshBearerToken,
    this.onServerTime,
    http.Client? client,
  }) : _client = client ?? http.Client();

  void dispose() => _client.close();

  /// Reports the server's clock from a response's `Date` header (RFC 1123, e.g.
  /// `Wed, 21 Oct 2015 07:28:00 GMT`), if present and parseable. Web-safe (no
  /// `dart:io`). Never throws — a missing/odd header is simply ignored.
  void _noteServerTime(Map<String, String> headers) {
    final observer = onServerTime;
    if (observer == null) return;
    final raw = headers['date'];
    if (raw == null) return;
    final m = RegExp(
      r'^[A-Za-z]{3}, (\d{2}) ([A-Za-z]{3}) (\d{4}) '
      r'(\d{2}):(\d{2}):(\d{2}) GMT$',
    ).firstMatch(raw.trim());
    if (m == null) return;
    const months = {
      'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
      'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
    };
    final month = months[m.group(2)];
    if (month == null) return;
    observer(DateTime.utc(
      int.parse(m.group(3)!), month, int.parse(m.group(1)!),
      int.parse(m.group(4)!), int.parse(m.group(5)!), int.parse(m.group(6)!),
    ));
  }

  Future<AllexResult<T, E>> callUnary<T, E>({
    required String module,
    required String method,
    required dynamic request,
    required T Function(dynamic json) decode,
    required E Function(dynamic json) decodeError,
  }) async {
    final response = await _postWithAuthRetry(
      module: module,
      method: method,
      body: _encodeRpcCall(module, method, request),
    );
    _noteServerTime(response.headers);

    if (response.statusCode == 200) {
      return Ok(decode(_decodePayload(response.body)));
    }

    if (response.statusCode == 422) {
      return Err(decodeError(_decodePayload(response.body)));
    }

    throw AllexError(
      _httpErrorMessage(response.statusCode, module, method, response.body),
      httpStatus: response.statusCode,
    );
  }

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
          _noteServerTime(streamed.headers);

          if (streamed.statusCode != 200) {
            throw AllexError(
              _httpErrorMessage(
                streamed.statusCode,
                module,
                method,
                await streamed.stream.bytesToString(),
              ),
              httpStatus: streamed.statusCode,
            );
          }

          final buffer = StringBuffer();
          await for (final chunk in streamed.stream.transform(utf8.decoder)) {
            buffer.write(chunk);
            final text = buffer.toString();
            final lines = text.split('\n');
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
                await controller.close();
                return;
              } else if (frame.containsKey('Error')) {
                final payload = _decodePayload(jsonEncode(frame['Error']));
                controller.add(AllexFailed(decodeError(payload)));
                await controller.close();
                return;
              }
            }
            buffer
              ..clear()
              ..write(lines.last);
          }
        } on AllexError {
          rethrow;
        } catch (_) {
          if (isCanceled) return;
          await Future<void>.delayed(const Duration(seconds: 2));
        }
        if (isCanceled) {
          await controller.close();
          return;
        }
        await Future<void>.delayed(const Duration(seconds: 1));
      }

      await controller.close();
    });
  }

  /// Opens a single-shot streaming call, yielding each decoded **Progress**
  /// payload as raw JSON (a `Map`, `List`, or scalar).
  ///
  /// Unlike [callStreaming], this performs **no** auto-reconnect and applies no
  /// typed decoding — it is the low-level primitive that state-sync layers such
  /// as `LiveStateRepository` build their own reconnect loop on top of.
  ///
  /// Completes normally when the server sends a terminal `Success` frame or
  /// closes the stream. Throws [AllexError] on a terminal `Error` frame, a
  /// non-200 status, or a transport failure.
  Stream<dynamic> watchRaw(
    String module,
    String method,
    dynamic request,
  ) async* {
    final streamed = await _sendWithAuthRetry(
      module: module,
      method: method,
      body: _encodeRpcCall(module, method, request),
    );
    _noteServerTime(streamed.headers);

    if (streamed.statusCode != 200) {
      throw AllexError(
        _httpErrorMessage(
          streamed.statusCode,
          module,
          method,
          await streamed.stream.bytesToString(),
        ),
        httpStatus: streamed.statusCode,
      );
    }

    final buffer = StringBuffer();
    await for (final chunk in streamed.stream.transform(utf8.decoder)) {
      buffer.write(chunk);
      final text = buffer.toString();
      final lines = text.split('\n');
      for (final line in lines.sublist(0, lines.length - 1)) {
        final trimmed = line.trim();
        if (trimmed.isEmpty) continue;
        final frame = jsonDecode(trimmed) as Map<String, dynamic>;
        if (frame.containsKey('Progress')) {
          yield _decodePayload(jsonEncode(frame['Progress']));
        } else if (frame.containsKey('Success')) {
          return;
        } else if (frame.containsKey('Error')) {
          final payload = _decodePayload(jsonEncode(frame['Error']));
          throw AllexError(
            'Stream error from $module/$method: '
            '${payload is String ? payload : jsonEncode(payload)}',
          );
        }
      }
      buffer
        ..clear()
        ..write(lines.last);
    }
  }

  /// Builds the request body: a payload nested in a payload, each one a JSON
  /// **string** rather than an array of byte values.
  ///
  /// The array form the server used to require spent roughly four characters
  /// per byte and forced the peer to walk one element per byte before it could
  /// parse what they spelled — on a snapshot-sized reply that was 10.1 MB and
  /// three million elements for 2.9 MB of JSON. Servers still read the array
  /// form, so an older client keeps working.
  String _encodeRpcCall(String module, String method, dynamic args) {
    final inner = jsonEncode([
      module,
      [method, jsonEncode(args)]
    ]);
    return jsonEncode(inner);
  }

  /// Unwraps one payload level. A payload arrives as a JSON string holding the
  /// encoded value; older servers send an array of byte values instead, which
  /// is still accepted. Anything else is already-decoded JSON.
  dynamic _decodePayload(String raw) {
    final outer = jsonDecode(raw);
    if (outer is String) {
      // A JSON string is ambiguous here: usually a payload wrapping encoded
      // JSON, but an application error can also BE a JSON string (`"Overflow"`
      // for a unit-variant error). Unwrap when the content parses as JSON;
      // otherwise the string is the value itself.
      try {
        return jsonDecode(outer);
      } on FormatException {
        return outer;
      }
    }
    if (outer is List<dynamic>) {
      final bytes = List<int>.from(outer);
      return jsonDecode(utf8.decode(bytes));
    }
    return outer;
  }

  String _httpErrorMessage(
    int statusCode,
    String module,
    String method,
    String body,
  ) {
    final trimmed = body.trim();
    if (trimmed.isEmpty) {
      return 'HTTP $statusCode for $module/$method';
    }

    return 'HTTP $statusCode for $module/$method: ${_decodeHttpBody(trimmed)}';
  }

  String _decodeHttpBody(String body) {
    try {
      final decoded = _decodePayload(body);
      return decoded is String ? decoded : jsonEncode(decoded);
    } catch (_) {
      return body;
    }
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
