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

/// Identifies the running client to the server, sent as `x-allex-client` on
/// every call. By convention `platform/version+build`, e.g. `ios/1.0.2+42`.
///
/// Self-reported and unverified — a server must treat it as telemetry, never as
/// a credential. It exists so a deployment can answer "which builds are still
/// out there", which is what makes retiring a compatibility shim a decision
/// rather than a guess.
typedef AllexClientInfoProvider = String? Function();

/// Called when a **streaming** call's link changes: `true` once a subscribe is
/// live (HTTP 200, frames can flow), `false` when it drops and the transport
/// starts its silent retry loop.
///
/// [AllexTransport.callStreaming] reconnects forever and reports nothing while it
/// does — by design, so a consumer isn't asked to write a retry loop. The cost is
/// that a dead server and an idle one look **identical** from the stream: no
/// error, and on a quiet feed no frames either. A UI then keeps rendering its
/// last snapshot as though it were current, which is the failure mode where
/// everything looks healthy and nothing is. Silence cannot be interpreted; this
/// observer is what makes the difference observable. Optional and additive — when
/// null the transport behaves exactly as before.
typedef AllexStreamLiveObserver = void Function(bool live);

class AllexTransport {
  final Uri baseUri;
  final AllexBearerTokenProvider? bearerToken;
  final AllexBearerTokenRefresher? refreshBearerToken;
  final AllexServerTimeObserver? onServerTime;
  final AllexClientInfoProvider? clientInfo;
  final AllexStreamLiveObserver? onStreamLive;
  final http.Client _client;

  AllexTransport({
    required this.baseUri,
    this.bearerToken,
    this.refreshBearerToken,
    this.onServerTime,
    this.clientInfo,
    this.onStreamLive,
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
        // Whether this iteration already reported the link down (the catch path),
        // so the fall-through below doesn't report it a second time.
        var announcedDown = false;
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
          // Subscribed: frames can flow. Re-announced on every reconnect, so a
          // consumer that only ever sees `true` has simply never lost the link.
          onStreamLive?.call(true);

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
          onStreamLive?.call(false);
          rethrow;
        } catch (_) {
          if (isCanceled) return;
          // The link died mid-stream (server gone, socket reset, DNS): say so
          // before going quiet for the retry, or the consumer cannot tell this
          // from a feed with nothing to report.
          onStreamLive?.call(false);
          announcedDown = true;
          await Future<void>.delayed(const Duration(seconds: 2));
        }
        if (isCanceled) {
          await controller.close();
          return;
        }
        // Not via the catch: the response body ended without a terminal frame —
        // the server closed an open subscription. Same ambiguity, same signal.
        if (!announcedDown) onStreamLive?.call(false);
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
  ///
  /// ## Authorization is in-band here, unlike a unary call
  ///
  /// A unary call refused by the role gate comes back as **HTTP 403**, which
  /// [_sendWithAuthRetry] turns into a token refresh and one retry. A streaming
  /// call refused by the same gate comes back as **HTTP 200** with an in-band
  /// `Error` frame carrying `AccessForbidden`: the server emits the denial as
  /// the first item of the stream, so there is no status code to react to.
  ///
  /// Left alone, that is a silent, permanent outage. A caller whose token stops
  /// validating — the server restarted with a fresh signing secret, say —
  /// reconnects forever with the same dead token, is refused as `anonymous`
  /// every time, and never refreshes, because nothing in the loop can tell an
  /// authorization failure from a server that is merely unhappy. Observed as
  /// exactly that: a dashboard retrying every 5s for 18 minutes until the page
  /// was reloaded by hand.
  ///
  /// So the in-band denial is treated as what it is: refresh the token and
  /// retry once, mirroring the unary path. A denial that survives the retry is
  /// thrown as an [AllexError] tagged `httpStatus: 403`, so callers classifying
  /// on status see "authentication required" rather than a generic drop. Only
  /// the *first* frame can trigger this — once a frame has been yielded the
  /// stream is established, and re-running it would replay state to the caller.
  Stream<dynamic> watchRaw(
    String module,
    String method,
    dynamic request,
  ) async* {
    var mayRetryAuth = refreshBearerToken != null;

    while (true) {
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

      var yieldedAny = false;
      var retryAfterRefresh = false;
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
            yieldedAny = true;
            yield _decodePayload(jsonEncode(frame['Progress']));
          } else if (frame.containsKey('Success')) {
            return;
          } else if (frame.containsKey('Error')) {
            final payload = _decodePayload(jsonEncode(frame['Error']));
            final denied = _isAccessDenied(payload);
            if (denied && mayRetryAuth && !yieldedAny) {
              mayRetryAuth = false;
              retryAfterRefresh = await refreshBearerToken!(
                AllexAuthFailure(
                  uri: baseUri,
                  module: module,
                  method: method,
                  statusCode: 403,
                ),
              );
              if (retryAfterRefresh) break;
            }
            throw AllexError(
              'Stream error from $module/$method: '
              '${payload is String ? payload : jsonEncode(payload)}',
              httpStatus: denied ? 403 : null,
            );
          }
        }
        if (retryAfterRefresh) break;
        buffer
          ..clear()
          ..write(lines.last);
      }
      if (!retryAfterRefresh) return;
    }
  }

  /// Whether a terminal `Error` payload is the transport's authorization
  /// refusal rather than an application error. The server encodes
  /// `transport::Error::AccessForbidden` as the bare string.
  static bool _isAccessDenied(dynamic payload) =>
      payload is String && payload == 'AccessForbidden';

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
    final client = clientInfo?.call();
    return {
      'Content-Type': 'application/json',
      if (token != null && token.isNotEmpty) 'Authorization': 'Bearer $token',
      if (client != null && client.isNotEmpty) 'x-allex-client': client,
    };
  }
}
