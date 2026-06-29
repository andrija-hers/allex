import 'dart:convert';

import 'package:allex/allex.dart';
import 'package:http/http.dart' as http;
import 'package:test/test.dart';

typedef _ResponseHandler = Future<http.StreamedResponse> Function(
  http.BaseRequest request,
);

class _QueueClient extends http.BaseClient {
  _QueueClient(this._handlers);

  final List<_ResponseHandler> _handlers;
  final List<http.BaseRequest> requests = [];

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    requests.add(request);
    if (_handlers.isEmpty) {
      throw StateError('No queued response for ${request.url}.');
    }
    return _handlers.removeAt(0)(request);
  }
}

String _wrap(dynamic payload) => jsonEncode(utf8.encode(jsonEncode(payload)));

http.StreamedResponse _textResponse(
  int statusCode,
  String body, {
  Map<String, String> headers = const {'content-type': 'application/json'},
}) {
  return http.StreamedResponse(
    Stream.value(utf8.encode(body)),
    statusCode,
    headers: headers,
  );
}

Future<String> _bodyOf(http.BaseRequest request) =>
    (request as http.Request).finalize().bytesToString();

void main() {
  group('AllexPasswordSession', () {
    test('logs in lazily and supplies the bearer token', () async {
      final client = _QueueClient([
        (request) async {
          expect(request.url.path, '/api/login');
          final body = jsonDecode(await _bodyOf(request)) as Map;
          expect(body['handle'], 'admin');
          expect(body['password'], 'adminpass');
          return _textResponse(
            200,
            jsonEncode({'token': 'tok-1', 'refresh_token': 'refresh-1'}),
          );
        },
      ]);
      final session = AllexPasswordSession(
        baseUri: Uri.parse('http://localhost:8787'),
        handle: 'admin',
        password: 'adminpass',
        client: client,
      );

      expect(session.rpcUri, Uri.parse('http://localhost:8787/rpc'));
      expect(await session.bearerToken(), 'tok-1');
      // Cached: no second login request.
      expect(await session.bearerToken(), 'tok-1');
      expect(client.requests, hasLength(1));
    });

    test('refreshes the access token via identity_v_1.refresh', () async {
      final client = _QueueClient([
        (_) async => _textResponse(
              200,
              jsonEncode({'token': 'tok-1', 'refresh_token': 'refresh-1'}),
            ),
        (request) async {
          expect(request.url.path, '/rpc');
          return _textResponse(
            200,
            _wrap({'token': 'tok-2', 'refresh_token': 'refresh-2'}),
          );
        },
      ]);
      final session = AllexPasswordSession(
        baseUri: Uri.parse('http://localhost:8787'),
        handle: 'admin',
        password: 'adminpass',
        client: client,
      );

      expect(await session.bearerToken(), 'tok-1');
      final refreshed = await session.refreshBearerToken(
        AllexAuthFailure(
          uri: _placeholder,
          module: 'active_schedules_v_1',
          method: 'get_active_schedules',
          statusCode: 401,
        ),
      );
      expect(refreshed, isTrue);
      expect(await session.bearerToken(), 'tok-2');
    });

    test('falls back to password login when refresh is rejected', () async {
      final client = _QueueClient([
        (_) async => _textResponse(
              200,
              jsonEncode({'token': 'tok-1', 'refresh_token': 'refresh-1'}),
            ),
        // identity_v_1.refresh rejected with a typed (HTTP 422) error.
        (_) async => _textResponse(422, _wrap('expired')),
        // Re-login with the password succeeds.
        (_) async => _textResponse(
              200,
              jsonEncode({'token': 'tok-3', 'refresh_token': 'refresh-3'}),
            ),
      ]);
      final session = AllexPasswordSession(
        baseUri: Uri.parse('http://localhost:8787'),
        handle: 'admin',
        password: 'adminpass',
        client: client,
      );

      expect(await session.bearerToken(), 'tok-1');
      final refreshed = await session.refreshBearerToken(
        AllexAuthFailure(
          uri: _placeholder,
          module: 'm',
          method: 'x',
          statusCode: 403,
        ),
      );
      expect(refreshed, isTrue);
      expect(await session.bearerToken(), 'tok-3');
    });

    test('refresh reports failure when re-login also fails', () async {
      final client = _QueueClient([
        (_) async => _textResponse(
              200,
              jsonEncode({'token': 'tok-1', 'refresh_token': 'refresh-1'}),
            ),
        (_) async => _textResponse(422, _wrap('expired')),
        (_) async => _textResponse(401, 'bad credentials'),
      ]);
      final session = AllexPasswordSession(
        baseUri: Uri.parse('http://localhost:8787'),
        handle: 'admin',
        password: 'adminpass',
        client: client,
      );

      expect(await session.bearerToken(), 'tok-1');
      final refreshed = await session.refreshBearerToken(
        AllexAuthFailure(
          uri: _placeholder,
          module: 'm',
          method: 'x',
          statusCode: 401,
        ),
      );
      expect(refreshed, isFalse);
    });
  });
}

final _placeholder = Uri.parse('http://localhost:8787/rpc');
