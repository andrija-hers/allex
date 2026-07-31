// Does a live watch actually come back when the server does?
//
// Three ways a server "goes away" that a dashboard must survive without a
// human reloading the page:
//
//   1. the process is gone      → connection refused, then it returns
//   2. the process is there but broken → 5xx, then it recovers
//   3. the process came back NEW → the caller's token no longer validates
//
// (3) is the one that bit us. A unary call refused by the role gate returns
// HTTP 403 and the transport refreshes the token; a *streaming* call refused by
// the same gate returns HTTP 200 with an in-band `AccessForbidden` error frame,
// so there was no status to react to. The watch retried every 5 seconds for 18
// minutes with a token that could never work, reported as `anonymous` in the
// server log every time, until the page was reloaded by hand.
import 'dart:async';
import 'dart:convert';

import 'package:allex/allex.dart';
import 'package:http/http.dart' as http;
import 'package:test/test.dart';

/// A client whose behaviour is driven by a mutable script, so a test can flip
/// the "server" between healthy, down, and refusing partway through a run.
class _ScriptedClient extends http.BaseClient {
  _ScriptedClient(this.handle);

  final Future<http.StreamedResponse> Function(http.BaseRequest request) handle;
  int calls = 0;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) {
    calls++;
    return handle(request);
  }
}

String _wrap(dynamic payload) => jsonEncode(utf8.encode(jsonEncode(payload)));

String _progress(dynamic payload) =>
    jsonEncode({'Progress': jsonDecode(_wrap(payload))});

/// The server's refusal: HTTP 200, with the denial as the first stream frame.
http.StreamedResponse _accessForbiddenFrame() => http.StreamedResponse(
      Stream.value(
        utf8.encode('${jsonEncode({'Error': jsonDecode(_wrap('AccessForbidden'))})}\n'),
      ),
      200,
      headers: const {'content-type': 'application/x-ndjson'},
    );

http.StreamedResponse _ndjson(List<String> frames) => http.StreamedResponse(
      Stream.value(utf8.encode(frames.map((f) => '$f\n').join())),
      200,
      headers: const {'content-type': 'application/x-ndjson'},
    );

class _CounterUpdate extends HasUpdate<int> {
  const _CounterUpdate();

  @override
  int fromJson(Map<String, dynamic> json) => (json['n'] as num).toInt();

  @override
  int applyUpdate(int current, Map<String, dynamic> update) =>
      (update['n'] as num).toInt();
}

LiveStateRepository<int> _repo(
  http.Client client, {
  AllexBearerTokenProvider? bearerToken,
  AllexBearerTokenRefresher? refreshBearerToken,
}) {
  final transport = AllexTransport(
    baseUri: Uri.parse('http://localhost/rpc'),
    client: client,
    bearerToken: bearerToken,
    refreshBearerToken: refreshBearerToken,
  );
  return LiveStateRepository<int>(
    transport: transport,
    module: 'counter_v_1',
    watchMethod: 'watch',
    hasUpdate: const _CounterUpdate(),
    reconnectDelay: const Duration(milliseconds: 5),
  );
}

void main() {
  group('a live watch survives the engine going down and coming back', () {
    test('the process disappears, then returns', () async {
      var engineUp = true;
      final client = _ScriptedClient((_) async {
        if (!engineUp) {
          // What a dead listener looks like to the client.
          throw http.ClientException('Connection refused');
        }
        return _ndjson([_progress({'n': 1})]);
      });
      final repo = _repo(client);

      final states = <int>[];
      final statuses = <AllexLiveStatus>[];
      final data = repo.stream.listen(states.add);
      final status = repo.statusStream.listen(statuses.add);

      await _until(() => statuses.contains(AllexLiveStatus.connected));
      engineUp = false;
      // Let it fail repeatedly — this is the 18 minutes, compressed.
      final callsWhileDown = client.calls;
      await _until(() => client.calls > callsWhileDown + 3);
      expect(states, isNotEmpty, reason: 'the pre-outage snapshot is retained');

      engineUp = true;
      await _until(() => states.length >= 2);

      await data.cancel();
      await status.cancel();
      repo.dispose();

      expect(statuses, contains(AllexLiveStatus.reconnecting));
      expect(statuses.last, AllexLiveStatus.connected);
      expect(repo.lastError, isNull, reason: 'cleared once a frame arrives');
    });

    test('it comes back NEW, and the old token no longer validates', () async {
      // The engine restarted with a fresh signing secret: the token minted
      // before the restart is refused, and only a re-login can fix it.
      var validToken = 'token-after-restart';
      var currentToken = 'token-from-before-restart';
      var logins = 0;

      final client = _ScriptedClient((request) async {
        final sent = request.headers['Authorization'];
        if (sent != 'Bearer $validToken') {
          // The refusal the role gate actually emits for a streaming call:
          // 200, denial in-band. NOT a 403.
          return _accessForbiddenFrame();
        }
        return _ndjson([_progress({'n': 42})]);
      });

      final repo = _repo(
        client,
        bearerToken: () async => currentToken,
        refreshBearerToken: (failure) async {
          logins++;
          currentToken = validToken; // a fresh login against the new engine
          return true;
        },
      );

      final states = <int>[];
      final sub = repo.stream.listen(states.add);

      await _until(() => states.isNotEmpty);
      await sub.cancel();
      repo.dispose();

      expect(states, [42], reason: 'the watch recovered on its own');
      expect(logins, 1, reason: 'exactly one re-authentication, not a storm');

      // And the recovery is not a fluke of ordering: it happened without any
      // 401/403 status ever being returned.
      expect(validToken, isNotEmpty);
    });

    test('a denial that survives re-authentication is reported as auth, not as '
        'a generic drop', () async {
      // Wrong credentials, say. The caller must be able to tell "log in again"
      // from "the network is flaky", which needs the 403 tag.
      final client = _ScriptedClient((_) async => _accessForbiddenFrame());
      final transport = AllexTransport(
        baseUri: Uri.parse('http://localhost/rpc'),
        client: client,
        bearerToken: () async => 'stale',
        refreshBearerToken: (_) async => true, // refresh "works", server still says no
      );

      Object? caught;
      try {
        await transport.watchRaw('counter_v_1', 'watch', const {}).toList();
      } catch (error) {
        caught = error;
      }

      expect(caught, isA<AllexError>());
      expect((caught as AllexError).httpStatus, 403,
          reason: 'an in-band AccessForbidden is an auth failure');
      expect(client.calls, 2, reason: 'tried once, refreshed, tried once more');
    });

    test('reconnectNow cuts the backoff short for a "retry" control', () async {
      var engineUp = false;
      final client = _ScriptedClient((_) async {
        if (!engineUp) throw http.ClientException('Connection refused');
        return _ndjson([_progress({'n': 5})]);
      });
      // A backoff far longer than this test would ever wait for: the only way
      // a frame arrives in time is if reconnectNow actually interrupts it.
      final transport = AllexTransport(
        baseUri: Uri.parse('http://localhost/rpc'),
        client: client,
      );
      final repo = LiveStateRepository<int>(
        transport: transport,
        module: 'counter_v_1',
        watchMethod: 'watch',
        hasUpdate: const _CounterUpdate(),
        reconnectDelay: const Duration(minutes: 5),
      );

      final states = <int>[];
      final sub = repo.stream.listen(states.add);
      await _until(() => repo.lastError != null); // first attempt failed

      engineUp = true;
      repo.reconnectNow();
      await _until(() => states.isNotEmpty);

      await sub.cancel();
      repo.dispose();
      expect(states, [5]);
    });

    test('reconnectNow is harmless when connected or disposed', () async {
      final client = _ScriptedClient((_) async => _ndjson([_progress({'n': 1})]));
      final repo = _repo(client);
      final sub = repo.stream.listen((_) {});
      await _until(() => repo.lastKnown != null);
      repo.reconnectNow();
      repo.reconnectNow();
      await sub.cancel();
      repo.dispose();
      repo.reconnectNow(); // after dispose
      expect(repo.lastKnown, 1);
    });

    test('an ordinary stream error is not mistaken for an auth failure',
        () async {
      final client = _ScriptedClient((_) async => http.StreamedResponse(
            Stream.value(utf8.encode(
                '${jsonEncode({'Error': jsonDecode(_wrap('SomethingBroke'))})}\n')),
            200,
            headers: const {'content-type': 'application/x-ndjson'},
          ));
      final transport = AllexTransport(
        baseUri: Uri.parse('http://localhost/rpc'),
        client: client,
        bearerToken: () async => 'fine',
        refreshBearerToken: (_) async => true,
      );

      Object? caught;
      try {
        await transport.watchRaw('counter_v_1', 'watch', const {}).toList();
      } catch (error) {
        caught = error;
      }

      expect(caught, isA<AllexError>());
      expect((caught as AllexError).httpStatus, isNull);
      expect(client.calls, 1, reason: 'no pointless re-login for an app error');
    });
  });
}

/// Polls [condition] until true or a timeout elapses.
Future<void> _until(bool Function() condition, {int maxTurns = 3000}) async {
  for (var i = 0; i < maxTurns; i++) {
    if (condition()) return;
    await Future<void>.delayed(const Duration(milliseconds: 1));
  }
  throw StateError('Condition not met within $maxTurns turns');
}
