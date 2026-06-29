import 'dart:async';
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

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    if (_handlers.isEmpty) {
      throw StateError('No queued response for ${request.url}.');
    }
    return _handlers.removeAt(0)(request);
  }
}

String _wrap(dynamic payload) => jsonEncode(utf8.encode(jsonEncode(payload)));

String _progress(dynamic payload) =>
    jsonEncode({'Progress': jsonDecode(_wrap(payload))});

http.StreamedResponse _ndjson(List<String> frames, {int statusCode = 200}) {
  return http.StreamedResponse(
    Stream.value(utf8.encode(frames.map((f) => '$f\n').join())),
    statusCode,
    headers: const {'content-type': 'application/x-ndjson'},
  );
}

http.StreamedResponse _status(int statusCode, String body) {
  return http.StreamedResponse(
    Stream.value(utf8.encode(body)),
    statusCode,
  );
}

/// Folds `{type: initial|update, n: int}` frames into a running counter.
class _CounterUpdate extends HasUpdate<int> {
  const _CounterUpdate();

  @override
  int fromJson(Map<String, dynamic> json) => (json['n'] as num).toInt();

  @override
  int applyUpdate(int current, Map<String, dynamic> update) =>
      current + (update['n'] as num).toInt();
}

LiveStateRepository<int> _repo(_QueueClient client) {
  final transport = AllexTransport(
    baseUri: Uri.parse('http://localhost/rpc'),
    client: client,
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
  group('LiveStateRepository', () {
    test('applies snapshot then delta frames into running state', () async {
      final client = _QueueClient([
        (_) async => _ndjson([_progress({'n': 1}), _progress({'n': 4})]),
      ]);
      final repo = _repo(client);

      final states = <int>[];
      final sub = repo.stream.listen(states.add);
      await _until(() => states.length >= 2);
      await sub.cancel();
      repo.dispose();

      // 1 (initial), then +4 (delta) = 5.
      expect(states, [1, 5]);
      expect(repo.lastKnown, 5);
    });

    test('reports connecting → connected on the status stream', () async {
      final client = _QueueClient([
        (_) async => _ndjson([_progress({'n': 1})]),
      ]);
      final repo = _repo(client);

      final statuses = <AllexLiveStatus>[];
      final sub = repo.statusStream.listen(statuses.add);
      await _until(() => statuses.contains(AllexLiveStatus.connected));
      await sub.cancel();
      repo.dispose();

      expect(statuses.first, AllexLiveStatus.connecting);
      expect(statuses, contains(AllexLiveStatus.connected));
    });

    test('records lastError and recovers on the next attempt', () async {
      final client = _QueueClient([
        (_) async => _status(500, 'boom'),
        (_) async => _ndjson([_progress({'n': 7})]),
      ]);
      final repo = _repo(client);

      final states = <int>[];
      final sub = repo.stream.listen(states.add);
      // First attempt fails (records lastError), second yields a value.
      await _until(() => states.isNotEmpty);
      await sub.cancel();
      repo.dispose();

      expect(states, [7]);
      // lastError is cleared once a frame arrives after recovery.
      expect(repo.lastError, isNull);
    });

    test('surfaces a terminal Error frame as lastError', () async {
      final errorFrame = jsonEncode({'Error': jsonDecode(_wrap('denied'))});
      final client = _QueueClient([
        (_) async => _ndjson([errorFrame]),
        // Keep the loop from starving; never reached before dispose.
        (_) async => _ndjson([_progress({'n': 1})]),
      ]);
      final repo = _repo(client);

      // Start the loop and let the first attempt fail.
      final sub = repo.stream.listen((_) {});
      await _until(() => repo.lastError != null);
      await sub.cancel();
      repo.dispose();

      expect(repo.lastError, isA<AllexError>());
      expect('${repo.lastError}', contains('denied'));
    });
  });
}

/// Polls [condition] until true or a timeout elapses (no wall-clock flakiness:
/// driven by event-loop turns).
Future<void> _until(
  bool Function() condition, {
  int maxTurns = 2000,
}) async {
  for (var i = 0; i < maxTurns; i++) {
    if (condition()) return;
    await Future<void>.delayed(const Duration(milliseconds: 1));
  }
  throw StateError('Condition not met within $maxTurns turns');
}
