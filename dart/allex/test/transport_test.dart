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

class _AddResult {
  final int r;

  const _AddResult(this.r);

  factory _AddResult.fromJson(dynamic json) {
    final map = json as Map<String, dynamic>;
    return _AddResult((map['r'] as num).toInt());
  }
}

class _CountStep {
  final int currentlyAt;

  const _CountStep(this.currentlyAt);

  factory _CountStep.fromJson(dynamic json) {
    final map = json as Map<String, dynamic>;
    return _CountStep((map['currently_at'] as num).toInt());
  }
}

class _StepsMade {
  final int total;

  const _StepsMade(this.total);

  factory _StepsMade.fromJson(dynamic json) {
    final map = json as Map<String, dynamic>;
    return _StepsMade((map['total'] as num).toInt());
  }
}

/// The legacy wire form: a payload as a JSON array of byte values. Servers no
/// longer write it, but both ends still read it, so it stays covered.
String _wrap(dynamic payload) => jsonEncode(utf8.encode(jsonEncode(payload)));

/// The current wire form: a payload as a JSON string holding encoded JSON.
String _wrapString(dynamic payload) => jsonEncode(jsonEncode(payload));

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

http.StreamedResponse _chunkedResponse(
  int statusCode,
  List<String> chunks, {
  Map<String, String> headers = const {'content-type': 'application/x-ndjson'},
}) {
  return http.StreamedResponse(
    Stream.fromIterable(chunks.map(utf8.encode)),
    statusCode,
    headers: headers,
  );
}

String? _authorization(http.BaseRequest request) =>
    request.headers['Authorization'] ?? request.headers['authorization'];

void main() {
  group('AllexTransport.callUnary', () {
    test('decodes a payload sent as a JSON string', () async {
      final client = _QueueClient([
        (_) async => _textResponse(200, _wrapString({'r': 7})),
      ]);
      final transport = AllexTransport(
        baseUri: Uri.parse('http://localhost/rpc'),
        client: client,
      );

      final result = await transport.callUnary<_AddResult, String>(
        module: 'arithmetic_v_1',
        method: 'add',
        request: {'a': 3, 'b': 4},
        decode: _AddResult.fromJson,
        decodeError: (json) => json as String,
      );

      expect((result as Ok<_AddResult, String>).value.r, 7);
    });

    test('sends the request as nested JSON strings, not arrays of bytes',
        () async {
      String? sentBody;
      final client = _QueueClient([
        (request) async {
          sentBody = (request as http.Request).body;
          return _textResponse(200, _wrapString({'r': 1}));
        },
      ]);
      final transport = AllexTransport(
        baseUri: Uri.parse('http://localhost/rpc'),
        client: client,
      );

      await transport.callUnary<_AddResult, String>(
        module: 'arithmetic_v_1',
        method: 'add',
        request: {'a': 3, 'b': 4},
        decode: _AddResult.fromJson,
        decodeError: (json) => json as String,
      );

      // Two string levels, and nowhere an array of byte values — that form
      // spent ~4 characters per byte in both directions.
      final outer = jsonDecode(sentBody!);
      expect(outer, isA<String>());
      final call = jsonDecode(outer as String) as List<dynamic>;
      expect(call.first, 'arithmetic_v_1');
      final method = call[1] as List<dynamic>;
      expect(method.first, 'add');
      expect(method[1], isA<String>());
      expect(jsonDecode(method[1] as String), {'a': 3, 'b': 4});
    });

    test('decodes direct success payloads into Ok', () async {
      final client = _QueueClient([
        (_) async => _textResponse(200, _wrap({'r': 7})),
      ]);
      final transport = AllexTransport(
        baseUri: Uri.parse('http://localhost/rpc'),
        client: client,
      );

      final result = await transport.callUnary<_AddResult, String>(
        module: 'arithmetic_v_1',
        method: 'add',
        request: {'a': 3, 'b': 4},
        decode: _AddResult.fromJson,
        decodeError: (json) => json as String,
      );

      expect(result, isA<Ok<_AddResult, String>>());
      expect((result as Ok<_AddResult, String>).value.r, 7);
    });

    test('decodes HTTP 422 application errors into Err', () async {
      final client = _QueueClient([
        (_) async => _textResponse(422, jsonEncode('Overflow')),
      ]);
      final transport = AllexTransport(
        baseUri: Uri.parse('http://localhost/rpc'),
        client: client,
      );

      final result = await transport.callUnary<_AddResult, String>(
        module: 'arithmetic_v_1',
        method: 'add',
        request: {'a': 1, 'b': 2},
        decode: _AddResult.fromJson,
        decodeError: (json) => json as String,
      );

      expect(result, isA<Err<_AddResult, String>>());
      expect((result as Err<_AddResult, String>).error, 'Overflow');
    });

    test('retries once after auth refresh and uses the new bearer token',
        () async {
      var token = 'old-token';
      var refreshCalls = 0;
      final authHeaders = <String?>[];
      final client = _QueueClient([
        (request) async {
          authHeaders.add(_authorization(request));
          return _textResponse(403, 'forbidden');
        },
        (request) async {
          authHeaders.add(_authorization(request));
          return _textResponse(200, _wrap({'r': 9}));
        },
      ]);
      final transport = AllexTransport(
        baseUri: Uri.parse('http://localhost/rpc'),
        bearerToken: () => token,
        refreshBearerToken: (failure) {
          refreshCalls += 1;
          expect(failure.module, 'arithmetic_v_1');
          expect(failure.method, 'mul');
          expect(failure.statusCode, 403);
          token = 'new-token';
          return true;
        },
        client: client,
      );

      final result = await transport.callUnary<_AddResult, String>(
        module: 'arithmetic_v_1',
        method: 'mul',
        request: {'a': 3, 'b': 3},
        decode: _AddResult.fromJson,
        decodeError: (json) => json as String,
      );

      expect(refreshCalls, 1);
      expect(authHeaders, ['Bearer old-token', 'Bearer new-token']);
      expect((result as Ok<_AddResult, String>).value.r, 9);
    });

    test('reports the server clock from the response Date header', () async {
      DateTime? observed;
      final client = _QueueClient([
        (_) async => _textResponse(
              200,
              _wrap({'r': 1}),
              headers: const {
                'content-type': 'application/json',
                'date': 'Wed, 21 Oct 2015 07:28:00 GMT',
              },
            ),
      ]);
      final transport = AllexTransport(
        baseUri: Uri.parse('http://localhost/rpc'),
        onServerTime: (t) => observed = t,
        client: client,
      );

      await transport.callUnary<_AddResult, String>(
        module: 'arithmetic_v_1',
        method: 'add',
        request: {'a': 0, 'b': 1},
        decode: _AddResult.fromJson,
        decodeError: (json) => json as String,
      );

      expect(observed, isNotNull);
      expect(observed!.isUtc, isTrue);
      expect(observed, DateTime.utc(2015, 10, 21, 7, 28, 0));
    });

    test('ignores a missing or unparseable Date header', () async {
      var calls = 0;
      final client = _QueueClient([
        (_) async => _textResponse(200, _wrap({'r': 1})), // no Date header
        (_) async => _textResponse(
              200,
              _wrap({'r': 1}),
              headers: const {'date': 'not-a-date'},
            ),
      ]);
      final transport = AllexTransport(
        baseUri: Uri.parse('http://localhost/rpc'),
        onServerTime: (_) => calls += 1,
        client: client,
      );

      for (final _ in [0, 1]) {
        await transport.callUnary<_AddResult, String>(
          module: 'arithmetic_v_1',
          method: 'add',
          request: {'a': 0, 'b': 1},
          decode: _AddResult.fromJson,
          decodeError: (json) => json as String,
        );
      }

      expect(calls, 0);
    });
  });

  group('AllexTransport.callStreaming', () {
    test('emits progress and success frames from NDJSON responses', () async {
      final progressFrame = jsonEncode({
        'Progress': jsonDecode(_wrap({'currently_at': 1})),
      });
      final successFrame = jsonEncode({
        'Success': jsonDecode(_wrap({'total': 4})),
      });
      final client = _QueueClient([
        (_) async => _chunkedResponse(200, [
              '$progressFrame\n$successFrame\n',
            ]),
      ]);
      final transport = AllexTransport(
        baseUri: Uri.parse('http://localhost/rpc'),
        client: client,
      );

      final events = await transport
          .callStreaming<_CountStep, _StepsMade, String>(
            module: 'arithmetic_v_1',
            method: 'count',
            request: {'from': 1, 'to': 4},
            decodeProgress: _CountStep.fromJson,
            decodeSuccess: _StepsMade.fromJson,
            decodeError: (json) => json as String,
          )
          .toList();

      expect(events, hasLength(2));
      expect(
          events.first, isA<AllexProgress<_CountStep, _StepsMade, String>>());
      expect(
        (events.first as AllexProgress<_CountStep, _StepsMade, String>)
            .value
            .currentlyAt,
        1,
      );
      expect(events.last, isA<AllexSuccess<_CountStep, _StepsMade, String>>());
      expect(
        (events.last as AllexSuccess<_CountStep, _StepsMade, String>)
            .value
            .total,
        4,
      );
    });
  });
}
