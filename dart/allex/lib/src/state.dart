/// Live state-sync on top of [AllexTransport].
///
/// Mirrors the Rust `allex_state` design: [HasUpdate] is the Dart equivalent of
/// the `HasUpdate` trait, and [LiveStateRepository] owns the reconnect loop and
/// the last-known state. Unlike a naive `await for` over a watch stream, it
/// surfaces an explicit [AllexLiveStatus] and records the last error instead of
/// disconnecting silently.
library;

import 'dart:async';

import 'transport.dart';
import 'types.dart';

/// Dart mirror of the Rust `HasUpdate` trait.
///
/// Implement this to fold a stream of watch frames into a single state value of
/// type [S]. If the server always sends full snapshots, leave [applyUpdate] as
/// its default (full replacement); override it for delta framing.
abstract class HasUpdate<S> {
  const HasUpdate();

  /// Deserialise a full-state frame into [S].
  S fromJson(Map<String, dynamic> json);

  /// Fold an incremental update into [current], returning the next state.
  ///
  /// Default: treat every frame as a full replacement.
  S applyUpdate(S current, Map<String, dynamic> update) => fromJson(update);
}

/// Connection lifecycle of a [LiveStateRepository].
///
/// - [connecting]: establishing the first connection (no state yet).
/// - [connected]: a frame has arrived and the stream is open.
/// - [reconnecting]: the stream dropped after connecting; retrying after a
///   delay. The cause is available on [LiveStateRepository.lastError].
/// - [disposed]: the repository has been disposed.
enum AllexLiveStatus { connecting, connected, reconnecting, disposed }

/// Streams live state from an allex service watch method.
///
/// - Owns the reconnect loop (with [reconnectDelay]).
/// - Caches the last known state so late subscribers see a value immediately.
/// - Supports full-snapshot or delta ([HasUpdate.applyUpdate]) framing.
/// - Publishes connection lifecycle on [statusStream] / [status] and records
///   the most recent failure on [lastError].
class LiveStateRepository<S> {
  LiveStateRepository({
    required AllexTransport transport,
    required String module,
    required String watchMethod,
    required HasUpdate<S> hasUpdate,
    dynamic watchRequest = const <String, dynamic>{},
    Duration reconnectDelay = const Duration(seconds: 3),
  })  : _transport = transport,
        _module = module,
        _watchMethod = watchMethod,
        _hasUpdate = hasUpdate,
        _watchRequest = watchRequest,
        _reconnectDelay = reconnectDelay;

  final AllexTransport _transport;
  final String _module;
  final String _watchMethod;
  final HasUpdate<S> _hasUpdate;
  final dynamic _watchRequest;
  final Duration _reconnectDelay;

  final _stateController = StreamController<S>.broadcast();
  final _statusController = StreamController<AllexLiveStatus>.broadcast();

  S? _lastKnown;
  Object? _lastError;
  AllexLiveStatus _status = AllexLiveStatus.connecting;
  bool _everConnected = false;
  bool _disposed = false;
  bool _started = false;

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /// The most recently observed state, or `null` before the first frame.
  S? get lastKnown => _lastKnown;

  /// The cause of the most recent disconnect, or `null` while healthy.
  Object? get lastError => _lastError;

  /// The current connection status.
  AllexLiveStatus get status => _status;

  /// The live stream of state values. Broadcasts; each new listener immediately
  /// receives the last known state (if any), then live updates. The underlying
  /// connection is established lazily on first listen.
  Stream<S> get stream {
    _ensureStarted();
    return Stream<S>.multi((listener) {
      final last = _lastKnown;
      if (last != null) listener.add(last);
      final sub = _stateController.stream.listen(
        listener.add,
        onError: listener.addError,
        onDone: listener.close,
      );
      listener.onCancel = sub.cancel;
    });
  }

  /// The live stream of connection status. Broadcasts; each new listener
  /// immediately receives the current status, then transitions.
  Stream<AllexLiveStatus> get statusStream {
    _ensureStarted();
    return Stream<AllexLiveStatus>.multi((listener) {
      listener.add(_status);
      final sub = _statusController.stream.listen(
        listener.add,
        onError: listener.addError,
        onDone: listener.close,
      );
      listener.onCancel = sub.cancel;
    });
  }

  /// Disposes the repository, stopping the reconnect loop and closing streams.
  void dispose() {
    if (_disposed) return;
    _disposed = true;
    _setStatus(AllexLiveStatus.disposed);
    _stateController.close();
    _statusController.close();
  }

  // -------------------------------------------------------------------------
  // Internal connection management
  // -------------------------------------------------------------------------

  void _ensureStarted() {
    if (_started || _disposed) return;
    _started = true;
    _runLoop();
  }

  void _setStatus(AllexLiveStatus next) {
    if (_status == next) return;
    _status = next;
    if (!_statusController.isClosed) _statusController.add(next);
  }

  Future<void> _runLoop() async {
    while (!_disposed) {
      _setStatus(_everConnected
          ? AllexLiveStatus.reconnecting
          : AllexLiveStatus.connecting);
      try {
        S? current = _lastKnown;
        await for (final raw
            in _transport.watchRaw(_module, _watchMethod, _watchRequest)) {
          if (_disposed) return;
          final frame = _asJsonObject(raw);
          final next = current == null
              ? _hasUpdate.fromJson(frame)
              : _hasUpdate.applyUpdate(current, frame);
          current = next;
          _lastKnown = next;
          _lastError = null;
          _everConnected = true;
          _setStatus(AllexLiveStatus.connected);
          if (!_stateController.isClosed) _stateController.add(next);
        }
        // Stream closed by the server without an error frame → reconnect.
      } catch (error) {
        _lastError = error;
      }
      if (_disposed) break;
      await Future<void>.delayed(_reconnectDelay);
    }
  }

  static Map<String, dynamic> _asJsonObject(Object? raw) {
    if (raw is Map<String, dynamic>) return raw;
    if (raw is Map) {
      return raw.map((key, value) => MapEntry(key.toString(), value));
    }
    throw AllexError('Expected a JSON object state frame, got: $raw');
  }
}
