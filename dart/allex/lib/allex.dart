/// allex Dart transport library.
///
/// Import this package from generated clients:
///
/// ```dart
/// import 'package:allex/allex.dart';
/// ```
///
/// Beyond the transport used by generated clients, this package provides:
/// - [AllexPasswordSession]: bridges a `handle`/`password` credential to the
///   transport's bearer-token callbacks (`/api/login` + `identity_v_1.refresh`).
/// - [LiveStateRepository] / [HasUpdate]: a reconnecting live-state stream that
///   surfaces connection lifecycle ([AllexLiveStatus]) instead of failing
///   silently — the Dart mirror of the Rust `allex_state` design.
library allex;

export 'src/session.dart' show AllexPasswordSession;
export 'src/state.dart' show AllexLiveStatus, HasUpdate, LiveStateRepository;
export 'src/transport.dart'
    show
        AllexAuthFailure,
        AllexBearerTokenProvider,
        AllexBearerTokenRefresher,
        AllexTransport;
export 'src/types.dart'
    show
        AllexError,
        AllexFailed,
        AllexProgress,
        AllexResult,
        AllexStep,
        AllexSuccess,
        Err,
        Ok;
