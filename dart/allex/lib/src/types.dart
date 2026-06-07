/// Core types shared by all generated allex clients.

// ---------------------------------------------------------------------------
// Unary call result
// ---------------------------------------------------------------------------

/// Returned by every unary RPC call.
///
/// Either `Ok(value)` or `Err(error)` — never both.
sealed class AllexResult<T, E> {
  const AllexResult();
}

final class Ok<T, E> extends AllexResult<T, E> {
  final T value;
  const Ok(this.value);
}

final class Err<T, E> extends AllexResult<T, E> {
  final E error;
  const Err(this.error);
}

// ---------------------------------------------------------------------------
// Streaming call events
// ---------------------------------------------------------------------------

/// Emitted by every streaming RPC call.
sealed class AllexStep<P, S, E> {
  const AllexStep();
}

final class AllexProgress<P, S, E> extends AllexStep<P, S, E> {
  final P value;
  const AllexProgress(this.value);
}

final class AllexSuccess<P, S, E> extends AllexStep<P, S, E> {
  final S value;
  const AllexSuccess(this.value);
}

final class AllexFailed<P, S, E> extends AllexStep<P, S, E> {
  final E error;
  const AllexFailed(this.error);
}

// ---------------------------------------------------------------------------
// Transport-level errors (not application errors — those live in generated E)
// ---------------------------------------------------------------------------

/// Thrown when the HTTP connection itself fails or the server returns a
/// non-200 status for a call that was never even dispatched.
class AllexError implements Exception {
  final String message;
  final int? httpStatus;

  const AllexError(this.message, {this.httpStatus});

  @override
  String toString() => httpStatus != null
      ? 'AllexError($httpStatus): $message'
      : 'AllexError: $message';
}
