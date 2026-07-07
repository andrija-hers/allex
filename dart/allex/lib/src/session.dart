/// Password-based auth session for allex services that expose a REST
/// `/api/login` endpoint and an `identity_v_1.refresh` RPC.
///
/// [AllexTransport] is auth-agnostic: it asks for a bearer token via callbacks
/// and refreshes via a callback on 401/403. [AllexPasswordSession] bridges a
/// `handle`/`password` credential to those callbacks, so application code can
/// wire a logged-in transport in two lines:
///
/// ```dart
/// final session = AllexPasswordSession(
///   baseUri: Uri.parse('http://127.0.0.1:8787'),
///   handle: 'admin',
///   password: 'adminpass',
/// );
/// final transport = AllexTransport(
///   baseUri: session.rpcUri,
///   bearerToken: session.bearerToken,
///   refreshBearerToken: session.refreshBearerToken,
/// );
/// ```
library;

import 'dart:convert';

import 'package:http/http.dart' as http;

import 'transport.dart';
import 'types.dart';

class AllexPasswordSession {
  AllexPasswordSession({
    required this.baseUri,
    required this.handle,
    required this.password,
    http.Client? client,
  })  : _client = client ?? http.Client(),
        _ownsClient = client == null;

  /// The allex service root (e.g. `http://127.0.0.1:8787`). The `/rpc` and
  /// `/api/login` endpoints are derived from it.
  final Uri baseUri;
  final String handle;
  final String password;

  final http.Client _client;
  final bool _ownsClient;

  String? _token;
  String? _refreshToken;
  String? _role;

  /// The authenticated caller's role (e.g. `admin` / `user`), learned from the
  /// login/refresh response. `null` until the first successful authentication.
  String? get role => _role;

  String get _root {
    final s = baseUri.toString();
    return s.endsWith('/') ? s.substring(0, s.length - 1) : s;
  }

  /// The RPC endpoint, for wiring an [AllexTransport]'s `baseUri`.
  Uri get rpcUri => Uri.parse('$_root/rpc');
  Uri get _loginUri => Uri.parse('$_root/api/login');

  /// An [AllexBearerTokenProvider]: the current access token, logging in lazily
  /// on first use.
  Future<String?> bearerToken() async {
    await _ensureToken();
    return _token;
  }

  /// An [AllexBearerTokenRefresher]: invoked by [AllexTransport] on a 401/403.
  ///
  /// Drops the stale access token, attempts a refresh-then-login, and reports
  /// whether a fresh token was obtained so the transport retries the call once.
  Future<bool> refreshBearerToken(AllexAuthFailure failure) async {
    _token = null;
    try {
      await _ensureToken();
      return _token != null;
    } catch (_) {
      return false;
    }
  }

  Future<void> _ensureToken() async {
    if (_token != null) return;
    if (_refreshToken != null) {
      try {
        await _refreshAccessToken();
        if (_token != null) return;
      } catch (_) {
        _refreshToken = null;
      }
    }
    await _loginWithPassword();
  }

  Future<void> _loginWithPassword() async {
    final response = await _client.post(
      _loginUri,
      headers: const {'Content-Type': 'application/json'},
      body: jsonEncode({'handle': handle, 'password': password}),
    );
    if (response.statusCode != 200) {
      throw AllexError(
        'Login failed for $handle: ${response.body}',
        httpStatus: response.statusCode,
      );
    }
    _applyTokens(jsonDecode(response.body) as Map<String, dynamic>);
  }

  Future<void> _refreshAccessToken() async {
    final refreshToken = _refreshToken;
    if (refreshToken == null || refreshToken.isEmpty) return;

    // The refresh RPC is unauthenticated (it carries the refresh token in its
    // body). Borrow our http client; do not dispose this throwaway transport,
    // since disposing it would close the shared client.
    final transport = AllexTransport(baseUri: rpcUri, client: _client);
    final result = await transport.callUnary<Map<String, dynamic>, Object?>(
      module: 'identity_v_1',
      method: 'refresh',
      request: {'refresh_token': refreshToken},
      decode: (json) => (json as Map).cast<String, dynamic>(),
      decodeError: (json) => json,
    );
    switch (result) {
      case Ok(value: final body):
        _applyTokens(body);
      case Err():
        throw const AllexError('Token refresh rejected by server');
    }
  }

  void _applyTokens(Map<String, dynamic> body) {
    final token = body['token'] as String?;
    if (token == null || token.isEmpty) {
      throw const AllexError('Auth response did not contain a token');
    }
    _token = token;
    final refresh = body['refresh_token'] as String?;
    if (refresh != null && refresh.isNotEmpty) {
      _refreshToken = refresh;
    }
    final role = body['role'] as String?;
    if (role != null && role.isNotEmpty) {
      _role = role;
    }
  }

  /// Releases the underlying HTTP client, but only if this session created it
  /// (a caller-injected client is left for the caller to close).
  void dispose() {
    if (_ownsClient) _client.close();
  }
}
