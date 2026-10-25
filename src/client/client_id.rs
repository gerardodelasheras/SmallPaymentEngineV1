/// A client identifier in the in-memory CLI's `u16` namespace.
pub type ClientId = u16;

/// A canonical client identifier minted by the distributed ledger.
///
/// The database uses a `BIGINT`; the shared ledger core is generic over this
/// boundary so the CLI keeps `u16` while the distributed adapter uses `i64`.
pub type CanonicalClientId = i64;
