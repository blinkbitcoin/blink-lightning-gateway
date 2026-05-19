//! Centralized `tonic::Status` mapping for the gateway's gRPC surface.
//!
//! Per CLAUDE.md ("gRPC `Status` mapping centralized in `src/api/grpc/error.rs`")
//! the gRPC layer must never construct a `Status` ad-hoc. Use the typed
//! domain errors and let `From<...> for tonic::Status` here own the
//! status-code choice and the operator-facing message.
//!
//! Mapping policy (mirrors `blink-card`'s status conventions):
//!   - Database / serialization / unknown event-type / listen disconnect
//!     → `Status::unavailable` (transient infra; client should retry).
//!   - Outbox configuration error → `Status::failed_precondition`
//!     (operator misconfiguration, retry will not help).
//!   - Wallet ownership check failure → `Status::permission_denied`.
//!   - Domain validation errors (`InvoiceError`, `LndError`) →
//!     `Status::invalid_argument` for caller-visible variants;
//!     `Status::internal` otherwise.

use tonic::Status;

use crate::app::AppError;
use crate::outbox::OutboxError;

impl From<OutboxError> for Status {
    fn from(err: OutboxError) -> Self {
        match err {
            OutboxError::Configuration(msg) => {
                Status::failed_precondition(format!("outbox listener misconfigured: {msg}"))
            }
            OutboxError::ListenDisconnected => Status::unavailable("outbox LISTEN connection lost"),
            OutboxError::Db(_) | OutboxError::Metadata(_) | OutboxError::UnknownEventType(_) => {
                ::tracing::error!(error = %err, "outbox error surfaced to gRPC layer");
                Status::unavailable(format!("outbox subsystem unavailable: {err}"))
            }
        }
    }
}

impl From<AppError> for Status {
    fn from(err: AppError) -> Self {
        match err {
            AppError::WalletOwnership(msg) => Status::permission_denied(msg),
            AppError::Invoice(inner) => Status::invalid_argument(inner.to_string()),
            // PaymentError covers a mix of caller-visible validation
            // failures and infra/concurrency failures wrapped via
            // `EsRepoError`. Surface the latter as `unavailable` (the
            // gRPC contract for retryable errors) so a transient DB
            // hiccup or ConcurrentModification doesn't look like client
            // misuse.
            AppError::Payment(crate::payment::PaymentError::EsRepo(inner)) => {
                ::tracing::error!(error = %inner, "payment EsRepo error surfaced to gRPC layer");
                Status::unavailable(inner.to_string())
            }
            AppError::Payment(inner) => Status::invalid_argument(inner.to_string()),
            AppError::InvalidBoltInvoice(msg) => {
                Status::invalid_argument(format!("invalid bolt invoice: {msg}"))
            }
            AppError::Lnd(inner) => {
                ::tracing::error!(error = %inner, "LND error surfaced to gRPC layer");
                Status::unavailable(inner.to_string())
            }
            AppError::Symphony(inner) => {
                ::tracing::error!(error = %inner, "Symphony error surfaced to gRPC layer");
                Status::unavailable(inner.to_string())
            }
            AppError::Outbox(inner) => Status::from(inner),
            AppError::Db(inner) => {
                ::tracing::error!(error = %inner, "database error surfaced to gRPC layer");
                Status::unavailable(format!("database unavailable: {inner}"))
            }
        }
    }
}
