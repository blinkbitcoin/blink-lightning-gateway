//! `PaymentError` — typed domain errors for the Payment aggregate.

use thiserror::Error;

use super::entity::PaymentState;

#[derive(Debug, Error)]
pub enum PaymentError {
    #[error("invalid amount — must be > 0")]
    InvalidAmount,

    /// Amountless invoice with no caller-supplied amount. Mirrors
    /// blink-core's `LnPaymentRequestNonZeroAmountRequiredError`.
    #[error("amount required — invoice is amountless and no amount was supplied")]
    AmountRequired,

    /// Amount-carrying invoice plus a caller-supplied amount. Mirrors
    /// blink-core's `LnPaymentRequestZeroAmountRequiredError`.
    #[error("amount overspecified — invoice already commits an amount; supply none")]
    AmountOverspecified,

    #[error("invalid max_fee_msat — must be > 0")]
    InvalidMaxFee,

    #[error("empty bolt invoice")]
    EmptyBoltInvoice,

    #[error("invalid bolt invoice: {0}")]
    InvalidBoltInvoice(String),

    #[error("invalid state transition from {from} attempting {attempted}")]
    InvalidStateTransition {
        from: PaymentState,
        attempted: &'static str,
    },

    #[error(transparent)]
    EsRepo(#[from] es_entity::EsRepoError),
}
