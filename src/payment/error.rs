//! `PaymentError` — typed domain errors for the Payment aggregate.

use thiserror::Error;

use super::entity::PaymentState;
use super::repo::{
    PaymentColumn, PaymentCreateError, PaymentFindError, PaymentModifyError, PaymentQueryError,
};

#[derive(Debug, Error)]
pub enum PaymentError {
    #[error("invalid amount — must be > 0")]
    InvalidAmount,

    #[error("amount required — invoice is amountless and no amount was supplied")]
    AmountRequired,

    #[error("amount overspecified — invoice already commits an amount; supply none")]
    AmountOverspecified,

    #[error("invalid max_fee_msat — must be > 0")]
    InvalidMaxFee,

    #[error("empty bolt invoice")]
    EmptyBoltInvoice,

    #[error("invalid bolt invoice: {0}")]
    InvalidBoltInvoice(String),

    #[error("bolt invoice expired")]
    ExpiredBoltInvoice,

    #[error("payment already exists for payment_hash {payment_hash}")]
    AlreadyPaid { payment_hash: String },

    #[error("cannot pay an invoice issued for your own wallet (intraledger self-payment)")]
    SelfPayment,

    #[error("recipient invoice was canceled and is no longer payable")]
    RecipientInvoiceCanceled,

    #[error("recipient invoice has a payment in progress")]
    RecipientInvoiceInProgress,

    #[error("invalid state transition from {from} attempting {attempted}")]
    InvalidStateTransition {
        from: PaymentState,
        attempted: &'static str,
    },

    #[error("corrupt payment event log")]
    CorruptEventLog,

    #[error(transparent)]
    PaymentCreate(PaymentCreateError),
    #[error(transparent)]
    PaymentModify(#[from] PaymentModifyError),
    #[error(transparent)]
    PaymentFind(#[from] PaymentFindError),
    #[error(transparent)]
    PaymentQuery(#[from] PaymentQueryError),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

// Intercept the `payment_hash` UNIQUE-violation on create and lift it into
// the domain-meaningful `AlreadyPaid` variant.
impl From<PaymentCreateError> for PaymentError {
    fn from(error: PaymentCreateError) -> Self {
        match error {
            PaymentCreateError::ConstraintViolation {
                column: Some(PaymentColumn::PaymentHash),
                value,
                ..
            } => Self::AlreadyPaid {
                payment_hash: value.unwrap_or_default(),
            },
            other => Self::PaymentCreate(other),
        }
    }
}
