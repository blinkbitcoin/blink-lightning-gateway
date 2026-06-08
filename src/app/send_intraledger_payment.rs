//! Intraledger `lnInvoicePaymentSend` — a wallet-to-wallet transfer that
//! short-circuits LND. The destination invoice was issued by THIS gateway for
//! another Blink wallet, so the funds move as a pure ledger transfer (debit
//! the sender's wallet-liability account, credit the recipient's) and LND is
//! never called for the send. ADR-0007 settle-inline: one synchronous
//! `AuthorizeSpend(intraledger=true)` posts the final SETTLED two-leg journal,
//! the recipient's LND invoice is canceled, and the `Payment` is persisted
//! already terminal (`Completed`) — never `initiated`, no orphan-hold surface.

use chrono::Utc;
use es_entity::Idempotent;

use crate::app::{App, AppError, SendPaymentRequest};
use crate::invoice::{Invoice, InvoiceError, InvoiceState};
use crate::outbox::NewOutboxEvent;
use crate::payment::entity::DecodedInvoice;
use crate::payment::{NewPayment, Payment, PaymentError};
use crate::primitives::{MilliSatoshi, Timestamp};
use crate::symphony::{
    AccountKind, AccountRef, SymphonyAuthorizeRequest, SymphonyAuthorizeStatus, SymphonyError,
};

impl App {
    /// Intraledger transfer use-case (ADR-0007). Reached from
    /// [`App::send_payment`] when the decoded `payment_hash` matches a local
    /// invoice. `recipient_invoice` is that local invoice (its `wallet_id` is
    /// the recipient). Order mirrors galoy's intraledger send
    /// (`send-lightning.ts:716-746`): ownership → guards → `AuthorizeSpend`
    /// (journal) → `cancel_invoice` (LND) → one local tx (`Completed` +
    /// `settle_intraledger` + reporting event).
    pub(crate) async fn send_intraledger_payment(
        &self,
        request: SendPaymentRequest,
        decoded: DecodedInvoice,
        mut recipient_invoice: Invoice,
        now: Timestamp,
    ) -> Result<Payment, AppError> {
        let payment_hash = decoded.payment_hash;
        let sender_wallet_id = request.wallet_id;
        let recipient_wallet_id = recipient_invoice.wallet_id;

        // Sender wallet-ownership is the first gate, exactly as the LN path.
        self.check_wallet_ownership(&request.caller_auth, &sender_wallet_id)
            .await?;

        // Self-payment guard (galoy `SelfPaymentError`,
        // `payment-flow-builder.ts:236-240`). LND never called.
        if recipient_wallet_id == sender_wallet_id {
            ::tracing::warn!(
                payment_hash = %payment_hash.to_hex(),
                wallet_id = %sender_wallet_id,
                correlation_id = %payment_hash.to_hex(),
                "intraledger self-payment rejected"
            );
            return Err(PaymentError::SelfPayment.into());
        }

        // Recipient-state guard (galoy's `.paid` guard, `helpers.ts:88`): only
        // an Open recipient invoice is a valid intraledger target. None of
        // these branches call LND.
        match recipient_invoice.state {
            InvoiceState::Open => {}
            InvoiceState::Settled => {
                return Err(PaymentError::AlreadyPaid {
                    payment_hash: payment_hash.to_hex(),
                }
                .into());
            }
            InvoiceState::Canceled => return Err(PaymentError::RecipientInvoiceCanceled.into()),
            InvoiceState::Held => return Err(PaymentError::RecipientInvoiceInProgress.into()),
        }

        // The recipient invoice carries the gateway-owned preimage for this
        // payment_hash — the truthful proof-of-payment for the sender's
        // terminal `Completed`, even though it is never revealed on the wire.
        let recipient_preimage = recipient_invoice.payment_preimage;

        // Build the (zero-fee) Payment intent up front so an amountless
        // invoice fails fast (AmountRequired) before anything is authorized.
        let new_payment = NewPayment::try_new_intraledger(decoded, sender_wallet_id, now)?;
        let amount_msat = new_payment.amount_msat;
        let amount_sat = amount_msat.whole_sat() as i64;

        // Synchronous AuthorizeSpend, settle-inline (ADR-0007): debit
        // sender + credit recipient posted SETTLED atomically inside this one
        // call. sat_amount == amount (zero-fee); recipient + intraledger flag
        // ride in the generic gateway_metadata, keeping the spend primitive
        // rail-neutral. Fail closed on any error/Declined: LND is never called,
        // no event is emitted, the recipient invoice is not touched, and no
        // Payment is recorded (nothing was persisted yet).
        let gateway_metadata = serde_json::json!({
            "intraledger": true,
            "recipient_wallet_id": recipient_wallet_id.to_string(),
        });
        let symphony_resp = match self
            .symphony
            .authorize_spend(SymphonyAuthorizeRequest {
                correlation_id: payment_hash.to_hex(),
                account: AccountRef {
                    kind: AccountKind::WalletLiability,
                    id: sender_wallet_id.to_string(),
                },
                sat_amount: amount_msat.whole_sat(),
                idempotency_key: payment_hash.to_hex(),
                gateway_metadata,
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                ::tracing::warn!(
                    payment_hash = %payment_hash.to_hex(),
                    wallet_id = %sender_wallet_id,
                    correlation_id = %payment_hash.to_hex(),
                    error = %e,
                    "intraledger AuthorizeSpend failed; declining fail-closed (LND not called)"
                );
                return Err(AppError::Symphony(e));
            }
        };
        if matches!(symphony_resp.status, SymphonyAuthorizeStatus::Declined) {
            ::tracing::warn!(
                payment_hash = %payment_hash.to_hex(),
                wallet_id = %sender_wallet_id,
                correlation_id = %payment_hash.to_hex(),
                decline_reason = ?symphony_resp.decline_reason,
                "intraledger AuthorizeSpend declined; nothing posted (LND not called)"
            );
            return Err(AppError::Symphony(SymphonyError::Declined {
                reason: symphony_resp.decline_reason.unwrap_or_else(|| {
                    crate::symphony::DeclineReason::Other("no reason".to_owned())
                }),
            }));
        }

        // Cancel the recipient's LND-side invoice BEFORE the local commit
        // (galoy order: journal → cancel → mark-paid, `send-lightning.ts:732`).
        // The funds moved in-ledger, so the on-wire invoice must not also be
        // payable; cancel-before-commit leaves no payable-invoice window.
        self.lnd.cancel_invoice(payment_hash).await?;

        // One DB tx: Payment straight to Completed (settle from Initiated, so
        // it never passes through pending), recipient invoice settled
        // intraledger (no accounting event), and the single reporting-only
        // outbox event. Atomic — no other tx ever observes state='initiated'.
        let mut tx = self.pool.begin().await?;
        let mut payment = self
            .payments
            .create_in_op(&mut tx, new_payment)
            .await
            .map_err(PaymentError::from)?;
        match payment.settle(recipient_preimage, MilliSatoshi::ZERO, Vec::new(), now)? {
            Idempotent::Executed(()) => {}
            // Unreachable on a fresh create, but never overwrite a terminal row.
            Idempotent::AlreadyApplied => {}
        }
        self.payments
            .update_in_op(&mut tx, &mut payment)
            .await
            .map_err(PaymentError::from)?;

        match recipient_invoice.settle_intraledger(now)? {
            Idempotent::Executed(()) => {}
            Idempotent::AlreadyApplied => {}
        }
        self.invoices
            .update_in_op(&mut tx, &mut recipient_invoice)
            .await
            .map_err(InvoiceError::from)?;

        self.outbox
            .publish_in_tx(
                &mut tx,
                NewOutboxEvent::for_lightning_intraledger_transfer_completed(
                    payment_hash.to_hex(),
                    payment_hash.to_hex(),
                    amount_sat,
                    Utc::now(),
                    serde_json::json!({
                        "intraledger": true,
                        "sender_wallet_id": sender_wallet_id.to_string(),
                        "recipient_wallet_id": recipient_wallet_id.to_string(),
                        "amount_msat": amount_msat.as_u64(),
                    }),
                ),
            )
            .await?;
        tx.commit().await?;

        ::tracing::info!(
            payment_hash = %payment_hash.to_hex(),
            wallet_id = %sender_wallet_id,
            correlation_id = %payment_hash.to_hex(),
            recipient_wallet_id = %recipient_wallet_id,
            "intraledger transfer completed"
        );
        Ok(payment)
    }
}
