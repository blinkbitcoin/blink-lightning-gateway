//! `Mutation` root. Slice 1a only carries `lnInvoiceCreate`. Resolver
//! validates input scalars (each scalar's `try_from`/parse already does)
//! and routes to `App::create_invoice`; no business logic in the resolver
//! (architecture L348).

use async_graphql::{Context, Object};

use super::types::{
    GraphqlError, LnInvoice, LnInvoiceCreateInput, LnInvoiceFeeProbeInput, LnInvoicePayload,
    LnInvoicePaymentInput, LnPaymentRequest, LnPaymentSecret, PaymentHash as GqlPaymentHash,
    PaymentSendPayload, PaymentSendResult, SatAmount, SatAmountPayload,
};
use crate::app::{App, AppError, FeeProbeRequest, NewInvoiceRequest, SendPaymentRequest};
use crate::payment::{PaymentError, PaymentState};

pub struct Mutation;

#[Object]
impl Mutation {
    async fn ln_invoice_create(
        &self,
        ctx: &Context<'_>,
        input: LnInvoiceCreateInput,
    ) -> async_graphql::Result<LnInvoicePayload> {
        let app = ctx
            .data::<App>()
            .map_err(|_| async_graphql::Error::new("App coordinator not configured"))?;

        // expiry_seconds default: galoy uses 1440 minutes (24h) when not
        // supplied. Bound is 60..=86_400 seconds (Invoice::create enforces).
        let expiry_minutes = input.expires_in.map(|m| m.0).unwrap_or(1440);
        // Saturate-cast to seconds (60 minutes -> 3600 secs); range bounded
        // by the entity validation, so caller-side overflow is rejected.
        let expiry_seconds = expiry_minutes.saturating_mul(60);

        let request = NewInvoiceRequest {
            wallet_id: input.wallet_id.into(),
            amount_msat: input.amount.to_msat(),
            expiry_seconds,
            memo: input.memo.map(|m| m.0),
        };

        match app.create_invoice(request).await {
            Ok(invoice) => Ok(LnInvoicePayload {
                errors: Vec::new(),
                invoice: Some(LnInvoice {
                    payment_hash: GqlPaymentHash(invoice.payment_hash),
                    payment_request: LnPaymentRequest(invoice.bolt_invoice.as_str().to_owned()),
                    payment_secret: LnPaymentSecret(String::new()),
                    satoshis: SatAmount(invoice.amount_msat.as_u64() / 1000),
                }),
            }),
            Err(e) => Ok(LnInvoicePayload {
                errors: vec![GraphqlError::from_message(e.to_string())],
                invoice: None,
            }),
        }
    }

    async fn ln_invoice_payment_send(
        &self,
        ctx: &Context<'_>,
        input: LnInvoicePaymentInput,
    ) -> async_graphql::Result<PaymentSendPayload> {
        let app = ctx
            .data::<App>()
            .map_err(|_| async_graphql::Error::new("App coordinator not configured"))?;

        let request = SendPaymentRequest {
            wallet_id: input.wallet_id.into(),
            payment_request: input.payment_request.0,
            memo: input.memo.map(|m| m.0),
        };

        match app.send_payment(request).await {
            Ok(payment) => {
                let status = match payment.state {
                    PaymentState::Pending | PaymentState::Initiated => PaymentSendResult::Pending,
                    PaymentState::Completed => PaymentSendResult::Success,
                    PaymentState::Failed | PaymentState::Reversed => PaymentSendResult::Failure,
                };
                Ok(PaymentSendPayload {
                    errors: Vec::new(),
                    status: Some(status),
                    transaction: None,
                })
            }
            Err(AppError::Payment(PaymentError::AlreadyPaid { .. })) => Ok(PaymentSendPayload {
                errors: Vec::new(),
                status: Some(PaymentSendResult::AlreadyPaid),
                transaction: None,
            }),
            Err(e) => Ok(PaymentSendPayload {
                errors: vec![GraphqlError::from_message(e.to_string())],
                status: Some(PaymentSendResult::Failure),
                transaction: None,
            }),
        }
    }

    async fn ln_invoice_fee_probe(
        &self,
        ctx: &Context<'_>,
        input: LnInvoiceFeeProbeInput,
    ) -> async_graphql::Result<SatAmountPayload> {
        let app = ctx
            .data::<App>()
            .map_err(|_| async_graphql::Error::new("App coordinator not configured"))?;

        let request = FeeProbeRequest {
            wallet_id: input.wallet_id.into(),
            payment_request: input.payment_request.0,
        };

        match app.fee_probe(request).await {
            Ok(fee_msat) => Ok(SatAmountPayload {
                amount: Some(SatAmount(fee_msat.as_u64() / 1000)),
                errors: Vec::new(),
            }),
            Err(e) => Ok(SatAmountPayload {
                amount: None,
                errors: vec![GraphqlError::from_message(e.to_string())],
            }),
        }
    }
}
