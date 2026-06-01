//! `lnInvoiceCreate` use-case: every gateway invoice is a
//! HODL invoice — the gateway generates the 32-byte preimage,
//! derives `payment_hash = SHA256(preimage)`, and calls LND's
//! `Invoices/AddHoldInvoice`.

use crate::app::{App, AppError, Mode, NewInvoiceRequest};
use crate::invoice::{Invoice, InvoiceError, NewInvoice};
use crate::lnd::AddHoldInvoiceParams;
use crate::primitives::{PaymentHash, Preimage, Timestamp};

/// blink-core's `checkedToLedgerExternalId` regex `^[a-z0-9_-]{1,100}$`
/// (`domain/ledger/validation.ts`), hand-checked to avoid a `regex`
/// dependency. The char class is ASCII-only, so `str::len()` (bytes) equals
/// the char count for any string that could pass the per-char check.
fn is_valid_external_id(s: &str) -> bool {
    (1..=100).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

/// Resolve + validate the `external_id` for a new invoice:
/// use the client-supplied value, else default to the lowercase-hex
/// `payment_hash` (matches blink-core `wallet-invoice-builder.ts:148-150`).
fn resolve_external_id(
    supplied: Option<String>,
    payment_hash: &PaymentHash,
) -> Result<String, InvoiceError> {
    let external_id = supplied.unwrap_or_else(|| payment_hash.to_hex());
    if is_valid_external_id(&external_id) {
        Ok(external_id)
    } else {
        Err(InvoiceError::InvalidExternalId(external_id))
    }
}

impl App {
    pub async fn create_invoice(&self, request: NewInvoiceRequest) -> Result<Invoice, AppError> {
        let now = Timestamp::now();
        self.check_wallet_ownership(&request.caller_auth, &request.wallet_id)
            .await?;

        // Gateway-owned preimage + derived payment_hash
        let payment_preimage = Preimage::generate();
        let payment_hash = payment_preimage.payment_hash();

        let external_id = resolve_external_id(request.external_id, &payment_hash)?;

        let lnd_resp = self
            .lnd
            .add_hold_invoice(AddHoldInvoiceParams {
                payment_hash,
                amount_msat: Some(request.amount_msat),
                memo: request.memo,
                expiry_seconds: request.expiry_seconds,
            })
            .await?;

        let new_invoice = NewInvoice::try_new(
            payment_hash,
            payment_preimage,
            request.wallet_id,
            Some(request.amount_msat),
            request.expiry_seconds,
            lnd_resp.bolt_invoice,
            external_id,
            now,
        )?;

        if matches!(self.mode, Mode::DryRun) {
            return Err(AppError::WalletOwnership(
                "DryRun mode not yet wired in slice 1a".to_owned(),
            ));
        }

        let invoice = self
            .invoices
            .create(new_invoice)
            .await
            .map_err(InvoiceError::from)?;

        // Spawn the per-hash `subscribe_invoice` listener
        self.invoice_dispatcher
            .spawn_listener_for(invoice.payment_hash);

        Ok(invoice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_id_defaults_to_lowercase_hex_payment_hash() {
        // AC17: omitted external_id defaults to the 64-char lowercase-hex
        // payment_hash (core validates against /^[a-z0-9_-]{1,100}$/, so the
        // lowercase form is load-bearing). Guards against a default that
        // changes form (uppercase, a UUID, the bytes verbatim) — and proves
        // the default always passes the format check.
        let payment_hash = PaymentHash::from([0xab; 32]);
        let resolved = resolve_external_id(None, &payment_hash).expect("default is valid");
        assert_eq!(resolved, "ab".repeat(32));
        assert_eq!(resolved.len(), 64);
    }

    #[test]
    fn default_from_a_real_generated_preimage_always_passes_the_regex() {
        // The omitted-external_id default runs the real production pipeline:
        // `Preimage::generate()` → `payment_hash()` → `to_hex()`. If that ever
        // produced a value outside `^[a-z0-9_-]{1,100}$` (uppercase hex, wrong
        // length), EVERY default-external_id invoice would be rejected at the
        // format check. Generate is random, so sweep a batch to assert the
        // property holds for any preimage, not one fixed byte array.
        for _ in 0..256 {
            let payment_hash = Preimage::generate().payment_hash();
            let resolved =
                resolve_external_id(None, &payment_hash).expect("defaulted hash must be valid");
            assert_eq!(resolved.len(), 64, "SHA-256 hex is always 64 chars");
            assert!(
                is_valid_external_id(&resolved),
                "generated default {resolved:?} failed the external_id regex"
            );
        }
    }

    #[test]
    fn external_id_passes_through_when_supplied_and_valid() {
        let payment_hash = PaymentHash::from([0xab; 32]);
        let resolved =
            resolve_external_id(Some("client-key_1".to_owned()), &payment_hash).expect("valid");
        assert_eq!(resolved, "client-key_1");
    }

    #[test]
    fn supplied_external_id_failing_the_regex_is_rejected() {
        // Mirrors blink-core's `checkedToLedgerExternalId`. A malformed
        // client value must fail loudly (rejected before LND/persistence),
        // not be silently stored.
        let payment_hash = PaymentHash::from([0xab; 32]);
        for bad in [
            "",               // empty (regex requires >=1)
            "UPPER",          // uppercase not allowed
            "has space",      // space not allowed
            "emoji😀",        // non-ascii
            "with.dot",       // dot not allowed
            &"a".repeat(101), // exceeds 100
        ] {
            let err = resolve_external_id(Some(bad.to_owned()), &payment_hash).unwrap_err();
            assert!(
                matches!(err, InvoiceError::InvalidExternalId(_)),
                "expected InvalidExternalId for {bad:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn boundary_lengths_accepted() {
        // 1 and 100 chars are in-range; the helper's bound is inclusive.
        assert!(is_valid_external_id("a"));
        assert!(is_valid_external_id(&"a".repeat(100)));
        assert!(!is_valid_external_id(&"a".repeat(101)));
        assert!(!is_valid_external_id(""));
    }
}
