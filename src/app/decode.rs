//! BOLT11 decoding at the App boundary.

use lightning_invoice::Bolt11Invoice;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::app::AppError;
use crate::payment::entity::DecodedInvoice;
use crate::payment::PaymentError;
use crate::primitives::{BoltInvoice, MilliSatoshi, PaymentHash};

pub fn decode_bolt11(payment_request: &str) -> Result<DecodedInvoice, AppError> {
    let invoice = Bolt11Invoice::from_str(payment_request)
        .map_err(|e| AppError::InvalidBoltInvoice(e.to_string()))?;

    // Expiry check — refuse to send against an already-expired invoice
    // rather than orphan a `Pending` row that LND would reject.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    if invoice.would_expire(now) {
        return Err(AppError::Payment(PaymentError::ExpiredBoltInvoice));
    }

    let hash_slice: &[u8] = invoice.payment_hash().as_ref();
    let hash_bytes: [u8; 32] = hash_slice
        .try_into()
        .map_err(|_| AppError::InvalidBoltInvoice("payment hash not 32 bytes".to_owned()))?;
    let payment_hash = PaymentHash::from(hash_bytes);

    let destination = invoice
        .payee_pub_key()
        .copied()
        .or_else(|| Some(invoice.recover_payee_pub_key()))
        .map(|pk| hex::encode(pk.serialize()))
        .unwrap_or_default();

    // Ceiling-round sub-sat msat amounts to the next whole sat — mirrors
    // blink-core's `safe_tokens` (`blink/core/api/src/domain/bitcoin/
    // lightning/ln-invoice.ts:32-33`). The aggregate's `amount_msat`
    // field then only ever holds whole-sat multiples, so the outbox
    // `amount_sat` and Symphony `sat_amount` (both `msat / 1000`) are
    // lossless. The user is over-held by at most ~1 sat for an invoice
    // that happens to carry sub-sat fragments.
    let amount_msat = invoice
        .amount_milli_satoshis()
        .map(MilliSatoshi::new)
        .map(MilliSatoshi::round_up_to_sat);

    Ok(DecodedInvoice {
        payment_hash,
        destination,
        amount_msat,
        bolt_invoice: BoltInvoice::new(payment_request),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::{sha256, Hash};
    use bitcoin::secp256k1::{Secp256k1, SecretKey};
    use lightning_invoice::{Currency, InvoiceBuilder, PaymentSecret};
    use std::time::Duration;

    /// Build a signed regtest BOLT11. `amount_msat = None` produces an
    /// amountless invoice. Uses the current system time + a 1h expiry so
    /// the `would_expire` guard in `decode_bolt11` does not reject the
    /// test invoice (the guard was added during Story 2.2 review).
    fn make_test_bolt11(amount_msat: Option<u64>, payment_hash_bytes: [u8; 32]) -> String {
        let private_key = SecretKey::from_slice(&[0x42; 32]).unwrap();
        let payment_hash = sha256::Hash::from_slice(&payment_hash_bytes).unwrap();
        let payment_secret = PaymentSecret([0x11; 32]);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        let base = InvoiceBuilder::new(Currency::Regtest)
            .description("decode-test".into())
            .payment_hash(payment_hash)
            .payment_secret(payment_secret)
            .duration_since_epoch(now)
            .expiry_time(Duration::from_secs(3600))
            .min_final_cltv_expiry_delta(144);

        let signed = match amount_msat {
            Some(amt) => base
                .amount_milli_satoshis(amt)
                .build_signed(|h| Secp256k1::new().sign_ecdsa_recoverable(h, &private_key))
                .unwrap(),
            None => base
                .build_signed(|h| Secp256k1::new().sign_ecdsa_recoverable(h, &private_key))
                .unwrap(),
        };
        signed.to_string()
    }

    #[test]
    fn decodes_amount_carrying_invoice() {
        let bolt11 = make_test_bolt11(Some(100_000_000), [0xcc; 32]);
        let decoded = decode_bolt11(&bolt11).unwrap();
        assert_eq!(decoded.payment_hash, PaymentHash::from([0xcc; 32]));
        assert_eq!(decoded.amount_msat, Some(MilliSatoshi::new(100_000_000)));
        assert_eq!(decoded.bolt_invoice.as_str(), &bolt11);
    }

    #[test]
    fn sub_sat_invoice_amount_is_ceil_rounded_to_whole_sat() {
        // BOLT11 amounts can express msat-precise values; decode rounds up
        // to the nearest whole sat so the aggregate's `amount_msat` field
        // only ever holds whole-sat multiples (avoids hold/release msat
        // mismatches at the Symphony boundary).
        let bolt11 = make_test_bolt11(Some(1), [0x01; 32]);
        let decoded = decode_bolt11(&bolt11).unwrap();
        assert_eq!(decoded.amount_msat, Some(MilliSatoshi::new(1000)));
    }

    #[test]
    fn amountless_invoice_returns_none_not_zero() {
        let bolt11 = make_test_bolt11(None, [0xab; 32]);
        let decoded = decode_bolt11(&bolt11).unwrap();
        assert_eq!(decoded.amount_msat, None);
    }

    #[test]
    fn invalid_bolt11_returns_invalid_bolt_invoice_error() {
        let result = decode_bolt11("not-a-bolt11");
        assert!(
            matches!(result, Err(AppError::InvalidBoltInvoice(_))),
            "expected InvalidBoltInvoice, got {result:?}"
        );
    }

    #[test]
    fn destination_recovered_from_signature() {
        // Our builder doesn't set an explicit payee_pub_key field, so the
        // destination MUST come from signature recovery via
        // `recover_payee_pub_key`. Locks in the `.or_else(...)` fallback
        // — without it, every real-world invoice would produce empty
        // destination.
        let bolt11 = make_test_bolt11(Some(1_000), [0xff; 32]);
        let decoded = decode_bolt11(&bolt11).unwrap();
        // Compressed secp256k1 pubkey = 33 bytes → 66 hex chars.
        assert_eq!(
            decoded.destination.len(),
            66,
            "destination={}",
            decoded.destination
        );
    }
}
