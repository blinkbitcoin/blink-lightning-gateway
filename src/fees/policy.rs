//! LN max-fee policy. Mirrors galoy's `LnFees().maxProtocolAndBankFee` at
//! `blink/core/api/src/domain/payments/ln-fees.ts:17-35`:
//!
//! `max_fee = max(round_half_down(amount_sat * FEE_CAP_BASIS_POINTS / 10_000),
//!                FEE_CAP_MIN_SAT)`.

use crate::primitives::MilliSatoshi;

/// 0.5% expressed in basis points (1 bp = 0.01%). 50 bp = 0.5%.
const FEE_CAP_BASIS_POINTS: u64 = 50;

/// 10 satoshi floor, denominated in msat.
const FEE_CAP_MIN_MSAT: u64 = 10_000;

pub struct LnFees;

impl LnFees {
    /// Returns the maximum permissible LND routing fee for an outbound
    /// payment of `amount_msat`. Round-half-down on the fractional msat
    /// to match galoy's `divRound`; the 10-sat floor handles micro-payments.
    pub fn max_for(amount_msat: MilliSatoshi) -> MilliSatoshi {
        let amount = amount_msat.as_u64();
        // round-half-down: scaled / 10_000, with `mod > 5000` bumping up.
        let scaled = amount.saturating_mul(FEE_CAP_BASIS_POINTS);
        let quotient = scaled / 10_000;
        let pct = if scaled % 10_000 > 5_000 {
            quotient + 1
        } else {
            quotient
        };
        MilliSatoshi::new(pct.max(FEE_CAP_MIN_MSAT))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Galoy parity tests. Expected values cross-checked against
    //    `blink/core/api/test/unit/domain/payments/ln-fees.spec.ts`:
    //    10_000 sat → 50 sat, micro-payments → FEECAP_MIN (10 sat).

    #[test]
    fn small_amount_returns_10_sat_floor() {
        // 100 msat * 0.5% = 0.5 msat → rounds to 0 → floor of 10 sat = 10_000 msat.
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(100)),
            MilliSatoshi::new(10_000)
        );
    }

    #[test]
    fn zero_returns_10_sat_floor() {
        assert_eq!(
            LnFees::max_for(MilliSatoshi::ZERO),
            MilliSatoshi::new(10_000)
        );
    }

    #[test]
    fn matches_galoy_test_10000_sat_to_50_sat() {
        // Galoy test: `btcAmount = 10_000n sat` → `maxFee = 50n sat`.
        // In msat: 10_000_000 msat → 50_000 msat.
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(10_000_000)),
            MilliSatoshi::new(50_000)
        );
    }

    #[test]
    fn one_sat_payment_pulled_to_floor() {
        // 1 sat = 1000 msat. 1000 * 50 / 10_000 = 5 msat. floor wins → 10_000.
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(1_000)),
            MilliSatoshi::new(10_000)
        );
    }

    #[test]
    fn large_amount_percent_dominates() {
        // 100_000_000 msat (100k sats) * 0.5% = 500_000 msat (= 500 sat).
        // Well above the 10_000-msat floor.
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(100_000_000)),
            MilliSatoshi::new(500_000)
        );
    }

    #[test]
    fn round_half_down_below_half() {
        // 200_001 msat * 50 = 10_000_050. quotient=1000, mod=50.
        // 50 > 5000? No → round down → 1000. Below floor → floor wins → 10_000.
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(200_001)),
            MilliSatoshi::new(10_000)
        );
    }

    #[test]
    fn round_half_down_at_tie() {
        // For `mod = 5000`: scaled = k*10_000 + 5000 ⇒ amount * 50 ≡ 5000 (mod 10_000)
        // ⇒ amount ≡ 100 (mod 200). Try amount = 21_100 msat:
        //   21_100 * 50 = 1_055_000. quotient=105, mod=5000.
        //   5000 > 5000? No → round down → 105.
        // Floor still wins (105 < 10_000) → 10_000.
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(21_100)),
            MilliSatoshi::new(10_000)
        );

        // Pick a larger amount past the floor: 21_100_100 msat.
        //   21_100_100 * 50 = 1_055_005_000. quotient=105_500, mod=5000.
        //   5000 > 5000? No → round down → 105_500. Above floor → 105_500.
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(21_100_100)),
            MilliSatoshi::new(105_500)
        );
    }

    #[test]
    fn round_half_down_above_half_rounds_up() {
        // mod = 5001 should round up. amount = 21_100_101 msat:
        //   21_100_101 * 50 = 1_055_005_050. quotient=105_500, mod=5050.
        //   5050 > 5000? Yes → 105_501.
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(21_100_101)),
            MilliSatoshi::new(105_501)
        );
    }
}
