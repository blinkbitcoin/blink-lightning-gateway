//! LN max-fee policy. Mirrors galoy's `LnFees().maxProtocolAndBankFee` at
//! `blink/core/api/src/domain/payments/ln-fees.ts:17-35`:
//!
//! `max_fee_sat = max(round_half_down(amount_sat * FEE_CAP_BASIS_POINTS / 10_000),
//!                    FEE_CAP_MIN_SAT)`
//!
//! Math runs in sat-space (matching blink-core's BigInt-sat domain) so the
//! result is identical at the sat boundary.

use crate::primitives::MilliSatoshi;

/// 0.5% expressed in basis points (1 bp = 0.01%). 50 bp = 0.5%.
const FEE_CAP_BASIS_POINTS: u64 = 50;

/// 10 satoshi floor — matches blink-core's `FEECAP_MIN = { amount: 10n }`.
const FEE_CAP_MIN_SAT: u64 = 10;

pub struct LnFees;

impl LnFees {
    /// Returns the maximum permissible LND routing fee for an outbound
    /// payment of `amount_msat`. The input is assumed to be a whole-sat
    /// multiple (entity-level invariant from `decode_bolt11`'s
    /// `round_up_to_sat` ceiling); sub-sat msat in the input is silently
    /// truncated. Round-half-down on the sat-remainder mirrors blink-core's
    /// `divRound`; the 10-sat floor handles micro-payments.
    pub fn max_for(amount_msat: MilliSatoshi) -> MilliSatoshi {
        let amount_sat = amount_msat.whole_sat();
        let scaled = amount_sat.saturating_mul(FEE_CAP_BASIS_POINTS);
        let quotient = scaled / 10_000;
        let pct_sat = if scaled % 10_000 > 5_000 {
            quotient + 1
        } else {
            quotient
        };
        let max_fee_sat = pct_sat.max(FEE_CAP_MIN_SAT);
        MilliSatoshi::new(max_fee_sat * 1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Galoy parity tests. Expected values cross-checked against
    //    `blink/core/api/test/unit/domain/payments/ln-fees.spec.ts`.

    #[test]
    fn small_amount_returns_10_sat_floor() {
        // 100 msat (= 0 whole sat) → percent = 0 → floor wins → 10 sat = 10_000 msat.
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
    fn percent_divides_evenly_no_rounding_applied() {
        // 10_000 sat × 0.5% = 50 sat exactly (mod = 0). No round-half-down
        // decision exercised. Cross-reference: ln-fees.spec.ts "returns
        // the maxProtocolAndBankFee".
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(10_000_000)),
            MilliSatoshi::new(50_000)
        );
    }

    #[test]
    fn round_half_down_below_half_rounds_down() {
        // 25_844 sat × 50 = 1_292_200; / 10_000 = 129 quot, 2200 mod;
        // 2200 > 5000? No → 129 sat. Cross-reference: ln-fees.spec.ts
        // "correctly rounds the fee" (`25_844n sat → 129n sat`).
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(25_844_000)),
            MilliSatoshi::new(129_000)
        );
    }

    #[test]
    fn one_sat_payment_pulled_to_floor() {
        // 1 sat × 50 / 10_000 = 0 sat. Floor wins → 10 sat.
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(1_000)),
            MilliSatoshi::new(10_000)
        );
    }

    #[test]
    fn large_amount_percent_dominates() {
        // 100_000 sat × 0.5% = 500 sat. Above the 10-sat floor.
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(100_000_000)),
            MilliSatoshi::new(500_000)
        );
    }

    #[test]
    fn round_half_down_at_tie_stays_down() {
        // amount_sat × 50 ≡ 5000 (mod 10_000) ⇒ amount_sat ≡ 100 (mod 200).
        // Pick 21_100 sat (above the floor):
        //   21_100 × 50 = 1_055_000; / 10_000 = 105 quot, 5000 mod;
        //   5000 > 5000? No → 105 sat (= 105_000 msat).
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(21_100_000)),
            MilliSatoshi::new(105_000)
        );
    }

    #[test]
    fn round_half_down_above_half_rounds_up() {
        // 21_101 sat × 50 = 1_055_050; / 10_000 = 105 quot, 5050 mod;
        // 5050 > 5000? Yes → 106 sat (= 106_000 msat).
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(21_101_000)),
            MilliSatoshi::new(106_000)
        );
    }

    #[test]
    fn sat_space_math_diverges_from_msat_space_at_the_boundary() {
        // 19_999_999 sat × 50 / 10_000 = 99_999.995 sat. blink-core's
        // sat-space round-half-down sees `mod = 9950 > 5000` → 100_000 sat
        // (= 100_000_000 msat). msat-space math would have given
        // 99_999_995 msat (mod = 0 → no round-up). The sat-space path is
        // what we use to stay bit-for-bit identical with blink-core; this
        // test would have failed under the prior msat-space implementation.
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(19_999_999_000)),
            MilliSatoshi::new(100_000_000)
        );
    }

    #[test]
    fn sub_sat_msat_input_silently_truncated_to_sat() {
        // Documented invariant: `LnFees::max_for` assumes whole-sat msat
        // input. A sub-sat msat input (200_001 msat = 200.001 sat) silently
        // truncates to 200 sat for the percent calculation. Floor wins:
        //   200 × 50 / 10_000 = 1 sat → max(1, 10) = 10 sat.
        assert_eq!(
            LnFees::max_for(MilliSatoshi::new(200_001)),
            MilliSatoshi::new(10_000)
        );
    }
}
