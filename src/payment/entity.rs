//! `Payment` aggregate — event-sourced via `es-entity` derive macros.
//!
//! Symmetric outbound counterpart to `Invoice`. `NewPayment::try_new`
//! is the validating constructor; it accepts a pre-validated
//! `DecodedInvoice` so BOLT11 parsing stays at the GraphQL/App
//! boundary and the entity remains infrastructure-free.
//!
//! Command methods (`mark_pending`, `settle`, `fail`, `reverse`) take
//! `&mut self`, mutate projected state, and push the event before
//! returning — canonical es-entity pattern.
//!
//! Return is `Result<Idempotent<()>, PaymentError>`:
//! - `Ok(Idempotent::Executed(()))` — first application; caller persists.
//! - `Ok(Idempotent::Ignored)` — duplicate replay (LND stream reconnect).
//! - `Err(InvalidStateTransition)` — genuine contradiction (e.g. settle
//!   after a Failed event).

use es_entity::{
    idempotency_guard, EntityEvents, EsEntity, EsEntityError, Idempotent, IntoEvents, TryFromEvents,
};
use serde::{Deserialize, Serialize};
use std::fmt;

use super::error::PaymentError;
use super::event::{FailureReason, Hop, PaymentEvent};
use crate::primitives::{
    BoltInvoice, MilliSatoshi, PaymentHash, PaymentId, Preimage, Timestamp, WalletId,
};

/// Validated BOLT11 decode result passed into `NewPayment::try_new`.
/// Constructed at the App boundary (see `App::send_payment`); the
/// entity does not depend on the `lightning-invoice` crate.
///
/// `amount_msat` is `Option` because BOLT11 invoices may omit the
/// amount field ("amountless" / no-amount invoices). It carries what
/// the wire said, nothing more — `NewPayment::try_new` resolves the
/// actual payment amount from either this or a caller-supplied amount.
#[derive(Clone, Debug)]
pub struct DecodedInvoice {
    pub payment_hash: PaymentHash,
    pub destination: String,
    pub amount_msat: Option<MilliSatoshi>,
    pub bolt_invoice: BoltInvoice,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "TEXT", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum PaymentState {
    Initiated,
    Pending,
    Completed,
    Failed,
    Reversed,
}

impl PaymentState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Initiated => "initiated",
            Self::Pending => "pending",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Reversed => "reversed",
        }
    }
}

impl fmt::Display for PaymentState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug)]
pub struct NewPayment {
    pub id: PaymentId,
    pub payment_hash: PaymentHash,
    pub wallet_id: WalletId,
    pub amount_msat: MilliSatoshi,
    pub max_fee_msat: MilliSatoshi,
    pub bolt_invoice: BoltInvoice,
    pub destination: String,
    pub initiated_at: Timestamp,
}

impl NewPayment {
    /// Validating constructor.
    ///
    /// The payment amount is resolved from exactly one of two sources,
    /// mirroring blink-core's split between `payInvoiceByWalletId` and
    /// `payNoAmountInvoiceByWalletId` (`core/api/src/app/payments/
    /// send-lightning.ts`):
    ///
    /// - **amount-carrying invoice** — `decoded.amount_msat` is `Some`,
    ///   `requested_amount_msat` MUST be `None`. The caller cannot
    ///   override an amount the invoice already commits to.
    /// - **amountless invoice** — `decoded.amount_msat` is `None`,
    ///   `requested_amount_msat` MUST be `Some`. The caller supplies it.
    pub fn try_new(
        decoded: DecodedInvoice,
        wallet_id: WalletId,
        requested_amount_msat: Option<MilliSatoshi>,
        max_fee_msat: MilliSatoshi,
        initiated_at: Timestamp,
    ) -> Result<Self, PaymentError> {
        let amount_msat = match (decoded.amount_msat, requested_amount_msat) {
            (Some(invoice_amount), None) => invoice_amount,
            (None, Some(requested)) => requested,
            (Some(_), Some(_)) => return Err(PaymentError::AmountOverspecified),
            (None, None) => return Err(PaymentError::AmountRequired),
        };
        if amount_msat.as_u64() == 0 {
            return Err(PaymentError::InvalidAmount);
        }
        if max_fee_msat.as_u64() == 0 {
            return Err(PaymentError::InvalidMaxFee);
        }
        if decoded.bolt_invoice.as_str().is_empty() {
            return Err(PaymentError::EmptyBoltInvoice);
        }
        Ok(Self {
            id: PaymentId::new(),
            payment_hash: decoded.payment_hash,
            wallet_id,
            amount_msat,
            max_fee_msat,
            bolt_invoice: decoded.bolt_invoice,
            destination: decoded.destination,
            initiated_at,
        })
    }

    pub fn state_str(&self) -> String {
        PaymentState::Initiated.as_str().to_owned()
    }
}

impl IntoEvents<PaymentEvent> for NewPayment {
    fn into_events(self) -> EntityEvents<PaymentEvent> {
        EntityEvents::init(
            self.id,
            [PaymentEvent::Initiated {
                payment_hash: self.payment_hash,
                wallet_id: self.wallet_id,
                amount_msat: self.amount_msat,
                max_fee_msat: self.max_fee_msat,
                bolt_invoice: self.bolt_invoice,
                destination: self.destination,
                initiated_at: self.initiated_at,
            }],
        )
    }
}

#[derive(EsEntity)]
pub struct Payment {
    pub id: PaymentId,
    pub payment_hash: PaymentHash,
    pub wallet_id: WalletId,
    pub amount_msat: MilliSatoshi,
    pub max_fee_msat: MilliSatoshi,
    pub state: PaymentState,
    pub fees_paid_msat: Option<MilliSatoshi>,
    pub payment_preimage: Option<Preimage>,
    pub initiated_at: Timestamp,
    pub settled_at: Option<Timestamp>,
    events: EntityEvents<PaymentEvent>,
}

impl Payment {
    pub fn state_str(&self) -> String {
        self.state.as_str().to_owned()
    }

    /// Transition `Initiated → Pending` on LND `IN_FLIGHT`.
    ///
    /// Idempotent on a prior `Pending` event (duplicate IN_FLIGHT
    /// replay). A prior `Completed` / `Failed` / `Reversed` event is a
    /// state-regression claim — LND saying "still in flight" for a
    /// payment we've already determined to be in a terminal state — and
    /// surfaces as `InvalidStateTransition` rather than being silently
    /// ignored.
    pub fn mark_pending(
        &mut self,
        sent_at: Timestamp,
    ) -> Result<Idempotent<()>, PaymentError> {
        idempotency_guard!(self.events.iter_all().rev(), PaymentEvent::Pending { .. });
        if !matches!(self.state, PaymentState::Initiated) {
            return Err(PaymentError::InvalidStateTransition {
                from: self.state,
                attempted: "mark_pending",
            });
        }
        self.events.push(PaymentEvent::Pending { sent_at });
        self.state = PaymentState::Pending;
        Ok(Idempotent::Executed(()))
    }

    /// Transition `(Initiated|Pending) → Completed` on LND `SUCCEEDED`.
    ///
    /// Idempotent on any prior `Completed` event (duplicate SUCCEEDED
    /// replay). A prior `Failed` or `Reversed` event is a genuine
    /// contradiction and surfaces as `InvalidStateTransition` — we do
    /// not silently overwrite a terminal-failure determination.
    pub fn settle(
        &mut self,
        payment_preimage: Preimage,
        fees_paid_msat: MilliSatoshi,
        route_hops: Vec<Hop>,
        settled_at: Timestamp,
    ) -> Result<Idempotent<()>, PaymentError> {
        idempotency_guard!(self.events.iter_all().rev(), PaymentEvent::Completed { .. });
        if !matches!(self.state, PaymentState::Initiated | PaymentState::Pending) {
            return Err(PaymentError::InvalidStateTransition {
                from: self.state,
                attempted: "settle",
            });
        }
        self.events.push(PaymentEvent::Completed {
            settled_at,
            payment_preimage,
            fees_paid_msat,
            route_hops,
        });
        self.state = PaymentState::Completed;
        self.fees_paid_msat = Some(fees_paid_msat);
        self.payment_preimage = Some(payment_preimage);
        self.settled_at = Some(settled_at);
        Ok(Idempotent::Executed(()))
    }

    /// Transition `(Initiated|Pending) → Failed`.
    ///
    /// Idempotent on any prior `Failed` event (duplicate FAILED replay).
    /// A prior `Completed` or `Reversed` event is a genuine contradiction
    /// and surfaces as `InvalidStateTransition`.
    pub fn fail(
        &mut self,
        failure_reason: FailureReason,
        failed_at: Timestamp,
    ) -> Result<Idempotent<()>, PaymentError> {
        idempotency_guard!(self.events.iter_all().rev(), PaymentEvent::Failed { .. });
        if !matches!(self.state, PaymentState::Initiated | PaymentState::Pending) {
            return Err(PaymentError::InvalidStateTransition {
                from: self.state,
                attempted: "fail",
            });
        }
        self.events.push(PaymentEvent::Failed {
            failed_at,
            failure_reason,
        });
        self.state = PaymentState::Failed;
        Ok(Idempotent::Executed(()))
    }

    /// Transition `Completed → Reversed`. Slice-2 does not exercise this;
    /// reserved for the discrepancy path.
    ///
    /// Idempotent on any prior `Reversed` event. Anything other than a
    /// prior `Completed` state is a contradiction.
    pub fn reverse(
        &mut self,
        reason: String,
        reversed_at: Timestamp,
    ) -> Result<Idempotent<()>, PaymentError> {
        idempotency_guard!(self.events.iter_all().rev(), PaymentEvent::Reversed { .. });
        if !matches!(self.state, PaymentState::Completed) {
            return Err(PaymentError::InvalidStateTransition {
                from: self.state,
                attempted: "reverse",
            });
        }
        self.events.push(PaymentEvent::Reversed {
            reversed_at,
            reason,
        });
        self.state = PaymentState::Reversed;
        Ok(Idempotent::Executed(()))
    }
}

// `EntityEvents` does not derive `Debug` in es-entity 0.9.5; same
// workaround as `Invoice`'s `Debug` impl.
impl fmt::Debug for Payment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Payment")
            .field("id", &self.id)
            .field("payment_hash", &self.payment_hash)
            .field("wallet_id", &self.wallet_id)
            .field("amount_msat", &self.amount_msat)
            .field("max_fee_msat", &self.max_fee_msat)
            .field("state", &self.state)
            .field("fees_paid_msat", &self.fees_paid_msat)
            .field("payment_preimage", &self.payment_preimage)
            .field("initiated_at", &self.initiated_at)
            .field("settled_at", &self.settled_at)
            .finish()
    }
}

impl TryFromEvents<PaymentEvent> for Payment {
    fn try_from_events(events: EntityEvents<PaymentEvent>) -> Result<Self, EsEntityError> {
        let id = *events.id();
        let mut iter = events.iter_all();
        let first = iter.next().ok_or(EsEntityError::NotFound)?;
        let PaymentEvent::Initiated {
            payment_hash,
            wallet_id,
            amount_msat,
            max_fee_msat,
            initiated_at,
            ..
        } = first
        else {
            return Err(EsEntityError::NotFound);
        };

        let mut state = PaymentState::Initiated;
        let mut fees_paid_msat: Option<MilliSatoshi> = None;
        let mut payment_preimage: Option<Preimage> = None;
        let mut settled_at: Option<Timestamp> = None;

        for ev in iter {
            match ev {
                PaymentEvent::Pending { .. } => state = PaymentState::Pending,
                PaymentEvent::Completed {
                    settled_at: s,
                    payment_preimage: p,
                    fees_paid_msat: f,
                    ..
                } => {
                    state = PaymentState::Completed;
                    settled_at = Some(*s);
                    payment_preimage = Some(*p);
                    fees_paid_msat = Some(*f);
                }
                PaymentEvent::Failed { .. } => state = PaymentState::Failed,
                PaymentEvent::Reversed { .. } => state = PaymentState::Reversed,
                PaymentEvent::Initiated { .. } => {
                    // Duplicate init — corrupt log.
                    return Err(EsEntityError::NotFound);
                }
            }
        }

        Ok(Payment {
            id,
            payment_hash: *payment_hash,
            wallet_id: *wallet_id,
            amount_msat: *amount_msat,
            max_fee_msat: *max_fee_msat,
            state,
            fees_paid_msat,
            payment_preimage,
            initiated_at: *initiated_at,
            settled_at,
            events,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    fn fixed_now() -> Timestamp {
        Timestamp::from(Utc.with_ymd_and_hms(2026, 5, 14, 10, 0, 0).unwrap())
    }

    /// Amount-carrying invoice — `amount_msat` is `Some`.
    fn ok_decoded() -> DecodedInvoice {
        DecodedInvoice {
            payment_hash: PaymentHash::from([0xcc; 32]),
            destination: "02abc123".to_owned(),
            amount_msat: Some(MilliSatoshi::new(1_000_000)),
            bolt_invoice: BoltInvoice::new("lnbc1u1pj..."),
        }
    }

    /// Amountless invoice — `amount_msat` is `None`.
    fn amountless_decoded() -> DecodedInvoice {
        DecodedInvoice {
            amount_msat: None,
            ..ok_decoded()
        }
    }

    #[test]
    fn try_new_happy_path() {
        let new = NewPayment::try_new(
            ok_decoded(),
            WalletId::from(Uuid::now_v7()),
            None,
            MilliSatoshi::new(5_000),
            fixed_now(),
        )
        .expect("ok");
        assert_eq!(new.amount_msat, MilliSatoshi::new(1_000_000));
        assert_eq!(new.max_fee_msat, MilliSatoshi::new(5_000));
    }

    #[test]
    fn try_new_amountless_uses_requested_amount() {
        let new = NewPayment::try_new(
            amountless_decoded(),
            WalletId::from(Uuid::now_v7()),
            Some(MilliSatoshi::new(777_000)),
            MilliSatoshi::new(5_000),
            fixed_now(),
        )
        .expect("ok");
        assert_eq!(new.amount_msat, MilliSatoshi::new(777_000));
    }

    #[test]
    fn try_new_amountless_without_requested_amount_is_amount_required() {
        let err = NewPayment::try_new(
            amountless_decoded(),
            WalletId::from(Uuid::now_v7()),
            None,
            MilliSatoshi::new(1),
            fixed_now(),
        )
        .unwrap_err();
        assert!(matches!(err, PaymentError::AmountRequired));
    }

    #[test]
    fn try_new_amount_carrying_with_requested_amount_is_overspecified() {
        let err = NewPayment::try_new(
            ok_decoded(),
            WalletId::from(Uuid::now_v7()),
            Some(MilliSatoshi::new(2_000_000)),
            MilliSatoshi::new(1),
            fixed_now(),
        )
        .unwrap_err();
        assert!(matches!(err, PaymentError::AmountOverspecified));
    }

    #[test]
    fn try_new_rejects_zero_amount() {
        let mut decoded = ok_decoded();
        decoded.amount_msat = Some(MilliSatoshi::ZERO);
        let err = NewPayment::try_new(
            decoded,
            WalletId::from(Uuid::now_v7()),
            None,
            MilliSatoshi::new(1),
            fixed_now(),
        )
        .unwrap_err();
        assert!(matches!(err, PaymentError::InvalidAmount));
    }

    #[test]
    fn try_new_rejects_zero_requested_amount() {
        let err = NewPayment::try_new(
            amountless_decoded(),
            WalletId::from(Uuid::now_v7()),
            Some(MilliSatoshi::ZERO),
            MilliSatoshi::new(1),
            fixed_now(),
        )
        .unwrap_err();
        assert!(matches!(err, PaymentError::InvalidAmount));
    }

    #[test]
    fn try_new_rejects_zero_max_fee() {
        let err = NewPayment::try_new(
            ok_decoded(),
            WalletId::from(Uuid::now_v7()),
            None,
            MilliSatoshi::ZERO,
            fixed_now(),
        )
        .unwrap_err();
        assert!(matches!(err, PaymentError::InvalidMaxFee));
    }

    #[test]
    fn try_new_rejects_empty_bolt_invoice() {
        let mut decoded = ok_decoded();
        decoded.bolt_invoice = BoltInvoice::new("");
        let err = NewPayment::try_new(
            decoded,
            WalletId::from(Uuid::now_v7()),
            None,
            MilliSatoshi::new(1),
            fixed_now(),
        )
        .unwrap_err();
        assert!(matches!(err, PaymentError::EmptyBoltInvoice));
    }

    fn fresh_payment() -> Payment {
        let new = NewPayment::try_new(
            ok_decoded(),
            WalletId::from(Uuid::now_v7()),
            None,
            MilliSatoshi::new(5_000),
            fixed_now(),
        )
        .unwrap();
        Payment::try_from_events(new.into_events()).unwrap()
    }

    /// Fast-forward to an arbitrary state without going through the
    /// command-method flow — lets a single test start from any state.
    fn push_event(p: &mut Payment, event: PaymentEvent, new_state: PaymentState) {
        p.events_mut().extend(std::iter::once(event));
        p.state = new_state;
    }

    fn sample_completed_event() -> PaymentEvent {
        PaymentEvent::Completed {
            settled_at: fixed_now(),
            payment_preimage: Preimage::from([0xdd; 32]),
            fees_paid_msat: MilliSatoshi::new(50),
            route_hops: Vec::new(),
        }
    }

    fn sample_failed_event() -> PaymentEvent {
        PaymentEvent::Failed {
            failed_at: fixed_now(),
            failure_reason: FailureReason::Timeout,
        }
    }

    fn sample_reversed_event() -> PaymentEvent {
        PaymentEvent::Reversed {
            reversed_at: fixed_now(),
            reason: "dispute".to_owned(),
        }
    }

    // ---- happy-path executions -------------------------------------------

    #[test]
    fn mark_pending_from_initiated_executes() {
        let mut p = fresh_payment();
        let outcome = p.mark_pending(fixed_now()).unwrap();
        assert!(matches!(outcome, Idempotent::Executed(())));
        assert_eq!(p.state, PaymentState::Pending);
    }

    #[test]
    fn settle_from_pending_executes() {
        let mut p = fresh_payment();
        push_event(
            &mut p,
            PaymentEvent::Pending {
                sent_at: fixed_now(),
            },
            PaymentState::Pending,
        );
        let outcome = p
            .settle(
                Preimage::from([0xdd; 32]),
                MilliSatoshi::new(50),
                Vec::new(),
                fixed_now(),
            )
            .unwrap();
        assert!(matches!(outcome, Idempotent::Executed(())));
        assert_eq!(p.state, PaymentState::Completed);
        assert_eq!(p.fees_paid_msat, Some(MilliSatoshi::new(50)));
        assert_eq!(p.payment_preimage, Some(Preimage::from([0xdd; 32])));
        assert!(p.settled_at.is_some());
    }

    #[test]
    fn fail_from_initiated_executes() {
        let mut p = fresh_payment();
        let outcome = p.fail(FailureReason::Timeout, fixed_now()).unwrap();
        assert!(matches!(outcome, Idempotent::Executed(())));
        assert_eq!(p.state, PaymentState::Failed);
    }

    // ---- idempotent replays ---

    #[test]
    fn mark_pending_from_pending_is_idempotent() {
        let mut p = fresh_payment();
        push_event(
            &mut p,
            PaymentEvent::Pending {
                sent_at: fixed_now(),
            },
            PaymentState::Pending,
        );
        let outcome = p.mark_pending(fixed_now()).unwrap();
        assert!(matches!(outcome, Idempotent::Ignored));
    }

    #[test]
    fn mark_pending_from_completed_is_invalid_state_transition() {
        // LND telling us "still in flight" for an already-Completed
        // payment is a state-regression claim, not a duplicate — we
        // surface it instead of silently ignoring.
        let mut p = fresh_payment();
        push_event(&mut p, sample_completed_event(), PaymentState::Completed);
        match p.mark_pending(fixed_now()) {
            Err(PaymentError::InvalidStateTransition {
                attempted: "mark_pending",
                ..
            }) => {}
            Err(other) => panic!("expected InvalidStateTransition(mark_pending), got {other:?}"),
            Ok(_) => panic!("expected InvalidStateTransition(mark_pending), got Ok"),
        }
    }

    #[test]
    fn settle_from_completed_is_idempotent() {
        // Duplicate SUCCEEDED replay → no-op rather than error. (Was
        // `InvalidStateTransition` before the idempotency-guard refactor.)
        let mut p = fresh_payment();
        push_event(&mut p, sample_completed_event(), PaymentState::Completed);
        let outcome = p
            .settle(
                Preimage::from([0xdd; 32]),
                MilliSatoshi::new(50),
                Vec::new(),
                fixed_now(),
            )
            .unwrap();
        assert!(matches!(outcome, Idempotent::Ignored));
    }

    #[test]
    fn fail_from_failed_is_idempotent() {
        let mut p = fresh_payment();
        push_event(&mut p, sample_failed_event(), PaymentState::Failed);
        let outcome = p.fail(FailureReason::Timeout, fixed_now()).unwrap();
        assert!(matches!(outcome, Idempotent::Ignored));
    }

    #[test]
    fn reverse_from_reversed_is_idempotent() {
        let mut p = fresh_payment();
        // Reversed only follows Completed in the state machine — push
        // both events so the log reflects a legitimate sequence.
        push_event(&mut p, sample_completed_event(), PaymentState::Completed);
        push_event(&mut p, sample_reversed_event(), PaymentState::Reversed);
        let outcome = p.reverse("dispute".to_owned(), fixed_now()).unwrap();
        assert!(matches!(outcome, Idempotent::Ignored));
    }

    // ---- genuine contradictions still surface as errors ------------------

    #[test]
    fn settle_from_failed_is_invalid_state_transition() {
        // `Failed` is a different terminal outcome than `Completed`, so
        // a SUCCEEDED arriving after FAILED is a real contradiction —
        // we surface it instead of silently overwriting.
        let mut p = fresh_payment();
        push_event(&mut p, sample_failed_event(), PaymentState::Failed);
        // `Idempotent<T>` doesn't impl `Debug` in es-entity 0.9.5, so
        // `unwrap_err()` (which formats the Ok value on unexpected Ok)
        // won't compile. Manual match instead.
        match p.settle(
            Preimage::from([0xdd; 32]),
            MilliSatoshi::new(50),
            Vec::new(),
            fixed_now(),
        ) {
            Err(PaymentError::InvalidStateTransition {
                attempted: "settle",
                ..
            }) => {}
            Err(other) => panic!("expected InvalidStateTransition(settle), got {other:?}"),
            Ok(_) => panic!("expected InvalidStateTransition(settle), got Ok"),
        }
    }

    #[test]
    fn fail_from_completed_is_invalid_state_transition() {
        let mut p = fresh_payment();
        push_event(&mut p, sample_completed_event(), PaymentState::Completed);
        match p.fail(FailureReason::Timeout, fixed_now()) {
            Err(PaymentError::InvalidStateTransition {
                attempted: "fail", ..
            }) => {}
            Err(other) => panic!("expected InvalidStateTransition(fail), got {other:?}"),
            Ok(_) => panic!("expected InvalidStateTransition(fail), got Ok"),
        }
    }

    #[test]
    fn reverse_from_pending_is_invalid_state_transition() {
        let mut p = fresh_payment();
        push_event(
            &mut p,
            PaymentEvent::Pending {
                sent_at: fixed_now(),
            },
            PaymentState::Pending,
        );
        match p.reverse("dispute".to_owned(), fixed_now()) {
            Err(PaymentError::InvalidStateTransition {
                attempted: "reverse",
                ..
            }) => {}
            Err(other) => panic!("expected InvalidStateTransition(reverse), got {other:?}"),
            Ok(_) => panic!("expected InvalidStateTransition(reverse), got Ok"),
        }
    }

    #[test]
    fn try_from_events_reconstructs_completed_payment() {
        // Round-trip from a hand-built event log: Initiated → Pending → Completed.
        let id = PaymentId::new();
        let payment_hash = PaymentHash::from([0xcc; 32]);
        let wallet_id = WalletId::from(Uuid::now_v7());
        let amount = MilliSatoshi::new(1_000_000);
        let max_fee = MilliSatoshi::new(5_000);
        let preimage = Preimage::from([0xdd; 32]);
        let fees_paid = MilliSatoshi::new(200);

        let events = EntityEvents::init(
            id,
            [
                PaymentEvent::Initiated {
                    payment_hash,
                    wallet_id,
                    amount_msat: amount,
                    max_fee_msat: max_fee,
                    bolt_invoice: BoltInvoice::new("lnbc1u1pj..."),
                    destination: "02abc".to_owned(),
                    initiated_at: fixed_now(),
                },
                PaymentEvent::Pending {
                    sent_at: fixed_now(),
                },
                PaymentEvent::Completed {
                    settled_at: fixed_now(),
                    payment_preimage: preimage,
                    fees_paid_msat: fees_paid,
                    route_hops: Vec::new(),
                },
            ],
        );

        let payment = Payment::try_from_events(events).expect("hydrate");
        assert_eq!(payment.id, id);
        assert_eq!(payment.state, PaymentState::Completed);
        assert_eq!(payment.fees_paid_msat, Some(fees_paid));
        assert_eq!(payment.payment_preimage, Some(preimage));
        assert!(payment.settled_at.is_some());
    }
}
