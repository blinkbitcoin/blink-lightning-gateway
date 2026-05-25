//! `Invoice` aggregate — event-sourced via `es-entity` derive macros.
//!
//! `NewInvoice::try_new` is the validating constructor — all validation
//! happens here because `IntoEvents::into_events` (called by the repo's
//! `create`) cannot fail. The `Open → Held / Settled / Canceled` state
//! machine uses the same `idempotency_guard!` shape as `Payment`.

use derive_builder::Builder;
use es_entity::{
    idempotency_guard, EntityEvents, EntityHydrationError, EsEntity, Idempotent, IntoEvents,
    TryFromEvents,
};
use serde::{Deserialize, Serialize};
use std::fmt;

use super::{
    error::InvoiceError,
    event::{CancelReason, InvoiceEvent},
};
use crate::primitives::{
    BoltInvoice, InvoiceId, MilliSatoshi, PaymentHash, Preimage, Timestamp, WalletId,
};

const SECS_PER_MIN: u32 = 60;
const SECS_PER_HOUR: u32 = 60 * 60;
const SECS_PER_4_HOURS: u32 = SECS_PER_HOUR * 4;
const SECS_PER_DAY: u32 = SECS_PER_HOUR * 24;

// BTC invoice expiration policy
//
// Out-of-range expiry values are silently coerced to
// `BTC_INVOICE_DEFAULT_SECONDS`, matching blink-core's behavior
const BTC_INVOICE_MIN_SECONDS: u32 = SECS_PER_MIN;
const BTC_INVOICE_MAX_SECONDS: u32 = SECS_PER_DAY;
const BTC_INVOICE_DEFAULT_SECONDS: u32 = SECS_PER_4_HOURS;

/// Lifecycle state. A new invoice is `Open` (created, awaiting
/// payment — LND's own term for the same state). `Held` is the blink
/// domain term for LND's `Accepted` (an HTLC is parked on a HOLD
/// invoice). `Settled` and `Canceled` are terminal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "TEXT", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum InvoiceState {
    Open,
    Held,
    Settled,
    Canceled,
}

impl InvoiceState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Held => "held",
            Self::Settled => "settled",
            Self::Canceled => "canceled",
        }
    }
}

impl fmt::Display for InvoiceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Validated input for creating an invoice — built via
/// `NewInvoice::try_new`. The `id` is minted in `try_new` so the repo
/// can use it for both the projection-row insert and the event-log
/// grouping.
#[derive(Clone, Debug)]
pub struct NewInvoice {
    pub id: InvoiceId,
    pub payment_hash: PaymentHash,
    pub wallet_id: WalletId,
    pub amount_msat: MilliSatoshi,
    pub expiry_at: Timestamp,
    pub bolt_invoice: BoltInvoice,
    pub created_at: Timestamp,
}

impl NewInvoice {
    /// Validating constructor. Rejects zero amount; coerces
    /// out-of-range `expiry_seconds` (outside 60s..=24h) to the 4-hour
    /// default. `memo` isn't stored — it's already encoded in
    /// `bolt_invoice` (BOLT11's `d` field).
    pub fn try_new(
        payment_hash: PaymentHash,
        wallet_id: WalletId,
        amount_msat: MilliSatoshi,
        expiry_seconds: u32,
        bolt_invoice: BoltInvoice,
        now: Timestamp,
    ) -> Result<Self, InvoiceError> {
        if amount_msat.as_u64() == 0 {
            return Err(InvoiceError::InvalidAmount);
        }
        let effective_expiry =
            if (BTC_INVOICE_MIN_SECONDS..=BTC_INVOICE_MAX_SECONDS).contains(&expiry_seconds) {
                expiry_seconds
            } else {
                BTC_INVOICE_DEFAULT_SECONDS
            };
        let expiry_at = Timestamp::from(
            now.into_inner() + chrono::Duration::seconds(i64::from(effective_expiry)),
        );
        Ok(Self {
            id: InvoiceId::new(),
            payment_hash,
            wallet_id,
            amount_msat,
            expiry_at,
            bolt_invoice,
            created_at: now,
        })
    }

    /// Accessor read by `EsRepo`'s `create(accessor = "state_str()")`.
    pub fn state_str(&self) -> String {
        InvoiceState::Open.as_str().to_owned()
    }
}

impl IntoEvents<InvoiceEvent> for NewInvoice {
    fn into_events(self) -> EntityEvents<InvoiceEvent> {
        EntityEvents::init(
            self.id,
            [InvoiceEvent::Created {
                payment_hash: self.payment_hash,
                wallet_id: self.wallet_id,
                amount_msat: self.amount_msat,
                expiry_at: self.expiry_at,
                bolt_invoice: self.bolt_invoice,
                created_at: self.created_at,
            }],
        )
    }
}

/// Hydrated invoice aggregate. Constructed from the event log via
/// `TryFromEvents`; never built directly outside this module.
#[derive(EsEntity, Builder)]
#[builder(pattern = "owned", build_fn(error = "EntityHydrationError"))]
pub struct Invoice {
    pub id: InvoiceId,
    pub payment_hash: PaymentHash,
    pub wallet_id: WalletId,
    pub amount_msat: MilliSatoshi,
    pub expiry_at: Timestamp,
    pub bolt_invoice: BoltInvoice,
    #[builder(default = "InvoiceState::Open")]
    pub state: InvoiceState,
    pub created_at: Timestamp,
    #[builder(default)]
    pub payment_preimage: Option<Preimage>,
    #[builder(default)]
    pub canceled_reason: Option<CancelReason>,
    events: EntityEvents<InvoiceEvent>,
}

impl Invoice {
    /// Accessor read by `EsRepo`'s `update(accessor = "state_str()")`.
    pub fn state_str(&self) -> String {
        self.state.as_str().to_owned()
    }

    /// `Open → Held` on LND `Accepted`. Idempotent on a duplicate
    /// `HtlcHeld` event.
    pub fn mark_held(
        &mut self,
        htlc_amount_msat: MilliSatoshi,
        held_at: Timestamp,
    ) -> Result<Idempotent<()>, InvoiceError> {
        idempotency_guard!(self.events.iter_all().rev(), already_applied: InvoiceEvent::HtlcHeld { .. });
        if !matches!(self.state, InvoiceState::Open) {
            return Err(InvoiceError::InvalidStateTransition {
                from: self.state,
                attempted: "mark_held",
            });
        }
        self.events.push(InvoiceEvent::HtlcHeld {
            held_at,
            htlc_amount_msat,
        });
        self.state = InvoiceState::Held;
        Ok(Idempotent::Executed(()))
    }

    /// `(Open|Held) → Settled` on LND `is_confirmed`. Idempotent on a
    /// duplicate `Settled` event; a prior `Canceled` surfaces as
    /// `InvalidStateTransition`. `Held` is accepted as a source state
    /// for the future HODL settle-command path.
    pub fn settle(
        &mut self,
        payment_preimage: Preimage,
        settled_at: Timestamp,
    ) -> Result<Idempotent<()>, InvoiceError> {
        idempotency_guard!(self.events.iter_all().rev(), already_applied: InvoiceEvent::Settled { .. });
        if !matches!(self.state, InvoiceState::Open | InvoiceState::Held) {
            return Err(InvoiceError::InvalidStateTransition {
                from: self.state,
                attempted: "settle",
            });
        }
        self.events.push(InvoiceEvent::Settled {
            settled_at,
            payment_preimage,
        });
        self.state = InvoiceState::Settled;
        self.payment_preimage = Some(payment_preimage);
        Ok(Idempotent::Executed(()))
    }

    /// `(Open|Held) → Canceled`. Idempotent on a duplicate `Canceled`
    /// event; a prior `Settled` surfaces as `InvalidStateTransition`.
    pub fn cancel(
        &mut self,
        reason: CancelReason,
        canceled_at: Timestamp,
    ) -> Result<Idempotent<()>, InvoiceError> {
        idempotency_guard!(self.events.iter_all().rev(), already_applied: InvoiceEvent::Canceled { .. });
        if !matches!(self.state, InvoiceState::Open | InvoiceState::Held) {
            return Err(InvoiceError::InvalidStateTransition {
                from: self.state,
                attempted: "cancel",
            });
        }
        self.events.push(InvoiceEvent::Canceled {
            canceled_at,
            reason: reason.clone(),
        });
        self.state = InvoiceState::Canceled;
        self.canceled_reason = Some(reason);
        Ok(Idempotent::Executed(()))
    }
}

// `EntityEvents` does not derive `Debug` in es-entity 0.9.5, so we cannot
// auto-derive `Debug` on `Invoice`. Hand-impl excludes the events field;
// callers that need event-level inspection use `Invoice::events()` /
// `Invoice::events_mut()` from the `EsEntity` impl.
impl fmt::Debug for Invoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Invoice")
            .field("id", &self.id)
            .field("payment_hash", &self.payment_hash)
            .field("wallet_id", &self.wallet_id)
            .field("amount_msat", &self.amount_msat)
            .field("expiry_at", &self.expiry_at)
            .field("bolt_invoice", &self.bolt_invoice)
            .field("state", &self.state)
            .field("created_at", &self.created_at)
            .field("payment_preimage", &self.payment_preimage)
            .field("canceled_reason", &self.canceled_reason)
            .finish()
    }
}

impl TryFromEvents<InvoiceEvent> for Invoice {
    fn try_from_events(events: EntityEvents<InvoiceEvent>) -> Result<Self, EntityHydrationError> {
        let mut builder = InvoiceBuilder::default().id(*events.id());

        for ev in events.iter_all() {
            match ev {
                InvoiceEvent::Created {
                    payment_hash,
                    wallet_id,
                    amount_msat,
                    expiry_at,
                    bolt_invoice,
                    created_at,
                } => {
                    builder = builder
                        .payment_hash(*payment_hash)
                        .wallet_id(*wallet_id)
                        .amount_msat(*amount_msat)
                        .expiry_at(*expiry_at)
                        .bolt_invoice(bolt_invoice.clone())
                        .created_at(*created_at);
                }
                InvoiceEvent::HtlcHeld { .. } => {
                    builder = builder.state(InvoiceState::Held);
                }
                InvoiceEvent::Settled {
                    payment_preimage, ..
                } => {
                    builder = builder
                        .state(InvoiceState::Settled)
                        .payment_preimage(Some(*payment_preimage));
                }
                InvoiceEvent::Canceled { reason, .. } => {
                    builder = builder
                        .state(InvoiceState::Canceled)
                        .canceled_reason(Some(reason.clone()));
                }
            }
        }

        // `build()` returns `Err(EntityHydrationError::UninitializedFieldError(...))`
        // if any non-`#[builder(default)]` field wasn't populated by the event
        // stream — that covers both the empty-events case AND the
        // first-event-isn't-Created corrupt-log case (no `payment_hash` etc.
        // ever set). No panics needed.
        builder.events(events).build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    fn fixed_now() -> Timestamp {
        Timestamp::from(Utc.with_ymd_and_hms(2026, 5, 7, 10, 0, 0).unwrap())
    }

    fn ok_args() -> (PaymentHash, WalletId, MilliSatoshi, u32, BoltInvoice) {
        (
            PaymentHash::from([0xaa; 32]),
            WalletId::from(Uuid::now_v7()),
            MilliSatoshi::new(1_000_000),
            3600,
            BoltInvoice::new("lnbc1u1pj..."),
        )
    }

    #[test]
    fn try_new_happy_path_constructs_new_invoice() {
        let (h, w, a, e, b) = ok_args();
        let now = fixed_now();
        let new = NewInvoice::try_new(h, w, a, e, b, now).expect("happy path");
        assert_eq!(new.amount_msat, MilliSatoshi::new(1_000_000));
        assert_eq!(new.created_at, now);
        let expected_expiry = Timestamp::from(now.into_inner() + chrono::Duration::seconds(3600));
        assert_eq!(new.expiry_at, expected_expiry);
    }

    #[test]
    fn try_new_coerces_expiry_below_min_to_default() {
        let (h, w, a, _, b) = ok_args();
        let now = fixed_now();
        let low_expiry = BTC_INVOICE_MIN_SECONDS - 1;
        let new = NewInvoice::try_new(h, w, a, low_expiry, b, now).expect("coerced, not rejected");
        let expected_expiry = Timestamp::from(
            now.into_inner() + chrono::Duration::seconds(i64::from(BTC_INVOICE_DEFAULT_SECONDS)),
        );
        assert_eq!(new.expiry_at, expected_expiry);
    }

    #[test]
    fn try_new_coerces_expiry_above_max_to_default() {
        let (h, w, a, _, b) = ok_args();
        let now = fixed_now();
        let high_expiry = BTC_INVOICE_MAX_SECONDS + 1;
        let new = NewInvoice::try_new(h, w, a, high_expiry, b, now).expect("coerced, not rejected");
        let expected_expiry = Timestamp::from(
            now.into_inner() + chrono::Duration::seconds(i64::from(BTC_INVOICE_DEFAULT_SECONDS)),
        );
        assert_eq!(new.expiry_at, expected_expiry);
    }

    #[test]
    fn try_new_rejects_zero_amount() {
        let (h, w, _, e, b) = ok_args();
        let err = NewInvoice::try_new(h, w, MilliSatoshi::ZERO, e, b, fixed_now()).unwrap_err();
        assert!(matches!(err, InvoiceError::InvalidAmount));
    }

    #[test]
    fn try_new_accepts_minimum_expiry() {
        let (h, w, a, _, b) = ok_args();
        assert!(NewInvoice::try_new(h, w, a, BTC_INVOICE_MIN_SECONDS, b, fixed_now()).is_ok());
    }

    #[test]
    fn try_new_accepts_maximum_expiry() {
        let (h, w, a, _, b) = ok_args();
        assert!(NewInvoice::try_new(h, w, a, BTC_INVOICE_MAX_SECONDS, b, fixed_now()).is_ok());
    }

    #[test]
    fn try_from_events_reconstructs_open_invoice() {
        let (h, w, a, e, b) = ok_args();
        let now = fixed_now();
        let new = NewInvoice::try_new(h, w, a, e, b, now).unwrap();
        let id = new.id;
        let entity_events = new.into_events();
        let invoice = Invoice::try_from_events(entity_events).expect("hydrate");
        assert_eq!(invoice.id, id);
        assert_eq!(invoice.state, InvoiceState::Open);
        assert_eq!(invoice.amount_msat, MilliSatoshi::new(1_000_000));
        assert_eq!(invoice.created_at, now);
        assert!(invoice.payment_preimage.is_none());
        assert!(invoice.canceled_reason.is_none());
    }

    #[test]
    fn try_from_events_with_no_events_returns_uninitialized_field_error() {
        // With the builder-pattern try_from_events, an empty event log surfaces as
        // `EntityHydrationError::UninitializedFieldError` because no event
        // ever populated the required `payment_hash` / `wallet_id` / etc.
        // fields. Same graceful error path covers the "first event is not
        // Created" corrupt-log case.
        let id = InvoiceId::new();
        let empty: EntityEvents<InvoiceEvent> = EntityEvents::init(id, std::iter::empty());
        let err = Invoice::try_from_events(empty).unwrap_err();
        assert!(matches!(
            err,
            EntityHydrationError::UninitializedFieldError(_)
        ));
    }

    // ---- Story 2.3: command-method state machine ----------------------

    fn fresh_invoice() -> Invoice {
        let (h, w, a, e, b) = ok_args();
        let new = NewInvoice::try_new(h, w, a, e, b, fixed_now()).unwrap();
        Invoice::try_from_events(new.into_events()).unwrap()
    }

    /// Fast-forward to an arbitrary state without going through command
    /// methods — lets a single test start from any state.
    fn push_event(inv: &mut Invoice, event: InvoiceEvent, new_state: InvoiceState) {
        inv.events_mut().extend(std::iter::once(event));
        inv.state = new_state;
    }

    fn sample_held_event() -> InvoiceEvent {
        InvoiceEvent::HtlcHeld {
            held_at: fixed_now(),
            htlc_amount_msat: MilliSatoshi::new(1_000_000),
        }
    }

    fn sample_settled_event() -> InvoiceEvent {
        InvoiceEvent::Settled {
            settled_at: fixed_now(),
            payment_preimage: Preimage::from([0xee; 32]),
        }
    }

    fn sample_canceled_event() -> InvoiceEvent {
        InvoiceEvent::Canceled {
            canceled_at: fixed_now(),
            reason: CancelReason::Expired,
        }
    }

    #[test]
    fn mark_held_from_open_executes() {
        let mut inv = fresh_invoice();
        let outcome = inv
            .mark_held(MilliSatoshi::new(1_000_000), fixed_now())
            .unwrap();
        assert!(matches!(outcome, Idempotent::Executed(())));
        assert_eq!(inv.state, InvoiceState::Held);
    }

    #[test]
    fn settle_from_open_executes() {
        let mut inv = fresh_invoice();
        let preimage = Preimage::from([0xee; 32]);
        let outcome = inv.settle(preimage, fixed_now()).unwrap();
        assert!(matches!(outcome, Idempotent::Executed(())));
        assert_eq!(inv.state, InvoiceState::Settled);
        assert_eq!(inv.payment_preimage, Some(preimage));
    }

    #[test]
    fn settle_from_held_executes() {
        let mut inv = fresh_invoice();
        push_event(&mut inv, sample_held_event(), InvoiceState::Held);
        let preimage = Preimage::from([0xee; 32]);
        let outcome = inv.settle(preimage, fixed_now()).unwrap();
        assert!(matches!(outcome, Idempotent::Executed(())));
        assert_eq!(inv.state, InvoiceState::Settled);
    }

    #[test]
    fn cancel_from_open_executes() {
        let mut inv = fresh_invoice();
        let outcome = inv.cancel(CancelReason::Expired, fixed_now()).unwrap();
        assert!(matches!(outcome, Idempotent::Executed(())));
        assert_eq!(inv.state, InvoiceState::Canceled);
        assert_eq!(inv.canceled_reason, Some(CancelReason::Expired));
    }

    #[test]
    fn cancel_from_held_executes() {
        let mut inv = fresh_invoice();
        push_event(&mut inv, sample_held_event(), InvoiceState::Held);
        let outcome = inv.cancel(CancelReason::Manual, fixed_now()).unwrap();
        assert!(matches!(outcome, Idempotent::Executed(())));
        assert_eq!(inv.state, InvoiceState::Canceled);
    }

    // ---- idempotent replays --------------------------------------------

    #[test]
    fn mark_held_from_held_is_idempotent() {
        let mut inv = fresh_invoice();
        push_event(&mut inv, sample_held_event(), InvoiceState::Held);
        let outcome = inv
            .mark_held(MilliSatoshi::new(1_000_000), fixed_now())
            .unwrap();
        assert!(matches!(outcome, Idempotent::AlreadyApplied));
    }

    #[test]
    fn settle_from_settled_is_idempotent() {
        let mut inv = fresh_invoice();
        push_event(&mut inv, sample_settled_event(), InvoiceState::Settled);
        let outcome = inv.settle(Preimage::from([0xee; 32]), fixed_now()).unwrap();
        assert!(matches!(outcome, Idempotent::AlreadyApplied));
    }

    #[test]
    fn cancel_from_canceled_is_idempotent() {
        let mut inv = fresh_invoice();
        push_event(&mut inv, sample_canceled_event(), InvoiceState::Canceled);
        let outcome = inv.cancel(CancelReason::Expired, fixed_now()).unwrap();
        assert!(matches!(outcome, Idempotent::AlreadyApplied));
    }

    // ---- genuine contradictions -----------------------------------------

    #[test]
    fn mark_held_from_settled_is_invalid_state_transition() {
        let mut inv = fresh_invoice();
        push_event(&mut inv, sample_settled_event(), InvoiceState::Settled);
        match inv.mark_held(MilliSatoshi::new(1), fixed_now()) {
            Err(InvoiceError::InvalidStateTransition {
                attempted: "mark_held",
                ..
            }) => {}
            Err(other) => panic!("expected InvalidStateTransition(mark_held), got {other:?}"),
            Ok(_) => panic!("expected InvalidStateTransition(mark_held), got Ok"),
        }
    }

    #[test]
    fn settle_from_canceled_is_invalid_state_transition() {
        let mut inv = fresh_invoice();
        push_event(&mut inv, sample_canceled_event(), InvoiceState::Canceled);
        match inv.settle(Preimage::from([0xee; 32]), fixed_now()) {
            Err(InvoiceError::InvalidStateTransition {
                attempted: "settle",
                ..
            }) => {}
            Err(other) => panic!("expected InvalidStateTransition(settle), got {other:?}"),
            Ok(_) => panic!("expected InvalidStateTransition(settle), got Ok"),
        }
    }

    #[test]
    fn cancel_from_settled_is_invalid_state_transition() {
        let mut inv = fresh_invoice();
        push_event(&mut inv, sample_settled_event(), InvoiceState::Settled);
        match inv.cancel(CancelReason::Expired, fixed_now()) {
            Err(InvoiceError::InvalidStateTransition {
                attempted: "cancel",
                ..
            }) => {}
            Err(other) => panic!("expected InvalidStateTransition(cancel), got {other:?}"),
            Ok(_) => panic!("expected InvalidStateTransition(cancel), got Ok"),
        }
    }

    // ---- TryFromEvents fold --------------------------------------------

    #[test]
    fn try_from_events_reconstructs_settled_after_held_invoice() {
        // Fold over Created → HtlcHeld → Settled.
        let (h, w, a, e, b) = ok_args();
        let new = NewInvoice::try_new(h, w, a, e, b, fixed_now()).unwrap();
        let id = new.id;
        let preimage = Preimage::from([0xee; 32]);
        let events = EntityEvents::init(
            id,
            [
                InvoiceEvent::Created {
                    payment_hash: h,
                    wallet_id: w,
                    amount_msat: a,
                    expiry_at: Timestamp::from(
                        fixed_now().into_inner() + chrono::Duration::seconds(i64::from(e)),
                    ),
                    bolt_invoice: BoltInvoice::new("lnbc1u1pj..."),
                    created_at: fixed_now(),
                },
                InvoiceEvent::HtlcHeld {
                    held_at: fixed_now(),
                    htlc_amount_msat: a,
                },
                InvoiceEvent::Settled {
                    settled_at: fixed_now(),
                    payment_preimage: preimage,
                },
            ],
        );
        let invoice = Invoice::try_from_events(events).unwrap();
        assert_eq!(invoice.state, InvoiceState::Settled);
        assert_eq!(invoice.payment_preimage, Some(preimage));
    }

    #[test]
    fn try_from_events_reconstructs_canceled_invoice() {
        let (h, w, a, e, b) = ok_args();
        let new = NewInvoice::try_new(h, w, a, e, b, fixed_now()).unwrap();
        let id = new.id;
        let events = EntityEvents::init(
            id,
            [
                InvoiceEvent::Created {
                    payment_hash: h,
                    wallet_id: w,
                    amount_msat: a,
                    expiry_at: Timestamp::from(
                        fixed_now().into_inner() + chrono::Duration::seconds(i64::from(e)),
                    ),
                    bolt_invoice: BoltInvoice::new("lnbc1u1pj..."),
                    created_at: fixed_now(),
                },
                InvoiceEvent::Canceled {
                    canceled_at: fixed_now(),
                    reason: CancelReason::Expired,
                },
            ],
        );
        let invoice = Invoice::try_from_events(events).unwrap();
        assert_eq!(invoice.state, InvoiceState::Canceled);
        assert_eq!(invoice.canceled_reason, Some(CancelReason::Expired));
    }
}
