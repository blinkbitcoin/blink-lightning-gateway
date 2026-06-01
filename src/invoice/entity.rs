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
/// grouping. `payment_preimage` is gateway-owned (HODL): generated
/// before calling LND's `AddHoldInvoice`, threaded into `Created`,
/// retained on the hydrated `Invoice` so `settle_hold_invoice` can
/// release it.
#[derive(Clone, Debug)]
pub struct NewInvoice {
    pub id: InvoiceId,
    pub payment_hash: PaymentHash,
    pub payment_preimage: Preimage,
    pub wallet_id: WalletId,
    pub amount_msat: Option<MilliSatoshi>,
    pub expiry_at: Timestamp,
    pub bolt_invoice: BoltInvoice,
    pub external_id: String,
    pub created_at: Timestamp,
}

impl NewInvoice {
    /// Validating constructor. Accepts `None` for amountless invoices
    /// (settled amount sourced from received HTLCs), `Some(positive)`
    /// for fixed-amount; rejects `Some(MilliSatoshi::ZERO)`. Coerces
    /// out-of-range `expiry_seconds` (outside 60s..=24h) to the 4-hour
    /// default. `memo` isn't stored — it's already encoded in
    /// `bolt_invoice` (BOLT11's `d` field).
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        payment_hash: PaymentHash,
        payment_preimage: Preimage,
        wallet_id: WalletId,
        amount_msat: Option<MilliSatoshi>,
        expiry_seconds: u32,
        bolt_invoice: BoltInvoice,
        external_id: String,
        now: Timestamp,
    ) -> Result<Self, InvoiceError> {
        if matches!(amount_msat, Some(a) if a.as_u64() == 0) {
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
            payment_preimage,
            wallet_id,
            amount_msat,
            expiry_at,
            bolt_invoice,
            external_id,
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
                payment_preimage: self.payment_preimage,
                wallet_id: self.wallet_id,
                amount_msat: self.amount_msat,
                expiry_at: self.expiry_at,
                bolt_invoice: self.bolt_invoice,
                external_id: self.external_id,
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
    pub payment_preimage: Preimage,
    pub wallet_id: WalletId,
    pub amount_msat: Option<MilliSatoshi>,
    pub expiry_at: Timestamp,
    pub bolt_invoice: BoltInvoice,
    pub external_id: String,
    #[builder(default = "InvoiceState::Open")]
    pub state: InvoiceState,
    pub created_at: Timestamp,
    /// Parked HTLC sum (from `HtlcHeld`)
    #[builder(default)]
    pub held_amount_msat: Option<MilliSatoshi>,
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
        self.held_amount_msat = Some(htlc_amount_msat);
        Ok(Idempotent::Executed(()))
    }

    /// `Held → Settled`. Blink only issues HODL invoices, so settle is
    /// only valid from Held. A duplicate Settled event short-circuits as
    /// `Idempotent::AlreadyApplied`, any other source state is
    /// `InvalidStateTransition`.
    pub fn settle(&mut self, settled_at: Timestamp) -> Result<Idempotent<()>, InvoiceError> {
        idempotency_guard!(self.events.iter_all().rev(), already_applied: InvoiceEvent::Settled { .. });
        if !matches!(self.state, InvoiceState::Held) {
            return Err(InvoiceError::InvalidStateTransition {
                from: self.state,
                attempted: "settle",
            });
        }
        self.events.push(InvoiceEvent::Settled { settled_at });
        self.state = InvoiceState::Settled;
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

// `EntityEvents` does not derive `Debug` in es-entity 0.10.36, so we cannot
// auto-derive `Debug` on `Invoice`. Hand-impl excludes the events field;
// callers that need event-level inspection use `Invoice::events()` /
// `Invoice::events_mut()` from the `EsEntity` impl.
impl fmt::Debug for Invoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Invoice")
            .field("id", &self.id)
            .field("payment_hash", &self.payment_hash)
            .field("payment_preimage", &self.payment_preimage)
            .field("wallet_id", &self.wallet_id)
            .field("amount_msat", &self.amount_msat)
            .field("expiry_at", &self.expiry_at)
            .field("bolt_invoice", &self.bolt_invoice)
            .field("external_id", &self.external_id)
            .field("state", &self.state)
            .field("created_at", &self.created_at)
            .field("held_amount_msat", &self.held_amount_msat)
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
                    payment_preimage,
                    wallet_id,
                    amount_msat,
                    expiry_at,
                    bolt_invoice,
                    external_id,
                    created_at,
                } => {
                    builder = builder
                        .payment_hash(*payment_hash)
                        .payment_preimage(*payment_preimage)
                        .wallet_id(*wallet_id)
                        .amount_msat(*amount_msat)
                        .expiry_at(*expiry_at)
                        .bolt_invoice(bolt_invoice.clone())
                        .external_id(external_id.clone())
                        .created_at(*created_at);
                }
                InvoiceEvent::HtlcHeld {
                    htlc_amount_msat, ..
                } => {
                    builder = builder
                        .state(InvoiceState::Held)
                        .held_amount_msat(Some(*htlc_amount_msat));
                }
                InvoiceEvent::Settled { .. } => {
                    builder = builder.state(InvoiceState::Settled);
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

    fn ok_args() -> (
        PaymentHash,
        Preimage,
        WalletId,
        Option<MilliSatoshi>,
        u32,
        BoltInvoice,
    ) {
        (
            PaymentHash::from([0xaa; 32]),
            Preimage::from([0xee; 32]),
            WalletId::from(Uuid::now_v7()),
            Some(MilliSatoshi::new(1_000_000)),
            3600,
            BoltInvoice::new("lnbc1u1pj..."),
        )
    }

    #[test]
    fn try_new_happy_path_constructs_new_invoice() {
        let (h, pre, w, a, e, b) = ok_args();
        let now = fixed_now();
        let new =
            NewInvoice::try_new(h, pre, w, a, e, b, "ext-id".to_owned(), now).expect("happy path");
        assert_eq!(new.amount_msat, Some(MilliSatoshi::new(1_000_000)));
        assert_eq!(new.payment_preimage, pre);
        assert_eq!(new.created_at, now);
        let expected_expiry = Timestamp::from(now.into_inner() + chrono::Duration::seconds(3600));
        assert_eq!(new.expiry_at, expected_expiry);
    }

    #[test]
    fn try_new_accepts_amountless() {
        // Amountless invoices (None) carry no nominal amount — the
        // settled amount is sourced from the received HTLCs at Held
        // time. AC4 aggregate-interface prep for Story 5.1's
        // `lnNoAmountInvoiceCreate` entrypoint.
        let (h, pre, w, _, e, b) = ok_args();
        let new =
            NewInvoice::try_new(h, pre, w, None, e, b, "ext-id".to_owned(), fixed_now()).unwrap();
        assert_eq!(new.amount_msat, None);
    }

    #[test]
    fn try_new_coerces_expiry_below_min_to_default() {
        let (h, pre, w, a, _, b) = ok_args();
        let now = fixed_now();
        let low_expiry = BTC_INVOICE_MIN_SECONDS - 1;
        let new = NewInvoice::try_new(h, pre, w, a, low_expiry, b, "ext-id".to_owned(), now)
            .expect("coerced, not rejected");
        let expected_expiry = Timestamp::from(
            now.into_inner() + chrono::Duration::seconds(i64::from(BTC_INVOICE_DEFAULT_SECONDS)),
        );
        assert_eq!(new.expiry_at, expected_expiry);
    }

    #[test]
    fn try_new_coerces_expiry_above_max_to_default() {
        let (h, pre, w, a, _, b) = ok_args();
        let now = fixed_now();
        let high_expiry = BTC_INVOICE_MAX_SECONDS + 1;
        let new = NewInvoice::try_new(h, pre, w, a, high_expiry, b, "ext-id".to_owned(), now)
            .expect("coerced, not rejected");
        let expected_expiry = Timestamp::from(
            now.into_inner() + chrono::Duration::seconds(i64::from(BTC_INVOICE_DEFAULT_SECONDS)),
        );
        assert_eq!(new.expiry_at, expected_expiry);
    }

    #[test]
    fn try_new_rejects_some_zero_amount() {
        let (h, pre, w, _, e, b) = ok_args();
        let err = NewInvoice::try_new(
            h,
            pre,
            w,
            Some(MilliSatoshi::ZERO),
            e,
            b,
            "ext-id".to_owned(),
            fixed_now(),
        )
        .unwrap_err();
        assert!(matches!(err, InvoiceError::InvalidAmount));
    }

    #[test]
    fn try_new_accepts_minimum_expiry() {
        let (h, pre, w, a, _, b) = ok_args();
        assert!(NewInvoice::try_new(
            h,
            pre,
            w,
            a,
            BTC_INVOICE_MIN_SECONDS,
            b,
            "ext-id".to_owned(),
            fixed_now()
        )
        .is_ok());
    }

    #[test]
    fn try_new_accepts_maximum_expiry() {
        let (h, pre, w, a, _, b) = ok_args();
        assert!(NewInvoice::try_new(
            h,
            pre,
            w,
            a,
            BTC_INVOICE_MAX_SECONDS,
            b,
            "ext-id".to_owned(),
            fixed_now()
        )
        .is_ok());
    }

    #[test]
    fn try_from_events_reconstructs_open_invoice() {
        let (h, pre, w, a, e, b) = ok_args();
        let now = fixed_now();
        let new = NewInvoice::try_new(h, pre, w, a, e, b, "ext-id".to_owned(), now).unwrap();
        let id = new.id;
        let entity_events = new.into_events();
        let invoice = Invoice::try_from_events(entity_events).expect("hydrate");
        assert_eq!(invoice.id, id);
        assert_eq!(invoice.state, InvoiceState::Open);
        assert_eq!(invoice.amount_msat, Some(MilliSatoshi::new(1_000_000)));
        assert_eq!(invoice.created_at, now);
        // Per AC3: payment_preimage is Some from creation (not only after Settled).
        assert_eq!(invoice.payment_preimage, pre);
        assert!(invoice.canceled_reason.is_none());
        assert!(invoice.held_amount_msat.is_none());
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

    // ---- Story 2.3/2.4: command-method state machine -------------------

    fn fresh_invoice() -> Invoice {
        let (h, pre, w, a, e, b) = ok_args();
        let new =
            NewInvoice::try_new(h, pre, w, a, e, b, "ext-id".to_owned(), fixed_now()).unwrap();
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
        }
    }

    fn sample_canceled_event() -> InvoiceEvent {
        InvoiceEvent::Canceled {
            canceled_at: fixed_now(),
            reason: CancelReason::Expired,
        }
    }

    #[test]
    fn mark_held_from_open_sets_held_amount() {
        // `held_amount_msat` is load-bearing for AC12 outbox amount
        // reconciliation across `Held → Settled` / `Canceled`. Catches
        // a regression where the field stops being populated by
        // `mark_held`.
        let mut inv = fresh_invoice();
        let htlc = MilliSatoshi::new(750_000);
        let outcome = inv.mark_held(htlc, fixed_now()).unwrap();
        assert!(matches!(outcome, Idempotent::Executed(())));
        assert_eq!(inv.state, InvoiceState::Held);
        assert_eq!(inv.held_amount_msat, Some(htlc));
    }

    #[test]
    fn settle_from_open_is_invalid_state_transition() {
        let mut inv = fresh_invoice();
        match inv.settle(fixed_now()) {
            Err(InvoiceError::InvalidStateTransition {
                from: InvoiceState::Open,
                attempted: "settle",
            }) => {}
            Err(e) => panic!("expected InvalidStateTransition from Open, got error: {e:?}"),
            Ok(_) => panic!("expected InvalidStateTransition from Open, got Ok"),
        }
    }

    #[test]
    fn settle_from_held_executes() {
        let mut inv = fresh_invoice();
        push_event(&mut inv, sample_held_event(), InvoiceState::Held);
        let outcome = inv.settle(fixed_now()).unwrap();
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
        let outcome = inv.cancel(CancelReason::Expired, fixed_now()).unwrap();
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
        let outcome = inv.settle(fixed_now()).unwrap();
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
        match inv.settle(fixed_now()) {
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
    fn try_from_events_held_sets_held_amount() {
        // Hydration must reconstruct `held_amount_msat` from the
        // `HtlcHeld` event so AC12 outbox amount reconciliation works
        // across a process restart. Catches a regression where the
        // builder fold omits the field.
        let (h, pre, w, a, e, b) = ok_args();
        let new =
            NewInvoice::try_new(h, pre, w, a, e, b, "ext-id".to_owned(), fixed_now()).unwrap();
        let id = new.id;
        let htlc = MilliSatoshi::new(420_000);
        let held_at = fixed_now();
        let events = EntityEvents::init(
            id,
            [
                InvoiceEvent::Created {
                    payment_hash: h,
                    payment_preimage: pre,
                    wallet_id: w,
                    amount_msat: a,
                    expiry_at: Timestamp::from(
                        held_at.into_inner() + chrono::Duration::seconds(i64::from(e)),
                    ),
                    bolt_invoice: BoltInvoice::new("lnbc1u1pj..."),
                    external_id: "ext-id".to_owned(),
                    created_at: fixed_now(),
                },
                InvoiceEvent::HtlcHeld {
                    held_at,
                    htlc_amount_msat: htlc,
                },
            ],
        );
        let invoice = Invoice::try_from_events(events).unwrap();
        assert_eq!(invoice.state, InvoiceState::Held);
        assert_eq!(invoice.held_amount_msat, Some(htlc));
    }

    #[test]
    fn try_from_events_reconstructs_settled_after_held_invoice() {
        // Fold over Created → HtlcHeld → Settled.
        let (h, pre, w, a, e, b) = ok_args();
        let new =
            NewInvoice::try_new(h, pre, w, a, e, b, "ext-id".to_owned(), fixed_now()).unwrap();
        let id = new.id;
        let events = EntityEvents::init(
            id,
            [
                InvoiceEvent::Created {
                    payment_hash: h,
                    payment_preimage: pre,
                    wallet_id: w,
                    amount_msat: a,
                    expiry_at: Timestamp::from(
                        fixed_now().into_inner() + chrono::Duration::seconds(i64::from(e)),
                    ),
                    bolt_invoice: BoltInvoice::new("lnbc1u1pj..."),
                    external_id: "ext-id".to_owned(),
                    created_at: fixed_now(),
                },
                InvoiceEvent::HtlcHeld {
                    held_at: fixed_now(),
                    htlc_amount_msat: MilliSatoshi::new(1_000_000),
                },
                InvoiceEvent::Settled {
                    settled_at: fixed_now(),
                },
            ],
        );
        let invoice = Invoice::try_from_events(events).unwrap();
        assert_eq!(invoice.state, InvoiceState::Settled);
        assert_eq!(invoice.payment_preimage, pre);
    }

    #[test]
    fn try_from_events_reconstructs_canceled_invoice() {
        let (h, pre, w, a, e, b) = ok_args();
        let new =
            NewInvoice::try_new(h, pre, w, a, e, b, "ext-id".to_owned(), fixed_now()).unwrap();
        let id = new.id;
        let events = EntityEvents::init(
            id,
            [
                InvoiceEvent::Created {
                    payment_hash: h,
                    payment_preimage: pre,
                    wallet_id: w,
                    amount_msat: a,
                    expiry_at: Timestamp::from(
                        fixed_now().into_inner() + chrono::Duration::seconds(i64::from(e)),
                    ),
                    bolt_invoice: BoltInvoice::new("lnbc1u1pj..."),
                    external_id: "ext-id".to_owned(),
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
