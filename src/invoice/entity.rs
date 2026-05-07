//! `Invoice` aggregate — event-sourced via `es-entity` derive macros.
//! Pure command method `Invoice::create` validates inputs and emits a
//! `Vec<InvoiceEvent>`. The repo `persist_in_tx` consumes that vec and writes
//! both the projection row and the event-source rows in one transaction.

use es_entity::{EntityEvents, EsEntity, IntoEvents, TryFromEvents};
use serde::{Deserialize, Serialize};
use std::fmt;

use super::{error::InvoiceError, event::InvoiceEvent};
use crate::primitives::{BoltInvoice, InvoiceId, MilliSatoshi, PaymentHash, Timestamp, WalletId};

/// Lifecycle state. Slice 1a only ever creates invoices in `Pending`;
/// `Settled` / `Cancelled` arrive with Story 2.2 (HOLD lifecycle).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "TEXT", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum InvoiceState {
    Pending,
}

impl fmt::Display for InvoiceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
        }
    }
}

/// Input parameters to create a new invoice. Caller (App coordinator) collects
/// these from the GraphQL request + the LND `add_invoice` response (which
/// supplies `payment_hash` and `bolt_invoice`).
#[derive(Clone, Debug)]
pub struct NewInvoice {
    pub payment_hash: PaymentHash,
    pub wallet_id: WalletId,
    pub amount_msat: MilliSatoshi,
    pub expiry_seconds: u32,
    pub memo: Option<String>,
    pub bolt_invoice: BoltInvoice,
}

/// Internal post-validation holder used solely to satisfy the
/// `EsEntity::New: IntoEvents` bound. The repo never constructs this directly;
/// `Invoice::create` returns events that the repo wraps via
/// `EntityEvents::init`.
///
/// Existing only because `IntoEvents::into_events` is infallible — validation
/// must have already happened, so we model that with a separate type.
pub struct NewInvoiceEvents {
    pub(crate) id: InvoiceId,
    pub(crate) events: Vec<InvoiceEvent>,
}

impl IntoEvents<InvoiceEvent> for NewInvoiceEvents {
    fn into_events(self) -> EntityEvents<InvoiceEvent> {
        EntityEvents::init(self.id, self.events)
    }
}

/// Hydrated invoice aggregate. Constructed from the event log via
/// `TryFromEvents`; never built directly outside this module.
#[derive(EsEntity)]
#[es_entity(new = "NewInvoiceEvents")]
pub struct Invoice {
    pub id: InvoiceId,
    pub payment_hash: PaymentHash,
    pub wallet_id: WalletId,
    pub amount_msat: MilliSatoshi,
    pub expiry_at: Timestamp,
    pub state: InvoiceState,
    pub created_at: Timestamp,
    events: EntityEvents<InvoiceEvent>,
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
            .field("state", &self.state)
            .field("created_at", &self.created_at)
            .finish()
    }
}

impl Invoice {
    /// Pure command method — no I/O. Validates inputs, generates a fresh
    /// `InvoiceId`, computes `expiry_at = now + expiry_seconds`, and returns
    /// the resulting `Vec<InvoiceEvent>`.
    ///
    /// Time is injected as `now` so tests are deterministic.
    pub fn create(params: NewInvoice, now: Timestamp) -> Result<Vec<InvoiceEvent>, InvoiceError> {
        // Match galoy's bounds: `INVOICE_EXPIRATIONS["BTC"]` ranges over
        // 60..=86_400 seconds (1 minute to 24 hours).
        if !(60..=86_400).contains(&params.expiry_seconds) {
            return Err(InvoiceError::InvalidExpiry(params.expiry_seconds));
        }
        if params.amount_msat.as_u64() == 0 {
            return Err(InvoiceError::InvalidAmount);
        }

        let id = InvoiceId::new();
        let expiry_at = Timestamp::from(
            now.into_inner() + chrono::Duration::seconds(i64::from(params.expiry_seconds)),
        );

        Ok(vec![InvoiceEvent::Created {
            id,
            payment_hash: params.payment_hash,
            wallet_id: params.wallet_id,
            amount_msat: params.amount_msat,
            expiry_at,
            memo: params.memo,
            bolt_invoice: params.bolt_invoice,
            created_at: now,
        }])
    }
}

impl TryFromEvents<InvoiceEvent> for Invoice {
    fn try_from_events(
        events: EntityEvents<InvoiceEvent>,
    ) -> Result<Self, es_entity::EsEntityError> {
        let mut id: Option<InvoiceId> = None;
        let mut payment_hash: Option<PaymentHash> = None;
        let mut wallet_id: Option<WalletId> = None;
        let mut amount_msat: Option<MilliSatoshi> = None;
        let mut expiry_at: Option<Timestamp> = None;
        let mut created_at: Option<Timestamp> = None;
        let mut state = InvoiceState::Pending;

        for ev in events.iter_all() {
            match ev {
                InvoiceEvent::Created {
                    id: i,
                    payment_hash: h,
                    wallet_id: w,
                    amount_msat: a,
                    expiry_at: e,
                    created_at: c,
                    ..
                } => {
                    id = Some(*i);
                    payment_hash = Some(*h);
                    wallet_id = Some(*w);
                    amount_msat = Some(*a);
                    expiry_at = Some(*e);
                    created_at = Some(*c);
                    state = InvoiceState::Pending;
                }
            }
        }

        Ok(Invoice {
            id: id.ok_or(es_entity::EsEntityError::NotFound)?,
            payment_hash: payment_hash.ok_or(es_entity::EsEntityError::NotFound)?,
            wallet_id: wallet_id.ok_or(es_entity::EsEntityError::NotFound)?,
            amount_msat: amount_msat.ok_or(es_entity::EsEntityError::NotFound)?,
            expiry_at: expiry_at.ok_or(es_entity::EsEntityError::NotFound)?,
            state,
            created_at: created_at.ok_or(es_entity::EsEntityError::NotFound)?,
            events,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn fixed_now() -> Timestamp {
        Timestamp::from(Utc.with_ymd_and_hms(2026, 5, 7, 10, 0, 0).unwrap())
    }

    fn ok_params() -> NewInvoice {
        NewInvoice {
            payment_hash: PaymentHash::from([0xaa; 32]),
            wallet_id: WalletId::new(),
            amount_msat: MilliSatoshi::new(1_000_000),
            expiry_seconds: 3600,
            memo: Some("test".to_owned()),
            bolt_invoice: BoltInvoice::new("lnbc1u1pj..."),
        }
    }

    #[test]
    fn create_happy_path_emits_one_created_event() {
        let now = fixed_now();
        let events = Invoice::create(ok_params(), now).expect("happy path");
        assert_eq!(events.len(), 1);
        match &events[0] {
            InvoiceEvent::Created {
                amount_msat,
                expiry_at,
                created_at,
                ..
            } => {
                assert_eq!(*amount_msat, MilliSatoshi::new(1_000_000));
                assert_eq!(*created_at, now);
                let expected_expiry =
                    Timestamp::from(now.into_inner() + chrono::Duration::seconds(3600));
                assert_eq!(*expiry_at, expected_expiry);
            }
        }
    }

    #[test]
    fn create_rejects_expiry_below_60_seconds() {
        let mut p = ok_params();
        p.expiry_seconds = 59;
        let err = Invoice::create(p, fixed_now()).unwrap_err();
        assert!(matches!(err, InvoiceError::InvalidExpiry(59)));
    }

    #[test]
    fn create_rejects_expiry_above_86400_seconds() {
        let mut p = ok_params();
        p.expiry_seconds = 86_401;
        let err = Invoice::create(p, fixed_now()).unwrap_err();
        assert!(matches!(err, InvoiceError::InvalidExpiry(86_401)));
    }

    #[test]
    fn create_rejects_zero_amount() {
        let mut p = ok_params();
        p.amount_msat = MilliSatoshi::ZERO;
        let err = Invoice::create(p, fixed_now()).unwrap_err();
        assert!(matches!(err, InvoiceError::InvalidAmount));
    }

    #[test]
    fn create_accepts_minimum_expiry() {
        let mut p = ok_params();
        p.expiry_seconds = 60;
        let res = Invoice::create(p, fixed_now());
        assert!(res.is_ok());
    }

    #[test]
    fn create_accepts_maximum_expiry() {
        let mut p = ok_params();
        p.expiry_seconds = 86_400;
        let res = Invoice::create(p, fixed_now());
        assert!(res.is_ok());
    }

    #[test]
    fn try_from_events_reconstructs_pending_invoice() {
        let now = fixed_now();
        let events_vec = Invoice::create(ok_params(), now).unwrap();
        let id = match &events_vec[0] {
            InvoiceEvent::Created { id, .. } => *id,
        };
        let entity_events = EntityEvents::init(id, events_vec);
        let invoice = Invoice::try_from_events(entity_events).expect("hydrate");
        assert_eq!(invoice.id, id);
        assert_eq!(invoice.state, InvoiceState::Pending);
        assert_eq!(invoice.amount_msat, MilliSatoshi::new(1_000_000));
        assert_eq!(invoice.created_at, now);
    }
}
