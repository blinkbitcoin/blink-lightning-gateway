//! Symphony-as-client placeholder.
//!
//! Slice 1 has no synchronous Symphony calls. The first real client
//! method lands in Story 2.2: an `authorize_spend`-equivalent
//! (analogous to `blink-card/src/symphony/client.rs::authorize_spend`)
//! that the gateway invokes before `LND::send_payment` to reserve the
//! sender's wallet liability against insufficient-balance / frozen-account
//! / amount-exceeds-limit. A separate `get_balance`-equivalent
//! (analogous to `blink-card/src/symphony/collateral_client.rs::get_balance`)
//! may follow if any LN-gateway GraphQL op needs to read wallet balance
//! directly.
//!
//! Symphony's *consumer* side (the long-lived `SubscribeEvents` stream
//! reader) lives in the symphony repo at
//! `symphony/src/gateways/lightning_gateway.rs`. The gateway-as-server
//! bootstrap lives in `src/server/` (Story 2.1).
