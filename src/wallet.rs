//! Wallet bounded-context support. The gateway does NOT own the `Wallet`
//! aggregate (blink-core does); this module holds the cross-subgraph
//! ownership-validation seam, not a local Wallet projection.

pub mod ownership;

pub use ownership::{
    ApolloRouterOwnershipChecker, CallerAuth, WalletOwnershipChecker, WalletOwnershipConfig,
    WalletOwnershipError,
};
