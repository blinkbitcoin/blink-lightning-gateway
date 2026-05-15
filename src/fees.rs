//! Fee policy. Slice 2 lands the LN max-fee policy; intraledger and
//! MPP-retry fees join in later slices.

pub mod policy;

pub use policy::LnFees;
