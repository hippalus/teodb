//! Catalog-backed distributed snapshot pin registry.
//!
//! In multi-node deployments, one data node's orphan sweep must not delete
//! files still pinned by a query running on a *different* data node. The
//! in-memory registry ([`teodb_core::snapshot_pin::InMemorySnapshotRegistry`])
//! cannot see across processes, so it is unsafe once retention windows are
//! short.
//!
//! [`CatalogSnapshotRegistry`] stores pins as Iceberg table properties, reusing
//! the shared REST catalog as the cross-node coordination primitive — the same
//! approach as the compaction lock (see [`crate::coordination`]): the catalog
//! provides the CAS, so no bespoke coordination service is required. A pin
//! written by node A is visible to node B's sweeper through the table it
//! already loads.
//!
//! ## Leases
//!
//! Each pin carries a lease (`owner` node id, `created_ms`, `deadline_ms`). A
//! pin is honored only while `now < deadline`. Long-running queries
//! [`renew`](teodb_core::snapshot_pin::ActiveSnapshotRegistry::renew) before the
//! deadline; a query whose node crashes leaves a pin that any node expires once
//! the lease elapses
//! ([`expire_stale`](teodb_core::snapshot_pin::ActiveSnapshotRegistry::expire_stale)).
//! Leases bound the blast radius of a lost release to `lease_ttl`, rather than
//! leaking pins forever.
//!
//! ## Write-side ownership vs. read-side visibility
//!
//! `pin`/`renew`/`release` are issued by the one node that owns the query, so
//! this registry keeps a small local index of which tables a query pinned to
//! target those writes without scanning the catalog. `active_snapshots` reads
//! straight from the table properties, so it sees every node's pins — that is
//! the cross-node guarantee the orphan sweep depends on.

mod registry;

pub use registry::{CatalogSnapshotRegistry, SnapshotRegistryConfig};

#[cfg(test)]
mod tests;
