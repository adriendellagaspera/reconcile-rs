# lww-register

Last-write-wins register domain used by reconcile: register entries plus timestamp and clock
primitives.

Replication membership, persistence, storage, networking and wall-clock adapters live outside this
crate.

This package is currently an implementation detail. Applications should depend on reconcile and use
its re-exported API.

Licensed under MIT OR Apache-2.0.
