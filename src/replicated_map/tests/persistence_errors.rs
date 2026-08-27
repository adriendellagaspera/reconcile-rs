// Copyright 2023 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! `PersistenceLoadError` display-text coverage, split out of `persistence.rs` (#99) to keep that
//! file under AGENTS.md §3's file-size budget.

use crate::replicated_map::PersistenceLoadError;

/// #99: `PersistenceLoadError`'s `Display` text is user-facing (it's what `with_persistence`
/// panics with) — assert its actual content for both variants, not merely that formatting them
/// doesn't panic.
#[test]
fn persistence_load_error_display_messages() {
    let err = std::io::Error::other("boom");
    assert_eq!(
        PersistenceLoadError::Corrupt(err).to_string(),
        "persisted state is corrupt or from an incompatible format, refusing to silently start fresh: boom"
    );
    let err = std::io::Error::other("boom");
    assert_eq!(
        PersistenceLoadError::RetriesExhausted(err).to_string(),
        format!(
            "failed to load persisted state after {} attempts: boom",
            super::super::persistence::LOAD_RETRY_ATTEMPTS
        )
    );
}
