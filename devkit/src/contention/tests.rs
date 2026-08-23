// Copyright 2026 Developers of the reconcile project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::cell::RefCell;
use std::sync::Mutex;

use super::*;

struct RecordingTarget(Mutex<Vec<u64>>);

impl ContentionTarget for RecordingTarget {
    fn insert(&self, key: u64) {
        self.0.lock().unwrap().push(key);
    }
}

#[test]
fn timed_concurrent_insert_partitions_keys_into_disjoint_per_thread_blocks() {
    let target = RecordingTarget(Mutex::new(Vec::new()));
    let (prefill, n, ops) = (1_000, 4, 50);
    timed_concurrent_insert(&target, n, ops, prefill);

    let mut keys = target.0.into_inner().unwrap();
    keys.sort_unstable();
    let expected: Vec<u64> = (prefill as u64..prefill as u64 + (n * ops) as u64).collect();
    assert_eq!(
        keys, expected,
        "n*ops disjoint keys starting at prefill, no gaps or duplicates"
    );
}

#[test]
fn throughput_ops_per_sec_divides_total_ops_by_elapsed_seconds() {
    let elapsed = Duration::from_secs(2);
    assert_eq!(throughput_ops_per_sec(elapsed, 4, 100), 200.0);
}

// `order` is a `RefCell`, not a plain `Vec`, because both closures below need to mutate it and
// `paired_trial` takes them as two simultaneous `&mut dyn FnMut`s: a plain `Vec` capture would
// need two live mutable borrows of the same variable at once, which the borrow checker rejects
// regardless of the fact that only one closure ever actually runs before the other returns.

#[test]
fn paired_trial_runs_a_before_b_when_a_first() {
    let order = RefCell::new(Vec::new());
    let (a, b) = paired_trial(
        true,
        &mut || {
            order.borrow_mut().push('a');
            1.0
        },
        &mut || {
            order.borrow_mut().push('b');
            2.0
        },
    );
    assert_eq!(*order.borrow(), vec!['a', 'b']);
    assert_eq!((a, b), (1.0, 2.0));
}

#[test]
fn paired_trial_runs_b_before_a_when_not_a_first() {
    let order = RefCell::new(Vec::new());
    let (a, b) = paired_trial(
        false,
        &mut || {
            order.borrow_mut().push('a');
            1.0
        },
        &mut || {
            order.borrow_mut().push('b');
            2.0
        },
    );
    assert_eq!(*order.borrow(), vec!['b', 'a']);
    assert_eq!((a, b), (1.0, 2.0));
}

#[test]
fn run_sweep_records_exactly_trials_paired_samples_per_count() {
    let counts = [1usize, 2usize];
    let trials = 6;
    let points = run_sweep(&counts, trials, 0, 42, |_n| 4.0, |_n| 2.0, None);

    assert_eq!(points.len(), counts.len());
    for point in &points {
        assert_eq!(point.a.len(), trials);
        assert_eq!(point.b.len(), trials);
        assert!(point.a.iter().all(|&v| v == 4.0));
        assert!(point.b.iter().all(|&v| v == 2.0));
        assert!(point.ratio.iter().all(|&r| r == 2.0));
        assert_eq!(
            point.ratio_a_first.len() + point.ratio_b_first.len(),
            trials,
            "every trial recorded in exactly one order bucket"
        );
    }
}

#[test]
fn run_sweep_calls_on_trial_once_per_retained_trial() {
    let counts = [1usize, 3usize];
    let trials = 4;
    let mut seen = Vec::new();
    let mut on_trial = |n: usize, a: f64, b: f64, a_first: bool| {
        seen.push((n, a, b, a_first));
    };
    run_sweep(
        &counts,
        trials,
        0,
        7,
        |_n| 1.0,
        |_n| 1.0,
        Some(&mut on_trial),
    );
    assert_eq!(seen.len(), counts.len() * trials);
}

#[test]
fn run_sweep_alternates_a_first_across_warmup_trials() {
    let order = RefCell::new(Vec::new());
    run_sweep(
        &[1],
        0,
        2,
        0,
        |_n| {
            order.borrow_mut().push('a');
            1.0
        },
        |_n| {
            order.borrow_mut().push('b');
            1.0
        },
        None,
    );
    assert_eq!(
        *order.borrow(),
        vec!['a', 'b', 'b', 'a'],
        "warmup trial 0 (even) runs a first, trial 1 (odd) runs b first"
    );
}

#[test]
fn run_sweep_marks_alternating_trials_as_a_first_by_parity() {
    let mut a_first_count = 0;
    let mut on_trial = |_n, _a, _b, a_first: bool| {
        if a_first {
            a_first_count += 1;
        }
    };
    run_sweep(&[1], 5, 0, 0, |_n| 1.0, |_n| 1.0, Some(&mut on_trial));
    assert_eq!(
        a_first_count, 3,
        "trials 0, 2, 4 (even) are a_first, out of 5 trials"
    );
}

#[test]
fn run_sweep_computes_delta_ns_per_op_as_the_reciprocal_difference() {
    let points = run_sweep(&[1], 1, 0, 0, |_n| 2.0, |_n| 4.0, None);
    let expected = 1e9 * (1.0 / 2.0 - 1.0 / 4.0);
    assert_eq!(points[0].delta_ns_per_op, vec![expected]);
}
