use std::collections::{BTreeMap, BTreeSet};

mod accounting;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct LogicalRecord {
    pub key: u64,
    pub value: u64,
    pub version: u64,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum DifferenceCase {
    Insert,
    EqualCountUpdate,
    Delete,
    DivergentVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ViewError {
    Mismatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RangeSummary {
    pub count: usize,
    pub fingerprint: u64,
}

#[derive(Clone)]
pub(super) struct ScanOnlyStore {
    view_id: String,
    records: BTreeMap<u64, LogicalRecord>,
}

impl ScanOnlyStore {
    pub(super) fn scan(&self, view_id: &str) -> Result<Vec<LogicalRecord>, ViewError> {
        self.guard(view_id)?;
        Ok(self.records.values().copied().collect())
    }

    pub(super) fn lookup(
        &self,
        view_id: &str,
        keys: &BTreeSet<u64>,
    ) -> Result<Vec<LogicalRecord>, ViewError> {
        self.guard(view_id)?;
        Ok(keys
            .iter()
            .filter_map(|key| self.records.get(key))
            .copied()
            .collect())
    }

    fn guard(&self, view_id: &str) -> Result<(), ViewError> {
        if self.view_id == view_id {
            Ok(())
        } else {
            Err(ViewError::Mismatch)
        }
    }
}

#[derive(Clone)]
pub(super) struct OrderedSummaryStore {
    view_id: String,
    records: BTreeMap<u64, LogicalRecord>,
}

impl OrderedSummaryStore {
    pub(super) fn range(
        &self,
        view_id: &str,
        start: u64,
        end: u64,
    ) -> Result<Vec<LogicalRecord>, ViewError> {
        self.guard(view_id)?;
        Ok(self
            .records
            .range(start..end)
            .map(|(_, record)| *record)
            .collect())
    }

    pub(super) fn lookup(
        &self,
        view_id: &str,
        keys: &BTreeSet<u64>,
    ) -> Result<Vec<LogicalRecord>, ViewError> {
        self.guard(view_id)?;
        Ok(keys
            .iter()
            .filter_map(|key| self.records.get(key))
            .copied()
            .collect())
    }

    pub(super) fn summary(
        &self,
        view_id: &str,
        start: u64,
        end: u64,
    ) -> Result<RangeSummary, ViewError> {
        let records = self.range(view_id, start, end)?;
        Ok(RangeSummary {
            count: records.len(),
            fingerprint: records
                .iter()
                .fold(0, |acc, record| acc ^ fingerprint(record)),
        })
    }

    fn guard(&self, view_id: &str) -> Result<(), ViewError> {
        if self.view_id == view_id {
            Ok(())
        } else {
            Err(ViewError::Mismatch)
        }
    }
}

pub(super) struct FixturePair {
    pub view_id: String,
    pub left_scan: ScanOnlyStore,
    pub right_scan: ScanOnlyStore,
    pub left_ordered: OrderedSummaryStore,
    pub right_ordered: OrderedSummaryStore,
    reference: ExactReference,
}

impl FixturePair {
    pub(super) fn verify_difference_ids(&self, ids: &BTreeSet<u64>) -> bool {
        self.reference.difference_ids() == *ids
    }

    pub(super) fn verify_right_payloads(
        &self,
        ids: &BTreeSet<u64>,
        payloads: &[LogicalRecord],
    ) -> bool {
        self.reference.right_payloads(ids) == payloads
    }
}

struct ExactReference {
    left: BTreeMap<u64, LogicalRecord>,
    right: BTreeMap<u64, LogicalRecord>,
}

impl ExactReference {
    fn difference_ids(&self) -> BTreeSet<u64> {
        self.left
            .keys()
            .chain(self.right.keys())
            .copied()
            .filter(|key| self.left.get(key) != self.right.get(key))
            .collect()
    }

    fn right_payloads(&self, ids: &BTreeSet<u64>) -> Vec<LogicalRecord> {
        ids.iter()
            .filter_map(|key| self.right.get(key))
            .copied()
            .collect()
    }
}

pub(super) fn fixture(case: DifferenceCase) -> FixturePair {
    let view_id = format!("fixture-{case:?}");
    let left = base_records();
    let mut right = left.clone();
    match case {
        DifferenceCase::Insert => {
            right.insert(5, record(5, 500, 0));
        }
        DifferenceCase::EqualCountUpdate => {
            right.insert(2, record(2, 999, 1));
        }
        DifferenceCase::Delete => {
            right.remove(&3);
        }
        DifferenceCase::DivergentVersion => {
            right.insert(4, record(4, 400, 7));
        }
    }

    FixturePair {
        view_id: view_id.clone(),
        left_scan: scan_store(&view_id, &left),
        right_scan: scan_store(&view_id, &right),
        left_ordered: ordered_store(&view_id, &left),
        right_ordered: ordered_store(&view_id, &right),
        reference: ExactReference {
            left: left.clone(),
            right: right.clone(),
        },
    }
}

pub(super) fn difference_ids(left: &[LogicalRecord], right: &[LogicalRecord]) -> BTreeSet<u64> {
    let left = records_by_key(left);
    let right = records_by_key(right);
    left.keys()
        .chain(right.keys())
        .copied()
        .filter(|key| left.get(key) != right.get(key))
        .collect()
}

fn records_by_key(records: &[LogicalRecord]) -> BTreeMap<u64, LogicalRecord> {
    records.iter().map(|record| (record.key, *record)).collect()
}

fn base_records() -> BTreeMap<u64, LogicalRecord> {
    (1..=4)
        .map(|key| (key, record(key, key * 100, 0)))
        .collect()
}

fn scan_store(view_id: &str, records: &BTreeMap<u64, LogicalRecord>) -> ScanOnlyStore {
    ScanOnlyStore {
        view_id: view_id.to_owned(),
        records: records.clone(),
    }
}

fn ordered_store(view_id: &str, records: &BTreeMap<u64, LogicalRecord>) -> OrderedSummaryStore {
    OrderedSummaryStore {
        view_id: view_id.to_owned(),
        records: records.clone(),
    }
}

fn record(key: u64, value: u64, version: u64) -> LogicalRecord {
    LogicalRecord {
        key,
        value,
        version,
    }
}

fn fingerprint(record: &LogicalRecord) -> u64 {
    record.key.wrapping_mul(0x9e37_79b9).rotate_left(17)
        ^ record.value.rotate_left(7)
        ^ record.version.wrapping_mul(0xbf58_476d)
}

#[cfg(test)]
mod tests;
