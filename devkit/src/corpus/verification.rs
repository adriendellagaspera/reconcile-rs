use std::collections::BTreeSet;

pub struct ExactDifference<T> {
    plus: Vec<T>,
    minus: Vec<T>,
}

impl<T: Ord + Clone> ExactDifference<T> {
    pub fn new(left: &[T], right: &[T]) -> Self {
        let left: BTreeSet<_> = left.iter().cloned().collect();
        let right: BTreeSet<_> = right.iter().cloned().collect();
        Self {
            plus: left.difference(&right).cloned().collect(),
            minus: right.difference(&left).cloned().collect(),
        }
    }

    pub fn matches(&self, plus: &[T], minus: &[T]) -> bool {
        let mut plus = plus.to_vec();
        let mut minus = minus.to_vec();
        plus.sort_unstable();
        minus.sort_unstable();
        plus == self.plus && minus == self.minus
    }

    pub fn verify(&self, plus: &[T], minus: &[T]) {
        assert!(
            self.matches(plus, minus),
            "decoded difference failed exact verification"
        );
    }
}

#[cfg(test)]
mod tests;
