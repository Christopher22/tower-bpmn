use std::ops::{Index, IndexMut};

/// A unique identifier for an item usable as an index.
#[derive(Debug)]
pub struct Id<T> {
    id: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Id<T> {
    const fn new(id: usize) -> Self {
        Id {
            id,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> Clone for Id<T> {
    fn clone(&self) -> Self {
        Self::new(self.id)
    }
}

impl<T> Copy for Id<T> {}

impl<T> From<usize> for Id<T> {
    fn from(id: usize) -> Self {
        Id::new(id)
    }
}

impl<T> PartialEq for Id<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Eq for Id<T> {}

impl<T> std::hash::Hash for Id<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<T> PartialOrd for Id<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl<T> Ord for Id<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Store<T>(Vec<T>);

impl<T> Store<T> {
    pub fn push(&mut self, item: T) -> Id<T> {
        let id = self.0.len();
        self.0.push(item);
        Id::new(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (Id<T>, &T)> {
        self.0
            .iter()
            .enumerate()
            .map(|(i, item)| (Id::new(i), item))
    }
}

impl<T> Default for Store<T> {
    fn default() -> Self {
        Store(Vec::new())
    }
}

impl<T> Index<Id<T>> for Store<T> {
    type Output = T;

    fn index(&self, index: Id<T>) -> &Self::Output {
        &self.0[index.id]
    }
}

impl<T> IndexMut<Id<T>> for Store<T> {
    fn index_mut(&mut self, index: Id<T>) -> &mut Self::Output {
        &mut self.0[index.id]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store() {
        let mut store = Store::default();
        let id1 = store.push(10);
        let id2 = store.push(20);
        assert_eq!(store[id1], 10);
        assert_eq!(store[id2], 20);
    }
}
