//! Struct for merging multiple sorted channels into a single iterator

use crossbeam::channel::Receiver;
use std::cmp::Ordering;
use std::fmt::Debug;

/// Representation of a merged set of channels as an iterator
///
/// Depends upon the assumption that all data in chans is already sorted.
///
/// Waits on chans at start of each next call to ensure that we have one head_item per channel.
///
/// Upon reading each head_item they are inserted into head_items using a binary_search and insert.
///
/// Once we have as many head_items as chans we pop the head and save the index that the item came
/// from. On the next iteration we wait on that channel before repeating the insert and pop.
///
/// Once we exhaust a channel we swap the exhausted channel with the last one, pop it, and find the
/// highest ID in head_items and replace it with the new ID which was assigned to the exhausted
/// channel.
///
/// Start yielding only None when chans is empty
#[derive(Debug)]
pub struct MergedChannels<T> {
    /// Set of channels to merge input from
    chans: Vec<Receiver<T>>,
    /// Sorted list of head items already grabbed from other channels and the index of that channel
    /// in chans
    head_items: Vec<(T, usize)>,
    /// the index of the source chan of the previously yielded head_item
    last_picked: Option<usize>,
}

impl<T> Iterator for MergedChannels<T>
where
    T: Ord + Debug,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        trace!("enter");
        if self.chans.is_empty() {
            return None;
        }

        if let Some(id) = self.last_picked {
            self.receive_from(id);
        } else {
            self.receive_from_all();
        }

        self.get_next_head_item()
    }
}

impl<T> MergedChannels<T>
where
    T: Ord + Debug,
{
    /// Construct a merged channels
    #[tracing::instrument]
    pub fn new(chans: Vec<Receiver<T>>) -> Self {
        trace!("enter");
        Self {
            chans,
            head_items: vec![],
            last_picked: None,
        }
    }

    /// pop the lowest item in the vec and save the id of the channel that item came from
    #[tracing::instrument]
    fn get_next_head_item(&mut self) -> Option<T> {
        trace!("enter");
        self.head_items.pop().map(|(item, last_picked)| {
            self.last_picked = Some(last_picked);

            item
        })
    }

    /// Receive the next item from chan id and insert that into head_items
    #[tracing::instrument]
    fn receive_from(&mut self, id: usize) {
        trace!("enter");
        match self.chans[id].recv() {
            Ok(item) => self.sorted_insert((item, id)),
            Err(e) => {
                debug!(message = "channel exhausted", ?id, ?e);
                self.remove_channel(id);
            }
        }
    }

    /// Receive one item for each channel to fill up head_items
    fn receive_from_all(&mut self) {
        trace!("enter");
        for id in 0..self.chans.len() {
            self.receive_from(id);
        }
    }

    /// Insert item into head_items in sorted order
    #[tracing::instrument]
    fn sorted_insert(&mut self, item: (T, usize)) {
        trace!("enter");
        let ind = match self
            .head_items
            .binary_search_by(|probe| match probe.cmp(&item) {
                Ordering::Less => Ordering::Greater,
                Ordering::Greater => Ordering::Less,
                item => item,
            }) {
            Ok(_id) => unreachable!(), // exact match exists. should never have duplicate ids
            Err(id) => id,             // insert location to maintain sort order
        };

        self.head_items.insert(ind, item);
    }

    /// Remove a channel that is exhausted from the set of channels and adjust the id of the
    /// head_item that swapped with the removed channel to indicate its new location in the chans
    /// vector
    #[tracing::instrument]
    fn remove_channel(&mut self, id: usize) {
        trace!(message = "removing id", ?id, ?self);
        let _ = self.chans.swap_remove(id);

        let old_id = self.chans.len();

        if let Some(dirty_head_item) = self.head_items.iter_mut().find(|item| item.1 == old_id) {
            dirty_head_item.1 = id;
        };

        trace!(message = "removed id", ?id, ?self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path() {
        crate::init_script("info");

        let (s1, r1) = crossbeam::channel::unbounded();
        let (s2, r2) = crossbeam::channel::unbounded();

        let mut lines = ["hi", "okay", "abc"];
        lines.sort();

        for line in lines.iter() {
            s1.send(line.to_string()).unwrap();
        }

        drop(s1);

        let mut lines2 = ["bcd", "hoho", "zyz"];
        lines2.sort();

        for line in lines2.iter() {
            s2.send(line.to_string()).unwrap();
        }

        drop(s2);

        let m = MergedChannels::new(vec![r1, r2]);

        for (id, item) in m.enumerate() {
            info!(%item, %id);
        }
    }
}
