use std::rc::Rc;
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::{Scope, Stream};

use crate::event::{Auction, Bid, Date, Person};

mod keyed_window_1_rocksdb_count;
mod keyed_window_1_rocksdb_rank;
mod keyed_window_1_faster_count_custom_slice;
mod keyed_window_1_faster_rank_custom_slice;
mod keyed_window_2a_rocksdb_count;
mod keyed_window_2b_rocksdb_rank;
mod keyed_window_3_faster_count;
mod keyed_window_2_faster_rank;
mod keyed_window_3a_rocksdb_count;

pub use self::keyed_window_1_rocksdb_count::keyed_window_1_rocksdb_count;
pub use self::keyed_window_1_rocksdb_rank::keyed_window_1_rocksdb_rank;
pub use self::keyed_window_1_faster_count_custom_slice::keyed_window_1_faster_count_custom_slice;
pub use self::keyed_window_1_faster_rank_custom_slice::keyed_window_1_faster_rank_custom_slice;
pub use self::keyed_window_2a_rocksdb_count::keyed_window_2a_rocksdb_count;
pub use self::keyed_window_2b_rocksdb_rank::keyed_window_2b_rocksdb_rank;
pub use self::keyed_window_3a_rocksdb_count::keyed_window_3a_rocksdb_count;
pub use self::keyed_window_2_faster_rank::keyed_window_2_faster_rank;
pub use self::keyed_window_3_faster_count::keyed_window_3_faster_count;

use faster_rs::FasterKv;

#[inline(always)]
fn maybe_refresh_faster(faster: &FasterKv, monotonic_serial_number: &mut u64) {
    if *monotonic_serial_number % (1 << 4) == 0 {
        faster.refresh();
        if *monotonic_serial_number % (1 << 10) == 0 {
            faster.complete_pending(true);
        }
    }
    if *monotonic_serial_number % (1 << 20) == 0 {
        println!("Size: {}", faster.size());
    }
    *monotonic_serial_number += 1;
}


pub struct NexmarkInput<'a> {
    pub bids: &'a Rc<EventLink<usize, Bid>>,
    pub auctions: &'a Rc<EventLink<usize, Auction>>,
    pub people: &'a Rc<EventLink<usize, Person>>,
    pub closed_auctions: &'a Rc<EventLink<usize, (Auction, Bid)>>,
    pub closed_auctions_flex: &'a Rc<EventLink<usize, (Auction, Bid)>>,
}

impl<'a> NexmarkInput<'a> {
    pub fn bids<S: Scope<Timestamp = usize>>(&self, scope: &mut S) -> Stream<S, Bid> {
        Some(self.bids.clone()).replay_into(scope)
    }

    pub fn auctions<S: Scope<Timestamp = usize>>(&self, scope: &mut S) -> Stream<S, Auction> {
        Some(self.auctions.clone()).replay_into(scope)
    }

    pub fn people<S: Scope<Timestamp = usize>>(&self, scope: &mut S) -> Stream<S, Person> {
        Some(self.people.clone()).replay_into(scope)
    }

    pub fn closed_auctions<S: Scope<Timestamp = usize>>(
        &self,
        scope: &mut S,
    ) -> Stream<S, (Auction, Bid)> {
        Some(self.closed_auctions.clone()).replay_into(scope)
    }
}

#[derive(Copy, Clone)]
pub struct NexmarkTimer {
    pub time_dilation: usize,
}

impl NexmarkTimer {
    #[inline(always)]
    fn to_nexmark_time(self, x: usize) -> Date {
        debug_assert!(
            x.checked_mul(self.time_dilation).is_some(),
            "multiplication failed: {} * {}",
            x,
            self.time_dilation
        );
        Date::new(x * self.time_dilation)
    }

    #[inline(always)]
    fn from_nexmark_time(self, x: Date) -> usize {
        *x / self.time_dilation
    }
}
