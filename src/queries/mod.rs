use std::rc::Rc;
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::{Scope, Stream};

use crate::event::{Auction, Bid, Person};

mod window_1_faster;
mod window_1_faster_count;
mod window_1_faster_rank;
mod window_2_faster;
mod window_1_rocksdb;
mod window_1_rocksdb_count;
mod window_1_rocksdb_rank;
mod window_2a_rocksdb;
mod window_2a_rocksdb_count;
mod window_2a_rocksdb_rank;
mod window_2b_rocksdb;
mod window_2b_rocksdb_count;
mod window_2_faster_count;
mod window_2_faster_rank;
mod window_3_faster;
mod window_3a_rocksdb;
mod window_3a_rocksdb_count;
mod window_3b_rocksdb;
mod window_3b_rocksdb_count;
mod window_3_faster_count;
mod window_3_faster_rank;

pub use self::window_1_rocksdb::window_1_rocksdb;
pub use self::window_1_rocksdb_count::window_1_rocksdb_count;
pub use self::window_1_rocksdb_rank::window_1_rocksdb_rank;
pub use self::window_2a_rocksdb::window_2a_rocksdb;
pub use self::window_2a_rocksdb_count::window_2a_rocksdb_count;
pub use self::window_2a_rocksdb_rank::window_2a_rocksdb_rank;
pub use self::window_2b_rocksdb::window_2b_rocksdb;
pub use self::window_2b_rocksdb_count::window_2b_rocksdb_count;
pub use self::window_3a_rocksdb::window_3a_rocksdb;
pub use self::window_3a_rocksdb_count::window_3a_rocksdb_count;
pub use self::window_3b_rocksdb::window_3b_rocksdb;
pub use self::window_3b_rocksdb_count::window_3b_rocksdb_count;
pub use self::window_1_faster::window_1_faster;
pub use self::window_1_faster_count::window_1_faster_count;
pub use self::window_1_faster_rank::window_1_faster_rank;
pub use self::window_2_faster::window_2_faster;
pub use self::window_2_faster_count::window_2_faster_count;
pub use self::window_2_faster_rank::window_2_faster_rank;
pub use self::window_3_faster::window_3_faster;
pub use self::window_3_faster_count::window_3_faster_count;
pub use self::window_3_faster_rank::window_3_faster_rank;


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

/*impl NexmarkTimer {
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
}*/
