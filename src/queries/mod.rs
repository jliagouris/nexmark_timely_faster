use std::rc::Rc;
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::{Scope, Stream};

use crate::event::{Auction, Bid, Date, Person};

mod q1;
mod q2;
mod q3_managed;
mod q4_managed;
mod q4_q6_common;
mod q5_managed;
mod q6_managed;
mod q7_managed;
mod q8_managed;

pub use self::q1::q1;
pub use self::q2::q2;
pub use self::q3_managed::q3_managed;
pub use self::q4_managed::q4_managed;
pub use self::q4_q6_common::q4_q6_common;
pub use self::q5_managed::q5_managed;
pub use self::q6_managed::q6_managed;
pub use self::q7_managed::q7_managed;
pub use self::q8_managed::q8_managed;

pub struct NexmarkInput<'a> {
    //pub control: &'a Rc<EventLink<usize, Control>>,
    pub bids: &'a Rc<EventLink<usize, Bid>>,
    pub auctions: &'a Rc<EventLink<usize, Auction>>,
    pub people: &'a Rc<EventLink<usize, Person>>,
    pub closed_auctions: &'a Rc<EventLink<usize, (Auction, Bid)>>,
    pub closed_auctions_flex: &'a Rc<EventLink<usize, (Auction, Bid)>>,
}

impl<'a> NexmarkInput<'a> {
    /*
    pub fn control<S: Scope<Timestamp=usize>>(&self, scope: &mut S) -> Stream<S, Control> {
        Some(self.control.clone()).replay_into(scope)
    }
    */

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

    pub fn closed_auctions_flex<S: Scope<Timestamp = usize>>(
        &self,
        scope: &mut S,
    ) -> Stream<S, (Auction, Bid)> {
        Some(self.closed_auctions_flex.clone()).replay_into(scope)
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
