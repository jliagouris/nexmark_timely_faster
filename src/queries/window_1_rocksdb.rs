use std::collections::HashMap;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::event::Bid;
use crate::queries::{NexmarkInput, NexmarkTimer};
use faster_rs::FasterRmw;
use rocksdb::{WriteBatch, DB};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;


impl FasterRmw for Bids {
    fn rmw(&self, _modification: Self) -> Self {
        panic!("RMW on AuctionBids not allowed!");
    }
}

pub fn window_1_rocksdb<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, usize> {
    input
        .bids(scope)
        .unary_notify(
            Exchange::new(|b: &Bid| b.auction as u64),
            "Accumulate records",
            None,
            move |input, output, notificator, state_handle| {
                let mut records = state_handle.get_managed_map("state");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    // Notify at end of this epoch
                    notificator.notify_at(
                        time.delayed(&(((time.time() / window_slide_ns) + 1) * window_slide_ns)),
                    );
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        let key = record.date_time;  // Use event timestamp as key
                        let _ = records.insert(key, record);
                    }
                });
                notificator.for_each(|cap, _, _| {
                    let window_start = cap.time() - window_slide_ns * window_slice_count;
                    let mut iter = records.
                    let window = records.range(window_start, &cap.time());
                    if window.len() > 0 {
                        output.session(&cap)
                            .give(window);
                    }
                });
            },
        )
}
