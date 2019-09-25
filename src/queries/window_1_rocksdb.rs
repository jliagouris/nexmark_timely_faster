use std::collections::HashMap;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};
use bincode;

use crate::event::Bid;
use crate::queries::{NexmarkInput, NexmarkTimer};
use timely::state::backends::RocksDBBackend;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;


pub fn window_1_rocksdb<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, (usize, usize)> {
    input
        .bids(scope)
        .map(move |b| {
            (
                b.auction,
                ((*b.date_time / window_slide_ns) + 1) * window_slide_ns,
            )
        })
        .unary_notify(
            Exchange::new(|b: &(usize,_)| b.0 as u64),
            "Accumulate records",
            None,
            move |input, output, notificator, state_handle| {
                let mut window_contents = state_handle.get_managed_map("window_contents");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    // Notify at end of this epoch
                    notificator.notify_at(
                        time.delayed(&(((time.time() / window_slide_ns) + 1) * window_slide_ns)),
                    );
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        let key = record.1; 
                        let auction_id = record.0;
                        let _ = window_contents.insert(key, auction_id);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    let window_end = cap.time();
                    let window_start = window_end - window_slide_ns * window_slice_count;
                    let mut window_iter = window_contents.iter(window_start);
                    for (key, value) in window_iter {
                        let timestamp: usize = bincode::deserialize(key.as_ref()).expect("Cannot deserialize timestamp");
                        let auction_id: usize = bincode::deserialize(value.as_ref()).expect("Cannot deserialize auction id");
                        // TODO (john): Apply aggregation
                        output.session(&cap).give((timestamp, auction_id));
                        if timestamp == *window_end {
                            // TODO (john): Purge window state in [window_start, window_start + window_slide_ns]
                            break;
                        }
                    }
                });
            },
        )
}
