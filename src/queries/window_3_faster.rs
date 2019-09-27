use std::collections::HashMap;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use faster_rs::FasterRmw;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;
use crate ::event::Bid;

pub fn window_3_faster<S: Scope<Timestamp = usize>>(
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
                *b.date_time
            )
        })
        .unary_notify(
            Exchange::new(|b: &(usize, _)| b.0 as u64),
            "Accumulate records",
            None,
            move |input, output, notificator, state_handle| {
                // pane end timestamp -> pane contents
                let mut pane_buckets = state_handle.get_managed_map("pane_buckets");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    // Notify for this pane
                    notificator.notify_at(time.delayed(&(((time.time() / window_slide_ns) + 1) * window_slide_ns)));
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        let pane = record.1 / window_slide_ns;  // Pane size equals slide size as window is a multiple of slide
                        pane_buckets.rmw(pane, vec![*record]);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    let records = pane_buckets.get(&cap.time()).expect("Pane must exist");
                    for record in records.iter() {
                        output.session(&cap).give(record.clone());
                    }
                    // Remove pane
                    let pane = cap.time() - window_slice_count;
                    let _ = pane_buckets.remove(&cap.time()).expect("Pane to remove must exist");
                });
            }
        )
}
