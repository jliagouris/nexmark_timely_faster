use std::collections::HashMap;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use faster_rs::FasterRmw;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;
use crate ::event::Bid;


pub fn window_1_faster<S: Scope<Timestamp = usize>>(
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
            Exchange::new(|b: &(usize, _)| b.0 as u64),
            "Accumulate records",
            None,
            move |input, output, notificator, state_handle| {
                // Slide end timestamp -> event timestamps
                let mut slide_index = state_handle.get_managed_map("slide_index");
                // Event timestmaps -> auction ids
                let mut window_contents = state_handle.get_managed_map("window_contents");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    // Notify at the end of this slide
                    let slide = (((time.time() / window_slide_ns) + 1) * window_slide_ns);
                    notificator.notify_at(time.delayed(&slide));
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        window_contents.insert(record.1, record.0);
                        slide_index.rmw(slide, vec![record.1]);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    for i in (0..window_slice_count).rev() {
                        let keys = slide_index.get(&((cap.time() - i) * window_slide_ns)).expect("Slide must exist");
                        for timestamp in keys.as_ref() {
                            let value = window_contents.get(timestamp).expect("Timestamp must exist");
                            output.session(&cap).give((*timestamp, *value.as_ref()));
                        }
                    }
                    // TODO (john): remove() doesn't actually remove entries from FASTER
                    let keys_to_remove = slide_index.remove(&((cap.time() - window_slice_count) * window_slide_ns)).expect("Slide to remove must exist");
                    for timestamp in keys_to_remove {
                        let _ = window_contents.remove(&timestamp).expect("Timestamp to remove must exist");
                    }
                });
            },
        )
}
