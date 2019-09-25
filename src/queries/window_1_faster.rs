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
                let mut slide_index = state_handle.get_managed_map("slide_index");
                let mut window_contents = state_handle.get_managed_map("window_contents");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    // Notify at end of this epoch
                    let slide = (((time.time() / window_slide_ns) + 1) * window_slide_ns);
                    notificator.notify_at(time.delayed(&slide));
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        let _ = window_contents.insert(record.1, record.0);
                        let _ = slide_index.rmw(slide, vec![record.1]);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    for i in 0..window_slice_count {
                        let keys = slide_index.get(&((cap.time() - i) * window_slide_ns)).expect("Slide must exist");
                        let timestamps = keys.as_ref();
                        for timestamp in timestamps {
                            let value = window_contents.get(timestamp).expect("Timestamp must exist");
                            let record = value.as_ref();
                            output.session(&cap).give((*timestamp, *record));
                        }
                    }
                    let keys_to_remove = slide_index.remove(&((cap.time() - window_slice_count) * window_slide_ns));
                    // TODO (john): remove entries from window_contents
                });
            },
        )
}
