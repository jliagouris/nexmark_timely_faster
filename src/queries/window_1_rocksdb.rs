use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};
use bincode;

use crate::queries::{NexmarkInput, NexmarkTimer};
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
                *b.date_time
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
                    // Notify at end of this slide
                    notificator.notify_at(
                        time.delayed(&(((time.time() / window_slide_ns) + 1) * window_slide_ns)),
                    );
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        let key = record.1; // Event time
                        let auction_id = record.0;
                        window_contents.insert(key, auction_id);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    // The end of each slide is also the end of a window since the window is always a multiple of the slide
                    let window_end = cap.time(); // Last slide id
                    let window_start = window_end - (window_slide_ns * window_slice_count) ;  // First slide id
                    // TODO (john): Check if iterator works if key is not found, i.e. if it starts at the next key
                    {
                        let window_iter = window_contents.iter(window_start);
                        for (key, value) in window_iter {
                            let timestamp: usize = bincode::deserialize(key.as_ref()).expect("Cannot deserialize timestamp");
                            let auction_id: usize = bincode::deserialize(value.as_ref()).expect("Cannot deserialize auction id");
                            output.session(&cap).give((timestamp, auction_id));
                            if timestamp == *window_end {
                                break;
                            }
                        }
                    }
                    // Purge state in [window_start - window_slide_ns, window_start)
                    for ts in (window_start-window_slide_ns)..window_start {
                        let _ = window_contents.remove(&ts).expect("Record to remove must exist");
                    }
                });
            },
        )
}
