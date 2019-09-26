use std::collections::HashMap;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use faster_rs::FasterRmw;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;
use crate ::event::Bid;

pub fn assign_windows(event_time: usize,
                      window_slide: usize,
                      window_size: usize
                     ) -> Vec<usize> {
    let mut windows = Vec::new();
    let last_window_start = (event_time / window_slide) * window_slide;
    let num_windows = ((window_size / window_slide) as f64).ceil();
    for i in 0..num_windows as usize {
        let mut w_id = last_window_start - (i * window_slide);
        if event_time < w_id + window_size {
            windows.push(w_id);
        }
    }
    windows
}

pub fn window_2_faster<S: Scope<Timestamp = usize>>(
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
                // window_start_timestamp -> window_contents
                let mut window_buckets = state_handle.get_managed_map("window_buckets");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        let windows = assign_windows(record.1, window_slide_ns, window_slice_count * window_slide_ns);
                        for win in windows {
                            // Notify at end of this window
                            notificator.notify_at(time.delayed(&(win + window_slide_ns * window_slice_count)));
                            window_buckets.rmw(win, vec![*record]);
                        }
                    }
                });

                notificator.for_each(|cap, _, _| {
                    let records = window_buckets.remove(&(cap.time() - window_slide_ns * window_slice_count)).expect("Must exist");
                    for record in records.iter() {
                        output.session(&cap).give(record.clone());
                    }
                });
            },
        )
}
