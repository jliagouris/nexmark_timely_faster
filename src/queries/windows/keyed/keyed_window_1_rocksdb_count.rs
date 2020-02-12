use std::collections::HashMap;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};
use bincode;

use crate::queries::{NexmarkInput, NexmarkTimer};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

const MIN_KEY: u64 = 0;
const MAX_KEY: u64 = u64::max_value();

pub fn keyed_window_1_rocksdb_count<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, (usize, u64, usize)> {

    let mut last_slide_seen = 0;

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
                let prefix_key_len: usize = window_contents.as_ref().get_key_prefix_length();
                let mut buffer = Vec::new();

                input.for_each(|time, data| {
                    // The end timestamp of the slide the current epoch corresponds to
                    let current_slide = ((time.time() / window_slide_ns) + 1) * window_slide_ns;
                    // println!("Current slide: {:?}", current_slide);
                    // Ask notifications for all remaining slides up to the current one
                    assert!(last_slide_seen <= current_slide);
                    if last_slide_seen < current_slide {
                        let start = last_slide_seen + window_slide_ns;
                        let end = current_slide + window_slide_ns;
                        // Make sure we don't miss any request for notification
                        for sl in (start..end).step_by(window_slide_ns) {
                            let window_end = sl + window_slide_ns * (window_slice_count - 1);
                            // println!("Asking notification for the end of window: {:?}", window_end);
                            notificator.notify_at(time.delayed(&window_end));
                            // Add enties for window margins so that we can iterate over the
                            // window contents upon notification
                            // TODO (john): We don't need these dummy entries if we use Seek() and
                            // change loop condition below
                            // println!("Inserting dummy record:: time: {:?}, value:{:?}", sl - window_slide_ns, 0);
                            // Start timestamp of window, minimum possible key (zero)
                            window_contents.insert(((sl - window_slide_ns).to_be(), MIN_KEY.to_be()), 0);
                            // println!("Inserting dummy record:: time: {:?}, value:{:?}", window_end, 0);
                            // End timestamp of window, max possible key
                            window_contents.insert((window_end.to_be(), MAX_KEY.to_be()), 0);
                        }
                        last_slide_seen = current_slide;
                    }
                    // Add window contents
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        let auction_id = record.0;
                        // Construct the composite key: (event time, auction id)
                        let key = ((record.1).to_be(), auction_id.to_be() as u64);
                        // println!("Inserting window record:: time: {}, value:{}", key, auction_id);
                        window_contents.insert(key, auction_id);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    let window_end = cap.time();
                    let window_start = window_end - (window_slide_ns * window_slice_count);
                    // Keep the end of first slide to know which records to delete
                    let first_slide_end = window_start + window_slide_ns;
                    // println!("Start of window: {}", window_start);
                    // println!("End of window: {}", *window_end);
                    // println!("End of first slide: {}", first_slide_end);
                    let mut to_delete = Vec::new();  // Keep keys to delete here
                    to_delete.push((window_start, MIN_KEY));
                    // A mapping form key to the number of entries with that key in the window
                    let mut key_counts = HashMap::new();
                    {   // Scan the window and count the number of entries per key
                        let mut window_iter = window_contents.iter((window_start.to_be(), MIN_KEY.to_be()));
                        let _ = window_iter.next();  // Skip first dummy record (window start)
                        for (ser_key, _ser_value) in window_iter {
                            let k = &ser_key[prefix_key_len..];  // Ignore prefix
                            let (mut timestamp, mut auction_id): (usize, u64) = bincode::deserialize(unsafe {
                                                        std::slice::from_raw_parts(k.as_ptr(), k.len())
                                                    }).expect("Cannot deserialize key");
                            timestamp = usize::from_be(timestamp);
                            auction_id = u64::from_be(auction_id);
                            // Note (john): We don't need to deserialize the values for COUNT
                            // println!("Found record:: time: {}", timestamp);
                            if (timestamp % window_slide_ns) != 0 {  // Omit last dummy record
                                let e = key_counts.entry(auction_id).or_insert(1);
                                *e += 1
                            }
                            if timestamp == *window_end {  // Reached end of the window, exit loop
                                break;
                            }
                            // Record event time must fall in the expired window
                            assert!(timestamp < *window_end);
                            // Keep track of entries in the first slide that must be discarded
                            if timestamp < first_slide_end {
                                to_delete.push((timestamp, auction_id));
                            }
                        }
                    }
                    // println!("*** End of window: {:?}, Count: {:?}", cap.time(), count);
                    for (auction, count) in key_counts.drain() {
                        output.session(&cap).give((*cap.time(), auction, count));
                    }
                    // Purge state of first slide in the expired window
                    for (ts, a_id) in to_delete {
                        // println!("Removing record with key ({},{})", ts, a_id);
                        window_contents.remove(&(ts.to_be(),a_id.to_be())).expect("Record to remove must exist");
                    }
                });
            },
        )
}
