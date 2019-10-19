use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

pub fn keyd_window_3a_rocksdb_count<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, (usize, usize)> {

    let mut last_slide_seen = 0;
    let mut max_window_seen = 0;

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
                // Slice -> distinct keys
                let mut state_index = state_handle.get_managed_map("pane_buckets");
                // pane end timestamp -> pane contents
                let mut pane_buckets = state_handle.get_managed_map("pane_buckets");
                let prefix_key_len: usize = pane_buckets.as_ref().get_key_prefix_length();
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
                        for sl in (start..end).step_by(window_slide_ns) {
                            let window_end = sl + window_slide_ns * (window_slice_count - 1);
                            // println!("Asking notification for the end of window: {:?}", window_end);
                            notificator.notify_at(time.delayed(&window_end));
                        }
                        last_slide_seen = current_slide;
                    }
                    // Add window start so that we can iterate over its contents upon notification
                    if max_window_seen <= current_slide {
                        let end = current_slide + window_slide_ns;
                        for window_start in (max_window_seen..end).step_by(window_slide_ns) {
                            // println!("First PUT operation for window start: {:?}", window_start);
                            pane_buckets.insert((window_start.to_be(),0), 0 as usize);  // Initialize window state
                        }
                        max_window_seen = end;
                    }
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        // Add key do the index, if not exists
                        let slice = ((record.1 / 1_000_000_000) + 1) * 1_000_000_000;
                        let mut exists = false;
                        if let Some(keys) = state_index.get(slice.to_be()) {
                            exists =  keys.iter().any(|k| *k==record.0);
                        }
                        if !exists {
                            let keys = state_index.remove(slice.to_be()).unwrap_or(Vec::new());
                            keys.push(record.0);
                            state_index.insert(slice.to_be(), keys);
                        }
                        let pane = ((record.1 / window_slide_ns) + 1) * window_slide_ns;  // Pane size equals slide size as window is a multiple of slide
                        // println!("Inserting record with time {:?} in pane {:?}", record.1, pane);
                        pane_buckets.rmw((record.0.to_be(), pane.to_be()), 1 as usize);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    let window_end = cap.time();
                    let window_start = window_end - (window_slide_ns * window_slice_count);
                    let first_pane_end = window_start + window_slide_ns; // To know which records to delete
                    // println!("Start of window: {}", window_start);
                    // println!("End of window: {}", *window_end);
                    // println!("End of first slide: {}", first_pane_end);

                    // Get all distinct keys in all slices belonging to the expired window
                    let all_keys = HashSet::new();
                    let slice_start = window_start + 1_000_000_000;
                    let mut index_iter = state_index.iter(slice_start.to_be());
                    for (ser_key, ser_value) in window_iter {
                        let k = &ser_key[prefix_key_len..];  // Ignore prefix
                        let mut timestamp: usize = bincode::deserialize(unsafe {
                                                            std::slice::from_raw_parts(k.as_ptr(), k.len())
                                                      }).expect("Cannot deserialize slice");
                        timestamp = usize::from_be(timestamp);  // The end timestamp of the pane
                        let keys: Vec<usize> = bincode::deserialize(unsafe {
                                                            std::slice::from_raw_parts(ser_value.as_ptr(), ser_value.len())
                                                        }).expect("Cannot deserialize keys");
                        // println!("Found pane:: time: {}, records:{:?}", timestamp, records);
                        if timestamp > window_end {  // Outside keyed window
                            break;
                        }
                        all_leys.insert(timestamp);
                    }

                    // Output result for each keyed window
                    let first_pane = window_start + window_slide_ns;
                    let last_pane = window_end + window_slide_ns;
                    {
                        for key in all_keys.iter() {
                            let mut count = 0;
                            let composite_key = (key.to_be(), first_pane.to_be());
                            let mut auction_id = 0;
                            {// Iterate over the panes belonging to the current window
                                let mut window_iter = pane_buckets.iter(composite_key);
                                let _ = window_iter.next();  // Skip dummy record
                                for (ser_key, ser_value) in window_iter {
                                    let k = &ser_key[prefix_key_len..];  // Ignore prefix
                                    let (mut timestamp, mut auction): (usize, usize) = bincode::deserialize(unsafe {
                                                                        std::slice::from_raw_parts(k.as_ptr(), k.len())
                                                                  }).expect("Cannot deserialize timestamp");
                                    timestamp = usize::from_be(timestamp);  // The end timestamp of the pane
                                    auction_id = usize::from_be(auction);
                                    let record_count: usize = bincode::deserialize(unsafe {
                                                                        std::slice::from_raw_parts(ser_value.as_ptr(), ser_value.len())
                                                                    }).expect("Cannot deserialize count");
                                    // println!("Found pane:: time: {}, records:{:?}", timestamp, records);
                                    if timestamp > last_pane {  // Outside keyed window
                                        break;
                                    }
                                    count += record_count;
                                }
                                // println!("*** End of window: {:?}, Count: {:?}", cap.time(), count);
                                output.session(&cap).give((auction_id, count));
                            }
                        }
                    }
                    
                    // Purge state of first slide/pane in window
                    let slice_end = window_end + 1_000_000_000;
                    for slice in slice_start..slice_end {
                        state_index.remove(&slice).expect("Pane must exist in index");
                    }
                    for key in all_keys {
                        for pane in first_pane..last_pane {
                            let composite_key = (key.to_be(), pane.to_be());
                            // println!("Removing pane with end timestamp time: {}", first_pane_end);
                            pane_buckets.remove(&composite_key).expect("Keyed pane to remove must exist");
                        }
                    }
                });
            }
        )
}
