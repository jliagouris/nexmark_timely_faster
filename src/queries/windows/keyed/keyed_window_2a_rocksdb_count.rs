use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer, assign_windows};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

// 2nd window implementation using put + merge
pub fn keyed_window_2a_rocksdb_count<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, (usize, usize, usize)> {
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
            "Count records per key",
            None,
            move |input, output, notificator, state_handle| {
                let window_size = window_slice_count * window_slide_ns;
                // window start timestamp -> window contents
                let mut window_buckets = state_handle.get_managed_map("window_buckets");
                let prefix_key_len: usize = window_buckets.as_ref().get_key_prefix_length();

                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        let windows = assign_windows(record.1, window_slide_ns, window_size);
                        for win in windows {
                            // Notify at end of this window
                            notificator.notify_at(time.delayed(&(win + window_size)));
                            println!("Asking notification for window {:?} with end timestamp {:?}", win, win + window_size);
                            window_buckets.rmw((win.to_be(), record.0.to_be()), 1);
                            println!("Appending record with timestamp {} to window {:?}.", record.1, (win, win + window_size));
                            if let Some(count) = window_buckets.get(&(win.to_be(), record.0.to_be())) {
                                println!("Inserted count {} fro key {:?}", count, (win, record.0));
                            }
                            else{
                                panic!("Key doesn't exist.");
                            }
                        }
                    }
                });

                notificator.for_each(|cap, _, _| {
                    println!("Firing window with start timestamp {}.", cap.time() - window_size);
                    let start_timestamp = cap.time() - window_size;
                    println!("Prefix scan using key: {:?}", (start_timestamp, 0));
                    let iter = window_buckets.iter((start_timestamp.to_be(), 0usize.to_be()));
                    for (ser_key, ser_value) in iter {
                        let k = &ser_key[prefix_key_len..];  // Ignore prefix
                        let (timestamp, key): (usize, usize) = bincode::deserialize(unsafe {
                                                                            std::slice::from_raw_parts(k.as_ptr(), k.len())
                                                                       }).expect("Cannot deserialize keyed window id");
                        let timestamp = usize::from_be(timestamp);
                        let key = usize::from_be(key);
                        let count: usize = bincode::deserialize(unsafe {
                                                            std::slice::from_raw_parts(ser_value.as_ptr(), ser_value.len())
                                                        }).expect("Cannot deserialize count");
                        println!("Found keyed window ({},{}) with count: {}.", timestamp, key, count);
                        if timestamp != start_timestamp {  // Outside keyed window
                            break;
                        }
                        output.session(&cap).give((*cap.time(), key, count));
                    }
                    // Purge state
                    let from = (start_timestamp.to_be(), 0usize.to_be());
                    let to = (start_timestamp.to_be(), usize::max_value().to_be());
                    println!("Purging state from {:?} to {:?}", (start_timestamp, 0), (start_timestamp, usize::max_value()));
                    window_buckets.delete_range(from, to);
                });
            },
        )
}
