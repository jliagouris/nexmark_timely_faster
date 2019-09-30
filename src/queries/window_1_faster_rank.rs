use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

pub fn window_1_faster_rank<S: Scope<Timestamp = usize>>(
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
                *b.date_time,
            )
        })
        .unary_notify(
            Exchange::new(|b: &(usize, _)| b.0 as u64),
            "Accumulate records",
            None,
            move |input, output, notificator, state_handle| {
                // Slide end timestamp -> event timestamps
                let mut slide_index = state_handle.get_managed_map("slide_index");
                // Event timestamp -> auction id
                let mut window_contents = state_handle.get_managed_map("window_contents");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    // Notify at the end of this slide
                    let slide = ((time.time() / window_slide_ns) + 1) * window_slide_ns;
                    //println!("Asking notification for end of window: {:?}", slide + (window_slide_ns * (window_slice_count - 1)));
                    notificator.notify_at(time.delayed(&(slide + window_slide_ns * (window_slice_count - 1))));
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        window_contents.insert(record.1, record.0);
                        //println!("Inserting timestamp in the index: slide: {:?}, timestamp: {:?}", slide, record.1);
                        slide_index.rmw(slide, vec![record.1]);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    //println!("End of window: {:?}", cap.time());
                    let mut records = Vec::new();
                    for i in 0..window_slice_count {
                        //println!("Lookup slide {:?}", &(cap.time() - window_slide_ns * i));
                        if let Some(keys) = slide_index.get(&(cap.time() - window_slide_ns * i)) {
                            for timestamp in keys.as_ref() {
                                let value = window_contents.get(timestamp).expect("Timestamp must exist");
                                records.push(value);
                            }
                        }
                        else {
                            println!("Processing slide {} of last window.", cap.time() - window_slide_ns * i);
                        }
                    }
                    // sort window contents
                    records.sort_unstable();
                    let mut rank = 1;
                    let mut count = 0;
                    let mut current_record = *records[0].as_ref();
                    for record in &records {
                        // output (timestamp, auctionID, rank)
                        let auction = *record.as_ref();
                        if auction != current_record {
                            // increase rank and update current
                            rank+=count;
                            count = 0;
                            current_record = auction;
                        }
                        count+=1;
                        output.session(&cap).give((*cap.time(), auction, rank));
                        println!("*** End of window: {:?}, Auction: {:?}, Rank: {:?}", cap.time(), auction, rank);
                    }
                    // TODO (john): remove() doesn't actually remove entries from FASTER
                    //println!("Removing slide {:?}", &(cap.time() - (window_slice_count - 1) * window_slide_ns));
                    if let Some(keys_to_remove) = slide_index.remove(&(cap.time() - (window_slice_count - 1) * window_slide_ns)) {
                        for timestamp in keys_to_remove {
                            let _ = window_contents.remove(&timestamp).expect("Timestamp to remove must exist");
                        }
                    }
                    else {
                        println!("Tried to remove slide {} of last window, which doesn't exist.", cap.time() - (window_slice_count - 1) * window_slide_ns);
                    }
                });
            },
        )
}