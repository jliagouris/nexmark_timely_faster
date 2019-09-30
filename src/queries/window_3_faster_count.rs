use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

pub fn window_3_faster_count<S: Scope<Timestamp = usize>>(
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
                    let slide = ((time.time() / window_slide_ns) + 1) * window_slide_ns;
                    //println!("Asking notification for end of window: {:?}", slide + (window_slide_ns * (window_slice_count - 1)));
                    notificator.notify_at(time.delayed(&(slide + window_slide_ns * (window_slice_count - 1))));
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        let pane = ((record.1 / window_slide_ns) + 1) * window_slide_ns;  // Pane size equals slide size as window is a multiple of slide
                        //println!("Inserting record with time {:?} in pane {:?}", record.1, pane);
                        pane_buckets.rmw(pane, vec![*record]);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    //println!("Received notification for end of window {:?}", &(cap.time()));
                    let mut count = 0;
                    //lookup all panes in the window
                    for i in 0..window_slice_count {
                        let pane = cap.time() - window_slide_ns * i;
                        //println!("Lookup pane {:?}", &pane);
                        if let Some(records) = pane_buckets.get(&pane) {
                            for _record in records.iter() {
                                count+=1;
                            }
                        } else {
                            println!("Processing pane {} of last window.", cap.time() - window_slide_ns * i);
                        }
                        // remove the first slide of the fired window
                        // TODO (john): remove() doesn't actually remove entries from FASTER
                        if i == window_slice_count - 1 {
                            //println!("Removing pane {:?}", pane);
                            let _ = pane_buckets.remove(&pane).expect("Pane to remove must exist");
                        }
                    }
                    println!("*** End of window: {:?}, Count: {:?}", cap.time(), count);
                    output.session(&cap).give((*cap.time(), count));
                });
            }
        )
}
