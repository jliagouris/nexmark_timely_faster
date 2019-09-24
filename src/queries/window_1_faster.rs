use std::collections::HashMap;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use faster_rs::FasterRmw;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;
use crate ::event::Bid;

#[derive(Deserialize, Serialize)]
struct Counts(HashMap<usize, usize>);

impl FasterRmw for Counts {
    fn rmw(&self, _modification: Self) -> Self {
        panic!("RMW on Counts not allowed!");
    }
}

#[derive(Deserialize, Serialize)]
struct AuctionBids((usize, usize));

impl FasterRmw for AuctionBids {
    fn rmw(&self, _modification: Self) -> Self {
        panic!("RMW on AuctionBids not allowed!");
    }
}

pub fn window_1_faster<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, Bid> {
    input
        .bids(scope)
        // // TODO: Could pre-aggregate pre-exchange, if there was reason to do so.
        // .unary_notify(
        //     Exchange::new(|b: &(usize, _)| b.auction as u64),
        //     "Append records",
        //     None,
        //     move |input, output, notificator, state_handle| {
        //         let mut slides = state_handle.get_managed_map("state1");s
        //         let mut records = state_handle.get_managed_map("state2");
        //         let mut buffer = Vec::new();
        //         input.for_each(|time, data| {
        //             // Notify at end of this epoch
        //             let slide = &(((time.time() / window_slide_ns) + 1) * window_slide_ns);
        //             notificator.notify_at(
        //                 time.delayed(slide),
        //             );
        //             data.swap(&mut buffer);
        //             for record in buffer.iter() {
        //                 let key = record.date_time;
        //                 let _ = records.insert(key, record);
        //                 let _ slides.rmw(slide, key, FUNCTION);
        //             }
        //         });

        //         notificator.for_each(|cap, _, _| {
        //             let mut window = Vec::new();
        //             for i in 0..window_slice_count.rev() {
        //                 if let Some(timestamps) =
        //                     slides.get(&(cap.time() - i * window_slide_ns))
        //                 {
        //                     for timestamp in timestamps {
        //                         let record = records.get(timestamp);
        //                         window.push_back(record);
        //                     }
        //                 }
        //             }
        //             output.session(&cap).give(window);
        //         });
        //     },
        // )
}
