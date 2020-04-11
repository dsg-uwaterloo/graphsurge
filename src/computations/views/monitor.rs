use crate::computations::TimelyTimeStamp;
use crate::util::timer::GSTimer;
use log::info;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};
use timely::Data;

pub trait MonitorStream<S: Scope<Timestamp = TimelyTimeStamp>, D: Data> {
    fn monitor(&self, limit: usize, context: &'static str, worker_index: usize) -> Stream<S, D>;
}

impl<S: Scope<Timestamp = TimelyTimeStamp>, D: Data> MonitorStream<S, D> for Stream<S, D> {
    fn monitor(&self, limit: usize, context: &'static str, worker_index: usize) -> Stream<S, D> {
        let mut count = 0;
        let mut current_limit = limit;
        let timer = GSTimer::now();
        let mut vector = Vec::new();
        self.unary(Pipeline, "Monitor", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    count += vector.len();
                    output.session(&time).give_vec(&mut vector);
                    if count >= current_limit {
                        info!(
                            "[worker {:>2}] {} items processed for {} in {}",
                            worker_index,
                            count,
                            context,
                            timer.elapsed().to_seconds_string()
                        );
                        current_limit += limit;
                    }
                });
            }
        })
    }
}
