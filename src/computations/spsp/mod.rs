use crate::computations::ComputationProperties;
use crate::error::GSError;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::{Consolidate, Join, JoinCore, Reduce, Threshold};
use differential_dataflow::Collection;
use gs_analytics_api::{ComputationTypes, EdgeArrangement};
use gs_analytics_api::{TimelyComputation, VertexId};
use hashbrown::HashMap;
use hashbrown::HashSet;
use std::convert::TryFrom;
use timely::dataflow::Scope;
use timely::order::Product;

mod differential_df;
mod differential_df_arranged;

const NAME: &str = "SPSP";
const PROPERTY: &str = "goals";
type Goal = (VertexId, VertexId);
type PathLength = u32;

#[derive(Clone)]
pub struct Spsp {
    goals: HashSet<Goal>,
}

impl Spsp {
    pub fn instance(properties: &HashMap<String, ComputationProperties>) -> Result<Self, GSError> {
        if properties.len() != 1 {
            return Err(GSError::PropertyCount(NAME, 1, vec![PROPERTY], properties.len()));
        }
        let value = properties.get(PROPERTY).ok_or_else(|| {
            GSError::Property(NAME, PROPERTY, properties.keys().cloned().collect::<Vec<_>>())
        })?;
        if let ComputationProperties::Pairs(goals) = value {
            Ok(Self {
                goals: goals
                    .iter()
                    .copied()
                    .map(|(u, v)| {
                        (
                            VertexId::try_from(u).expect("Overflow"),
                            VertexId::try_from(v).expect("Overflow"),
                        )
                    })
                    .collect(),
            })
        } else {
            Err(GSError::PropertyType(NAME, PROPERTY, "pairs", value.get_type()))
        }
    }
}

impl ComputationTypes for Spsp {
    type Result = (Goal, PathLength);
}

impl TimelyComputation for Spsp {
    type TimelyResult = ();
}

/// Bi-directional Dijkstra search using arranged forward and reverse edge collections.
///
/// Code adapted from [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow/blob/master/src/algorithms/graphs/bijkstra.rs).
fn bidijkstra_arranged<G>(
    forward: &EdgeArrangement<G>,
    reverse: &EdgeArrangement<G>,
    goals: &Collection<G, Goal>,
) -> Collection<G, (Goal, u32)>
where
    G: Scope,
    G::Timestamp: Lattice + Ord + Copy,
{
    forward.stream.scope().iterative::<u64, _, _>(|inner| {
        let forward_edges = forward.enter(inner);
        let reverse_edges = reverse.enter(inner);

        // Our plan is to start evolving distances from both sources and destinations.
        // The evolution from a source or destination should continue as long as there
        // is a corresponding destination or source that has not yet been reached.

        // forward and reverse (node, (root, dist))
        let forward = Variable::new_from(
            goals.map(|(x, _)| (x, (x, 0))).enter(inner),
            Product::new(Default::default(), 1),
        );
        let reverse = Variable::new_from(
            goals.map(|(_, y)| (y, (y, 0))).enter(inner),
            Product::new(Default::default(), 1),
        );

        let goals = goals.enter(inner);
        // let edges = edges.enter(inner);

        // Let's determine which (src, dst) pairs are ready to return.
        //
        //   done(src, dst) := forward(src, med), reverse(dst, med), goal(src, dst).
        //
        // This is a cyclic join, which should scare us a bunch.
        let reached = forward
            .join_map(&reverse, |_, (src, d1), (dst, d2)| ((*src, *dst), *d1 + *d2))
            .reduce(|_key, s, t| t.push((*s[0].0, 1)))
            .semijoin(&goals);

        let active = reached.negate().map(|(srcdst, _)| srcdst).concat(&goals).consolidate();

        // Let's expand out forward queries that are active.
        let forward_active = active.map(|(x, _y)| x).distinct();
        let forward_next = forward
            .map(|(med, (src, dist))| (src, (med, dist)))
            .semijoin(&forward_active)
            .map(|(src, (med, dist))| (med, (src, dist)))
            .join_core(&forward_edges, |_med, (src, dist), next| Some((*next, (*src, *dist + 1))))
            .concat(&forward)
            .map(|(next, (src, dist))| ((next, src), dist))
            .reduce(|_key, s, t| t.push((*s[0].0, 1)))
            .map(|((next, src), dist)| (next, (src, dist)));

        forward.set(&forward_next);

        // Let's expand out reverse queries that are active.
        let reverse_active = active.map(|(_x, y)| y).distinct();
        let reverse_next = reverse
            .map(|(med, (rev, dist))| (rev, (med, dist)))
            .semijoin(&reverse_active)
            .map(|(rev, (med, dist))| (med, (rev, dist)))
            .join_core(&reverse_edges, |_med, (rev, dist), next| Some((*next, (*rev, *dist + 1))))
            .concat(&reverse)
            .map(|(next, (rev, dist))| ((next, rev), dist))
            .reduce(|_key, s, t| t.push((*s[0].0, 1)))
            .map(|((next, rev), dist)| (next, (rev, dist)));

        reverse.set(&reverse_next);

        reached.leave()
    })
}
