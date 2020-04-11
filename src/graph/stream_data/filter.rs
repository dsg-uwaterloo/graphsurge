use crate::graph::GraphPointer;
use crate::graph::VertexOrEdgeId;
use crate::query_handler::create_view::{PredicateFunction, WhereCondition};

pub fn test_where_conditions(
    id: VertexOrEdgeId,
    where_conditions: &[WhereCondition],
    graph_pointer: GraphPointer,
) -> bool {
    for (is_negation, where_predicates) in where_conditions {
        let is_success = where_predicates
            .iter()
            .all(|(_, closure)| test_edge_where_predicate(closure, id, graph_pointer));
        if (*is_negation && is_success) || (!*is_negation && !is_success) {
            return false;
        }
    }
    true
}

#[inline]
pub fn test_edge_where_predicate(
    closure: &PredicateFunction,
    id: VertexOrEdgeId,
    graph_pointer: GraphPointer,
) -> bool {
    (&*closure.0)(id, graph_pointer)
}

#[cfg(test)]
mod tests {
    use crate::global_store::GlobalStore;
    use crate::graph::properties::operations::{Operand, Operator, RightOperand};
    use crate::graph::properties::property_value::PropertyValue;
    use crate::graph::stream_data::edge_data::get_edge_closure;
    use crate::graph::stream_data::filter::test_edge_where_predicate;
    use crate::graph::GraphPointer;
    use crate::process_query;
    use itertools::Itertools;

    #[test]
    fn test_edge_filter() {
        let mut global_store = GlobalStore::default();

        let mut graph_query = "load graph with vertices from 'data/small_properties/vertices.txt'
         and edges from 'data/small_properties/edges.txt' comment '#';"
            .to_string();
        process_query(&mut global_store, &mut graph_query).expect("Graph not loaded");

        let graph = &global_store.graph;
        let graph_pointer = GraphPointer::new(&graph);
        let key_store = &global_store.key_store;

        let amount_key_id = key_store.get_key_id("amount").expect("property key missing");
        let year_key_id = key_store.get_key_id("year").expect("property key missing");
        let id_key_id = key_store.get_key_id("id").expect("property key missing");
        let include_key_id = key_store.get_key_id("include").expect("property key missing");
        let include2_key_id = key_store.get_key_id("include2").expect("property key missing");
        let city_key_id = key_store.get_key_id("city").expect("property key missing");
        let gender_key_id = key_store.get_key_id("gender").expect("property key missing");

        // Test for every combination of left and right operands, because each of them constructs
        // a different closure.
        for (index, (left_operand, operator, right_operand, expected_edge_ids)) in vec![
            (
                Operand::Property(amount_key_id),
                Operator::LessEqual,
                RightOperand::Value(PropertyValue::Isize(200)),
                vec![1, 2, 3],
            ),
            (
                Operand::Property(amount_key_id),
                Operator::Greater,
                RightOperand::Variable(Operand::Property(year_key_id)),
                vec![4],
            ),
            (
                Operand::Property(amount_key_id),
                Operator::Greater,
                RightOperand::Variable(Operand::Edge),
                vec![],
            ),
            (
                Operand::Property(year_key_id),
                Operator::GreaterEqual,
                RightOperand::Variable(Operand::SourceVertex(id_key_id)),
                vec![0, 1, 2, 3, 4, 5, 6],
            ),
            (
                Operand::Property(include_key_id),
                Operator::Equal,
                RightOperand::Variable(Operand::DestinationVertex(include_key_id)),
                vec![3, 6],
            ),
            (
                Operand::Edge,
                Operator::Equal,
                RightOperand::Value(PropertyValue::Pair(6, 7)),
                vec![4],
            ),
            (
                Operand::Edge,
                Operator::NotEqual,
                RightOperand::Variable(Operand::Property(id_key_id)),
                vec![0, 1, 2, 3, 4, 5, 6],
            ),
            (
                Operand::Edge,
                Operator::Equal,
                RightOperand::Variable(Operand::Edge),
                vec![0, 1, 2, 3, 4, 5, 6],
            ),
            (Operand::Edge, Operator::NotEqual, RightOperand::Variable(Operand::Edge), vec![]),
            (
                Operand::SourceVertex(city_key_id),
                Operator::Equal,
                RightOperand::Value(PropertyValue::String("waterloo".to_string())),
                vec![1, 2],
            ),
            (
                Operand::SourceVertex(include_key_id),
                Operator::NotEqual,
                RightOperand::Variable(Operand::Property(include_key_id)),
                vec![2, 3, 5],
            ),
            (
                Operand::SourceVertex(include_key_id),
                Operator::NotEqual,
                RightOperand::Variable(Operand::SourceVertex(include2_key_id)),
                vec![0, 5],
            ),
            (
                Operand::SourceVertex(gender_key_id),
                Operator::Equal,
                RightOperand::Variable(Operand::DestinationVertex(gender_key_id)),
                vec![1, 4, 5],
            ),
            (
                Operand::DestinationVertex(include2_key_id),
                Operator::NotEqual,
                RightOperand::Variable(Operand::Property(include_key_id)),
                vec![0, 1, 3, 6],
            ),
            (
                Operand::DestinationVertex(gender_key_id),
                Operator::NotEqual,
                RightOperand::Variable(Operand::SourceVertex(gender_key_id)),
                vec![0, 2, 3, 6],
            ),
            (
                Operand::DestinationVertex(include_key_id),
                Operator::Equal,
                RightOperand::Variable(Operand::DestinationVertex(include2_key_id)),
                vec![0, 1],
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let closure = get_edge_closure(left_operand, operator, right_operand);
            let filtered_edge_ids = (0..graph.edges_count())
                .filter(|edge_id| test_edge_where_predicate(&closure, *edge_id, graph_pointer))
                .collect_vec();
            assert_eq!(
                filtered_edge_ids, expected_edge_ids,
                "edge filter failed for test {}",
                index
            );
        }
    }
}
