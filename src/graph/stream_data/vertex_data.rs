use crate::graph::properties::operations::{LeftOperand, Operand, Operator, RightOperand};
use crate::graph::properties::property_value::PropertyValue;
use crate::graph::{GraphPointer, VertexId};
use crate::query_handler::create_view::{PredicateFunction, PredicateFunctionClosure};
use std::sync::Arc;

#[allow(clippy::wildcard_enum_match_arm)]
pub fn get_vertex_closure(
    operand1: LeftOperand,
    operator: Operator,
    operand2: RightOperand,
) -> PredicateFunction {
    let closure: PredicateFunctionClosure = match operand1 {
        Operand::Property(key_id1) => match operand2 {
            RightOperand::Value(right_value) => {
                Arc::new(move |vertex_id: VertexId, graph_pointer: GraphPointer| {
                    let pv = PropertyValue::get_id(vertex_id);
                    graph_pointer
                        .get_vertex_id_property_value(vertex_id, &pv, key_id1)
                        .map_or(false, |left_value| left_value.compare(&right_value, operator))
                })
            }
            RightOperand::Variable(Operand::Property(key_id2)) => {
                Arc::new(move |vertex_id: VertexId, graph_pointer: GraphPointer| {
                    let pv = PropertyValue::get_id(vertex_id);
                    graph_pointer.get_vertex_id_property_value(vertex_id, &pv, key_id2).map_or(
                        false,
                        |right_value| {
                            graph_pointer
                                .get_vertex_id_property_value(vertex_id, &pv, key_id1)
                                .map_or(false, |left_value| {
                                    left_value.compare(&right_value, operator)
                                })
                        },
                    )
                })
            }
            _ => unreachable!("Right vertex operand should not have edge properties"),
        },
        _ => unreachable!("Left vertex operand should not have edge properties"),
    };
    PredicateFunction::new(closure)
}
