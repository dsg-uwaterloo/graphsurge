use crate::graph::properties::operations::{LeftOperand, Operand, Operator, RightOperand};
use crate::graph::properties::property_value::PropertyValue;
use crate::graph::Edge;
use crate::graph::GraphPointer;
use crate::query_handler::create_view::{PredicateFunction, PredicateFunctionClosure};
use gs_analytics_api::EdgeId;
use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::Arc;

pub fn get_edge_closure(
    operand1: LeftOperand,
    operator: Operator,
    operand2: RightOperand,
) -> PredicateFunction {
    let closure: PredicateFunctionClosure = match operand1 {
        Operand::Property(left_edge_pki) => match operand2 {
            RightOperand::Value(right_value) => {
                Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                    let graph = graph_pointer.deref();
                    let edge = graph.get_edge(edge_id);

                    let pv = PropertyValue::get_id(edge_id);
                    let left = graph.get_edge_property_value(edge, &pv, left_edge_pki);

                    compare_left_opt_right(left, &right_value, operator)
                })
            }
            RightOperand::Variable(right_variable) => match right_variable {
                Operand::Property(right_edge_pki) => {
                    Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                        let graph = graph_pointer.deref();
                        let edge = graph.get_edge(edge_id);

                        let pv = PropertyValue::get_id(edge_id);
                        let left = graph.get_edge_property_value(edge, &pv, left_edge_pki);
                        let right = graph.get_edge_property_value(edge, &pv, right_edge_pki);

                        compare_left_opt_right_opt(left, right, operator)
                    })
                }
                Operand::Edge => Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                    let graph = graph_pointer.deref();
                    let edge = graph.get_edge(edge_id);

                    let pv = PropertyValue::get_id(edge_id);
                    let left = graph.get_edge_property_value(edge, &pv, left_edge_pki);
                    let right_value = get_edge_pair(edge);

                    compare_left_opt_right(left, &right_value, operator)
                }),
                Operand::SourceVertex(right_src_pki) => {
                    Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                        let graph = graph_pointer.deref();
                        let edge = graph.get_edge(edge_id);

                        let pv = PropertyValue::get_id(edge_id);
                        let left = graph.get_edge_property_value(edge, &pv, left_edge_pki);

                        let pv = PropertyValue::get_id(edge.src_vertex_id);
                        let right = graph.get_vertex_id_property_value(
                            edge.src_vertex_id,
                            &pv,
                            right_src_pki,
                        );

                        compare_left_opt_right_opt(left, right, operator)
                    })
                }
                Operand::DestinationVertex(right_dst_pki) => {
                    Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                        let graph = graph_pointer.deref();
                        let edge = graph.get_edge(edge_id);

                        let pv = PropertyValue::get_id(edge_id);
                        let left = graph.get_edge_property_value(edge, &pv, left_edge_pki);

                        let pv2 = PropertyValue::get_id(edge.dst_vertex_id);
                        let right = graph.get_vertex_id_property_value(
                            edge.dst_vertex_id,
                            &pv2,
                            right_dst_pki,
                        );

                        compare_left_opt_right_opt(left, right, operator)
                    })
                }
            },
        },
        Operand::Edge => match operand2 {
            RightOperand::Value(right_value) => {
                Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                    let graph = graph_pointer.deref();
                    let edge = graph.get_edge(edge_id);
                    let left_value = get_edge_pair(edge);

                    left_value.compare(&right_value, operator)
                })
            }
            RightOperand::Variable(right_variable) => match right_variable {
                Operand::Property(right_edge_pki) => {
                    Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                        let graph = graph_pointer.deref();
                        let edge = graph.get_edge(edge_id);
                        let left_value = get_edge_pair(edge);

                        let pv = PropertyValue::get_id(edge_id);
                        let right = graph.get_edge_property_value(edge, &pv, right_edge_pki);

                        compare_left_right_opt(&left_value, right, operator)
                    })
                }
                Operand::Edge => Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                    let graph = graph_pointer.deref();
                    let edge = graph.get_edge(edge_id);

                    let edge_value = get_edge_pair(edge);

                    edge_value.compare(&edge_value, operator)
                }),
                Operand::SourceVertex(right_src_pki) => {
                    Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                        let graph = graph_pointer.deref();
                        let edge = graph.get_edge(edge_id);
                        let left_value = get_edge_pair(edge);

                        let pv = PropertyValue::get_id(edge.src_vertex_id);
                        let right = graph.get_vertex_id_property_value(
                            edge.src_vertex_id,
                            &pv,
                            right_src_pki,
                        );

                        compare_left_right_opt(&left_value, right, operator)
                    })
                }
                Operand::DestinationVertex(right_dst_pki) => {
                    Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                        let graph = graph_pointer.deref();
                        let edge = graph.get_edge(edge_id);

                        let left_value = get_edge_pair(edge);
                        let pv = PropertyValue::get_id(edge.dst_vertex_id);
                        let right = graph.get_vertex_id_property_value(
                            edge.dst_vertex_id,
                            &pv,
                            right_dst_pki,
                        );

                        compare_left_right_opt(&left_value, right, operator)
                    })
                }
            },
        },
        Operand::SourceVertex(left_src_pki) => match operand2 {
            RightOperand::Value(right_value) => {
                Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                    let graph = graph_pointer.deref();
                    let edge = graph.get_edge(edge_id);

                    let pv = PropertyValue::get_id(edge.src_vertex_id);
                    let left =
                        graph.get_vertex_id_property_value(edge.src_vertex_id, &pv, left_src_pki);

                    compare_left_opt_right(left, &right_value, operator)
                })
            }
            RightOperand::Variable(right_variable) => match right_variable {
                Operand::Property(right_edge_pki) => {
                    Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                        let graph = graph_pointer.deref();
                        let edge = graph.get_edge(edge_id);

                        let pv = PropertyValue::get_id(edge.src_vertex_id);
                        let left = graph.get_vertex_id_property_value(
                            edge.src_vertex_id,
                            &pv,
                            left_src_pki,
                        );

                        let pv2 = PropertyValue::get_id(edge_id);
                        let right = graph.get_edge_property_value(edge, &pv2, right_edge_pki);

                        compare_left_opt_right_opt(left, right, operator)
                    })
                }
                Operand::Edge => Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                    let graph = graph_pointer.deref();
                    let edge = graph.get_edge(edge_id);

                    let pv = PropertyValue::get_id(edge.src_vertex_id);
                    let left =
                        graph.get_vertex_id_property_value(edge.src_vertex_id, &pv, left_src_pki);

                    let right_value = get_edge_pair(edge);

                    compare_left_opt_right(left, &right_value, operator)
                }),
                Operand::SourceVertex(right_src_pki) => {
                    Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                        let graph = graph_pointer.deref();
                        let edge = graph.get_edge(edge_id);

                        let vertex_id = edge.src_vertex_id;
                        let vertex = graph.get_vertex(vertex_id);
                        let pv = PropertyValue::get_id(vertex_id);
                        let left = graph.get_vertex_property_value(vertex, &pv, left_src_pki);
                        let right = graph.get_vertex_property_value(vertex, &pv, right_src_pki);

                        compare_left_opt_right_opt(left, right, operator)
                    })
                }
                Operand::DestinationVertex(right_dst_pki) => {
                    Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                        let graph = graph_pointer.deref();
                        let edge = graph.get_edge(edge_id);

                        let pv = PropertyValue::get_id(edge.src_vertex_id);
                        let left = graph.get_vertex_id_property_value(
                            edge.src_vertex_id,
                            &pv,
                            left_src_pki,
                        );

                        let pv2 = PropertyValue::get_id(edge.dst_vertex_id);
                        let right = graph.get_vertex_id_property_value(
                            edge.dst_vertex_id,
                            &pv2,
                            right_dst_pki,
                        );

                        compare_left_opt_right_opt(left, right, operator)
                    })
                }
            },
        },
        Operand::DestinationVertex(left_dst_pki) => match operand2 {
            RightOperand::Value(right_value) => {
                Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                    let graph = graph_pointer.deref();
                    let edge = graph.get_edge(edge_id);

                    let pv = PropertyValue::get_id(edge.dst_vertex_id);
                    let left =
                        graph.get_vertex_id_property_value(edge.dst_vertex_id, &pv, left_dst_pki);

                    compare_left_opt_right(left, &right_value, operator)
                })
            }
            RightOperand::Variable(right_variable) => match right_variable {
                Operand::Property(right_edge_pki) => {
                    Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                        let graph = graph_pointer.deref();
                        let edge = graph.get_edge(edge_id);

                        let pv = PropertyValue::get_id(edge.dst_vertex_id);
                        let left = graph.get_vertex_id_property_value(
                            edge.dst_vertex_id,
                            &pv,
                            left_dst_pki,
                        );

                        let pv2 = PropertyValue::get_id(edge_id);
                        let right = graph.get_edge_property_value(edge, &pv2, right_edge_pki);

                        compare_left_opt_right_opt(left, right, operator)
                    })
                }
                Operand::Edge => Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                    let graph = graph_pointer.deref();
                    let edge = graph.get_edge(edge_id);

                    let pv = PropertyValue::get_id(edge.dst_vertex_id);
                    let left =
                        graph.get_vertex_id_property_value(edge.dst_vertex_id, &pv, left_dst_pki);

                    let right_value = get_edge_pair(edge);

                    compare_left_opt_right(left, &right_value, operator)
                }),
                Operand::SourceVertex(right_src_pki) => {
                    Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                        let graph = graph_pointer.deref();
                        let edge = graph.get_edge(edge_id);

                        let pv = PropertyValue::get_id(edge.dst_vertex_id);
                        let left = graph.get_vertex_id_property_value(
                            edge.dst_vertex_id,
                            &pv,
                            left_dst_pki,
                        );

                        let pv2 = PropertyValue::get_id(edge.src_vertex_id);
                        let right = graph.get_vertex_id_property_value(
                            edge.src_vertex_id,
                            &pv2,
                            right_src_pki,
                        );

                        compare_left_opt_right_opt(left, right, operator)
                    })
                }
                Operand::DestinationVertex(right_dst_pki) => {
                    Arc::new(move |edge_id: EdgeId, graph_pointer: GraphPointer| {
                        let graph = graph_pointer.deref();
                        let edge = graph.get_edge(edge_id);

                        let vertex_id = edge.dst_vertex_id;
                        let vertex = graph.get_vertex(vertex_id);
                        let pv = PropertyValue::get_id(vertex_id);
                        let left = graph.get_vertex_property_value(vertex, &pv, left_dst_pki);
                        let right = graph.get_vertex_property_value(vertex, &pv, right_dst_pki);

                        compare_left_opt_right_opt(left, right, operator)
                    })
                }
            },
        },
    };
    PredicateFunction::new(closure)
}

#[inline(always)]
fn compare_left_opt_right_opt(
    left: Option<&PropertyValue>,
    right: Option<&PropertyValue>,
    operator: Operator,
) -> bool {
    left.map_or(false, |left_value| {
        right.map_or(false, |right_value| left_value.compare(&right_value, operator))
    })
}

#[inline(always)]
fn compare_left_right_opt(
    left_value: &PropertyValue,
    right: Option<&PropertyValue>,
    operator: Operator,
) -> bool {
    right.map_or(false, |right_value| left_value.compare(&right_value, operator))
}

#[inline(always)]
fn compare_left_opt_right(
    left: Option<&PropertyValue>,
    right_value: &PropertyValue,
    operator: Operator,
) -> bool {
    left.map_or(false, |left_value| left_value.compare(right_value, operator))
}

#[inline(always)]
fn get_edge_pair(edge: &Edge) -> PropertyValue {
    PropertyValue::Pair(
        isize::try_from(edge.src_vertex_id).expect("Id value overflow"),
        isize::try_from(edge.dst_vertex_id).expect("Id value overflow"),
    )
}
