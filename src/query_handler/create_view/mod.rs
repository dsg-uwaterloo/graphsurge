use crate::graph::properties::operations::{LeftOperand, Operator, RightOperand};
use crate::graph::properties::PropertyKeyId;
use crate::graph::stream_data::aggregation::AggregationOperation;
use crate::graph::GraphPointer;
use crate::graph::VertexOrEdgeId;
use hashbrown::HashMap;
use serde::export::fmt::Debug;
use std::num::NonZeroU8;
use std::sync::Arc;

pub mod executor;

#[derive(Default, Debug, Clone, new)]
pub struct CreateViewAst {
    pub name: String,
    pub vertex_sections: VertexSections,
    pub edge_sections: EdgeSections,
    pub label_map: (LabelMap, LabelMap),
    pub save_to: Option<String>,
}
pub type VertexSections = HashMap<SectionId, SectionDetails>;
pub type EdgeSections = HashMap<SectionId, (SrcDstVertexSections, SectionDetails)>;
pub type SrcDstVertexSections = Option<(SectionId, SectionId)>;
pub type SectionId = NonZeroU8;
pub type LabelMap = HashMap<SectionId, String>;
#[derive(Default, Debug, Clone)]
pub struct SectionDetails {
    pub where_conditions: WhereConditions,
    pub group_clauses: Vec<GroupClause>,
    pub aggregate_clauses: Vec<AggregateClause>,
}
pub type WhereConditions = Vec<WhereCondition>;
pub type WhereCondition = (IsNegation, Vec<WherePredicate>);
pub type WherePredicate = ((LeftOperand, Operator, RightOperand), PredicateFunction);
#[derive(Clone, new)]
pub struct PredicateFunction(pub PredicateFunctionClosure);
pub type PredicateFunctionClosure = Arc<dyn Fn(VertexOrEdgeId, GraphPointer) -> bool + Send + Sync>;
impl Debug for PredicateFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "[closure]",)
    }
}
pub type IsNegation = bool;
pub type GroupClause = Vec<GroupCondition>;
#[derive(Debug, Clone)]
pub enum GroupCondition {
    Variable(PropertyKeyId),
    WherePredicate(WherePredicate),
    List(Vec<WhereConditions>),
}
#[derive(Debug, Clone)]
pub enum FlattenedGroupCondition {
    Variable(PropertyKeyId),
    WhereConditions(WhereConditions),
}
pub type AggregateClause = (PropertyName, AggregationOperation, PropertyKeyId);
pub type PropertyName = String;

impl std::fmt::Display for CreateViewAst {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "create view {} with{}{}",
            self.name,
            fmt_vertex_sections(&self.vertex_sections, &self.label_map),
            fmt_edge_sections(&self.edge_sections, &self.label_map),
        )
    }
}

pub fn fmt_vertex_sections(
    vertex_sections: &VertexSections,
    label_map: &(LabelMap, LabelMap),
) -> String {
    vertex_sections
        .iter()
        .map(|(label, v)| format!("\nvertices {}{}", fmt_label(*label, &label_map.0), v))
        .collect::<Vec<_>>()
        .join("\t")
}

pub fn fmt_edge_sections(edge_sections: &EdgeSections, label_map: &(LabelMap, LabelMap)) -> String {
    edge_sections
        .iter()
        .map(|(label, (b, v))| {
            format!(
                "\nedges {}{}{}",
                fmt_label(*label, &label_map.1),
                if let Some((src, dst)) = b {
                    format!(
                        " between ({},{})",
                        label_map.0.get(src).expect("Missing label"),
                        label_map.0.get(dst).expect("Missing label"),
                    )
                } else {
                    String::new()
                },
                v
            )
        })
        .collect::<Vec<_>>()
        .join("\t")
}

fn fmt_label(section_id: SectionId, label_map: &LabelMap) -> String {
    if let Some(label) = label_map.get(&section_id) {
        format!("as {}", label)
    } else {
        "".to_string()
    }
}

impl std::fmt::Display for SectionDetails {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        if !self.where_conditions.is_empty() {
            write!(f, "\n\twhere {}", fmt_condition_clauses(&self.where_conditions))?;
        }
        if !self.group_clauses.is_empty() {
            write!(
                f,
                "\n\tgroup by [\n\t\t{}\n\t]",
                self.group_clauses
                    .iter()
                    .map(|gcl| {
                        format!(
                            "({})",
                            gcl.iter().map(|gc| { gc.to_string() }).collect::<Vec<_>>().join(", ")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n\t\t")
            )?;
        }
        if !self.aggregate_clauses.is_empty() {
            write!(
                f,
                "\n\taggregate {{{}}}",
                self.aggregate_clauses
                    .iter()
                    .map(|(p, f, a)| format!("{}: {:?}({})", p, f, a))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
        }
        Ok(())
    }
}

impl std::fmt::Display for GroupCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            GroupCondition::Variable(v) => write!(f, "{}", v),
            GroupCondition::WherePredicate(wp) => write!(f, "{}", fmt_where_clause(&wp)),
            GroupCondition::List(l) => write!(
                f,
                "[{}]",
                l.iter().map(|cc| { fmt_condition_clauses(cc) }).collect::<Vec<_>>().join(", ")
            ),
        }
    }
}

pub fn fmt_condition_clauses(cc: &[WhereCondition]) -> String {
    cc.iter().map(fmt_condition_clause).collect::<Vec<_>>().join(" and ")
}

pub fn fmt_condition_clause((ctype, c): &WhereCondition) -> String {
    format!(
        "{}{}{}",
        if *ctype { "!(" } else { "" },
        c.iter().map(fmt_where_clause).collect::<Vec<String>>().join(" and "),
        if *ctype { ")" } else { "" },
    )
}

pub fn fmt_where_clause(((op1, c, op2), _): &WherePredicate) -> String {
    format!("{}{}{}", op1.to_string(), c.as_string(), op2.to_string())
}
