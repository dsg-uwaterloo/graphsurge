use crate::create_generic_pointer;
use crate::filtered_cubes::materialise::get_differential_data;
use crate::filtered_cubes::materialise::DifferentialData;
use crate::filtered_cubes::timestamp::timestamp_mappings::TimestampMappings;
use crate::filtered_cubes::timestamp::{DimensionId, GSTimestamp};
use crate::util::timer::GsTimer;
use gs_analytics_api::{DiffCount, EdgeId, FilteredCubeData, SimpleEdge};
use hashbrown::HashMap;
use itertools::Itertools;
use log::info;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;

pub mod materialise;
pub mod serde;
pub mod timestamp;

#[derive(Default, Serialize, Deserialize)]
pub struct FilteredCubeStore {
    pub cubes: HashMap<String, FilteredCube>,
}

impl FilteredCubeStore {
    pub fn delete_all(&mut self) {
        self.cubes.clear();
    }

    pub fn reset(&mut self) {
        self.cubes.clear();
    }
}

impl Display for FilteredCubeStore {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(
            f,
            "{}",
            if self.cubes.is_empty() {
                "No cubes registered".to_owned()
            } else {
                self.cubes
                    .iter()
                    .enumerate()
                    .map(|(index, (name, _))| format!("({}) cube {}", (index + 1), name,))
                    .collect::<Vec<String>>()
                    .join("\n")
            }
        )
    }
}

#[derive(Serialize, Deserialize)]
pub struct FilteredCube {
    pub timestamp_mappings: TimestampMappings,
    pub dimension_lengths: DimensionLengths,
    #[serde(skip)]
    pub differential_data: Option<DifferentialData>,
    pub data: FilteredCubeData<GSTimestamp>,
}
pub type DimensionLengths = Vec<DimensionLength>;
pub type DimensionLength = DimensionId;
pub type CubeDataEntries2<T> = (T, T, Vec<(SimpleEdge, DiffCount)>);
pub type FilteredCubeEntriesEdgeId = (Vec<EdgeId>, Vec<(EdgeId, DiffCount)>);
create_generic_pointer!(CubePointer, FilteredCubeData);

impl FilteredCube {
    pub fn new(
        timestamp_mappings: TimestampMappings,
        dimension_lengths: DimensionLengths,
        differential_data: Option<DifferentialData>,
        data: FilteredCubeData<GSTimestamp>,
    ) -> Self {
        Self { timestamp_mappings, dimension_lengths, differential_data, data }
    }

    pub fn prepare_differential_data(&mut self) {
        if self.differential_data.is_none() {
            info!("Materializing differential data:");
            let timer = GsTimer::now();
            let diff_data = get_differential_data(&self);
            info!("Data materialized in {}", timer.elapsed().seconds_string());
            self.differential_data = Some(diff_data);
        }
    }

    pub fn get_full_data_string(&self) -> String {
        self.data
            .entries
            .iter()
            .map(|(_, timestamp, (full_edges, _), _)| {
                format!(
                    "{}:\n\ttotal: {}, sample: {}",
                    timestamp,
                    full_edges.len(),
                    full_edges.iter().take(5).map(|e| format!("{:?}", e)).collect_vec().join(",")
                )
            })
            .collect_vec()
            .join("\n")
    }
}

impl Display for FilteredCube {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(
            f,
            "Timestamps count: {}\nDimensions:\n TODO(INFO)",
            self.timestamp_mappings.0.len(),
            //            self.dimensions
            //                .iter()
            //                .map(|(k, (t, v))| format!(
            //                    "{} {{\n\ttype '{}';\n\t{};\n}}",
            //                    k,
            //                    t,
            //                    v.iter()
            //                        .map(|(k, v)| format!(
            //                            "{}: {}",
            //                            k,
            //                            v.iter()
            //                                .map(|(ctype, c)| format!(
            //                                    "{}{}{}",
            //                                    if *ctype { "!(" } else { "" },
            //                                    c.iter()
            //                                        .map(|(op1, c, op2)| format!(
            //                                            "{} {} {}",
            //                                            op1.to_string(),
            //                                            c.as_string(),
            //                                            op2.to_string()
            //                                        ))
            //                                        .collect::<Vec<String>>()
            //                                        .join(" and "),
            //                                    if *ctype { ")" } else { "" },
            //                                ))
            //                                .collect::<Vec<String>>()
            //                                .join(" and ")
            //                        ))
            //                        .collect::<Vec<String>>()
            //                        .join(";\n\t"),
            //                ))
            //                .collect::<Vec<String>>()
            //                .join(",\n"),
        )
    }
}
