use crate::error::{create_cube_error, GraphSurgeError};
use crate::filtered_cubes::serde::{deserialize, serialize};
use crate::global_store::GlobalStore;
use crate::query_handler::create_filtered_cube::executor::print_totals;
use crate::query_handler::serde::{Operation, Serde};
use crate::query_handler::GraphSurgeQuery;
use crate::query_handler::GraphSurgeResult;

impl GraphSurgeQuery for Serde {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GraphSurgeError> {
        match &self.operation {
            Operation::SerializeGraph => {
                global_store.serialize(self.bin_dir.as_str(), self.threads, self.block_size)
            }
            Operation::SerializeCollection(name) => {
                if let Some(cube) = global_store.filtered_cube_store.cubes.get(name) {
                    serialize(cube, self.bin_dir.as_str(), name, self.threads, self.block_size)?;
                    Ok(GraphSurgeResult::new("Serialization done.".to_string()))
                } else {
                    Err(create_cube_error(format!("Cube name '{}' does not exist in store", name)))
                }
            }
            Operation::DeserializeGraph => {
                global_store.deserialize(self.bin_dir.as_str(), self.threads)
            }
            Operation::DeserializeCollection => {
                let (name, cube) = deserialize(self.bin_dir.as_str(), self.threads)?;
                if global_store.filtered_cube_store.cubes.contains_key(&name) {
                    return Err(create_cube_error(format!(
                        "Cube name '{}' already exists in store",
                        name
                    )));
                }
                print_totals(&cube);
                global_store.filtered_cube_store.cubes.insert(name, cube);
                Ok(GraphSurgeResult::new("Done".to_string()))
            }
        }
    }
}
