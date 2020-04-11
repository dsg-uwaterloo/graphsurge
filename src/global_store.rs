use crate::computations::builder::{initialize_computations, ComputationBuilder};
use crate::error::{serde_error, GraphSurgeError};
use crate::filtered_cubes::FilteredCubeStore;
use crate::graph::key_store::KeyStore;
use crate::graph::Graph;
use crate::util::io::{get_buf_reader, GSWriter};
use crate::util::timer::GSTimer;
use crate::GraphSurgeResult;
use hashbrown::HashMap;
use log::info;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::num::NonZeroUsize;
use std::path::Path;

const SERDE_FILE_KEY_STORE: &str = "key_store";
const SERDE_FILE_FILTERED_CUBES: &str = "filtered_cubes";
const SERDE_FILE_EXTENSION: &str = "bin";

pub struct GlobalStore {
    pub graph: Graph,
    pub key_store: KeyStore,
    pub filtered_cube_store: FilteredCubeStore,
    pub computations: HashMap<String, Box<dyn ComputationBuilder>>,
    pub threads: NonZeroUsize,
    pub process_id: usize,
}

impl Default for GlobalStore {
    fn default() -> Self {
        let mut global_store = GlobalStore {
            graph: Graph::default(),
            key_store: KeyStore::default(),
            filtered_cube_store: FilteredCubeStore::default(),
            computations: HashMap::new(),
            threads: NonZeroUsize::new(1).expect("Unreachable"),
            process_id: 0,
        };
        global_store.reset(); // Reuse reset logic.
        global_store
    }
}

impl GlobalStore {
    fn reset(&mut self) {
        self.graph.reset();
        self.key_store.reset();
        self.filtered_cube_store.reset();
        self.computations.clear();
        self.threads = NonZeroUsize::new(1).expect("Unreachable");
        initialize_computations(&mut self.computations);
    }

    pub fn serialize(
        &self,
        bin_dir: &str,
        thread_count: usize,
        block_size: Option<usize>,
    ) -> Result<GraphSurgeResult, GraphSurgeError> {
        if !Path::new(bin_dir).is_dir() {
            return Err(serde_error(format!("Invalid binary directory: '{}'", bin_dir)));
        }
        self.graph.serialize(bin_dir, thread_count, block_size)?;
        serialize_object(bin_dir, SERDE_FILE_KEY_STORE, &self.key_store)?;
        serialize_object(bin_dir, SERDE_FILE_FILTERED_CUBES, &self.filtered_cube_store)?;
        Ok(GraphSurgeResult::new("Serialization done.".to_string()))
    }

    pub fn deserialize(
        &mut self,
        bin_dir: &str,
        thread_count: usize,
    ) -> Result<GraphSurgeResult, GraphSurgeError> {
        if !Path::new(bin_dir).is_dir() {
            return Err(serde_error(format!("Invalid binary directory: '{}'", bin_dir)));
        }
        self.reset();
        let ret = self.deserialize_inner(bin_dir, thread_count);
        if ret.is_err() {
            self.reset();
        }
        ret
    }

    fn deserialize_inner(
        &mut self,
        bin_dir: &str,
        thread_count: usize,
    ) -> Result<GraphSurgeResult, GraphSurgeError> {
        self.graph.deserialize(bin_dir, thread_count)?;
        self.key_store = deserialize_object(bin_dir, SERDE_FILE_KEY_STORE)?;
        self.filtered_cube_store = deserialize_object(bin_dir, SERDE_FILE_FILTERED_CUBES)?;
        Ok(GraphSurgeResult::new("Deserialization done.".to_string()))
    }
}

pub fn serialize_object<T: Serialize>(
    bin_dir: &str,
    name: &str,
    object: &T,
) -> Result<(), GraphSurgeError> {
    let output_file_path = get_file_path(bin_dir, name);
    info!("Serializing to '{}'", output_file_path);
    let timer = GSTimer::now();
    let writer = GSWriter::new(output_file_path)?;
    bincode::serialize_into(writer.into_buf_writer(), object)
        .map_err(|e| serde_error(format!("Could not serialize '{}': {}", name, e)))?;
    info!("Serialized '{}' in {}", name, timer.elapsed().to_seconds_string());
    Ok(())
}

pub fn deserialize_object<T: DeserializeOwned>(
    bin_dir: &str,
    name: &str,
) -> Result<T, GraphSurgeError> {
    let input_file_path = get_file_path(bin_dir, name);
    info!("Deserializing '{}'", input_file_path);
    let timer = GSTimer::now();
    let reader = get_buf_reader(&input_file_path)?;
    let object = bincode::deserialize_from(reader)
        .map_err(|e| serde_error(format!("Could not deserialize '{}': {}", name, e)))?;
    info!("Deserialized '{}' in {}", name, timer.elapsed().to_seconds_string());
    Ok(object)
}

fn get_file_path(bin_dir: &str, name: &str) -> String {
    format!("{}/{}.{}", bin_dir, name, SERDE_FILE_EXTENSION)
}
