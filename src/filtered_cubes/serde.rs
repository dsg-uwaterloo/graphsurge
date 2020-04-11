use crate::error::GraphSurgeError;
use crate::filtered_cubes::timestamp::timestamp_mappings::get_timestamp_mappings;
use crate::filtered_cubes::{DimensionLengths, FilteredCube, FilteredCubeData};
use crate::global_store::{deserialize_object, serialize_object};
use crate::graph::serde::{deserialize_blocks, serialize_blocks};
use crossbeam_utils::thread;
use log::info;

const SERDE_FILE: &str = "cube";
const SERDE_FILE_CUBE_BLOCK_COUNT: &str = "cube_block_count";
const SERDE_FILE_CUBE_LEN: &str = "cube_len";
const SERDE_FILE_DIMENSION_LENGTHS: &str = "cube_dimension_lengths";
const SERDE_FILE_NAME: &str = "cube_name";
const MIN_BLOCK_SIZE: usize = 1; // Data is allocated per timestamp.

pub fn serialize(
    cube: &FilteredCube,
    bin_dir: &str,
    name: &str,
    thread_count: usize,
    block_size: Option<usize>,
) -> Result<(), GraphSurgeError> {
    let res = thread::scope(|s| {
        let data_thread = s.spawn(|r| {
            serialize_blocks(
                r,
                bin_dir,
                thread_count,
                block_size,
                &cube.data.entries,
                "cube",
                SERDE_FILE,
                SERDE_FILE_CUBE_BLOCK_COUNT,
                SERDE_FILE_CUBE_LEN,
                MIN_BLOCK_SIZE,
            )
        });

        serialize_object(bin_dir, SERDE_FILE_DIMENSION_LENGTHS, &cube.dimension_lengths)?;
        serialize_object(bin_dir, SERDE_FILE_NAME, &name)?;

        data_thread.join().expect("Error joining data_thread")?;

        Ok(())
    })
    .expect("Error ending serialization scope");
    info!("Serialized {} timestamps of cube", cube.data.entries.len());
    res
}

pub fn deserialize(
    bin_dir: &str,
    thread_count: usize,
) -> Result<(String, FilteredCube), GraphSurgeError> {
    let (dimension_lengths, name, data) = thread::scope(|s| {
        let data_thread = s.spawn(|r| {
            deserialize_blocks(
                r,
                bin_dir,
                thread_count,
                "cube",
                SERDE_FILE,
                SERDE_FILE_CUBE_BLOCK_COUNT,
                SERDE_FILE_CUBE_LEN,
            )
        });

        let dimension_lengths: DimensionLengths =
            deserialize_object(bin_dir, SERDE_FILE_DIMENSION_LENGTHS)?;
        let name: String = deserialize_object(bin_dir, SERDE_FILE_NAME)?;

        let data = data_thread.join().expect("Error joining data_thread")?;

        Ok((dimension_lengths, name, data))
    })
    .expect("Error ending serialization scope")?;
    info!("Deserialized {} timestamps of cube", data.len());
    Ok((
        name,
        FilteredCube {
            timestamp_mappings: get_timestamp_mappings(&dimension_lengths),
            dimension_lengths,
            differential_data: None,
            data: FilteredCubeData::new(data),
        },
    ))
}
