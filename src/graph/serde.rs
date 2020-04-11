use crate::error::GraphSurgeError;
use crate::global_store::{deserialize_object, serialize_object};
use crate::graph::Graph;
use crossbeam_utils::thread;
use crossbeam_utils::thread::Scope;
use itertools::Itertools;
use log::info;
use serde::de::DeserializeOwned;
use serde::Serialize;

const SERDE_FILE_VERTICES: &str = "graph_vertices";
const SERDE_FILE_VERTICES_BLOCK_COUNT: &str = "graph_vertices_block_count";
const SERDE_FILE_VERTICES_LEN: &str = "graph_vertices_len";
const SERDE_FILE_EDGES: &str = "graph_edges";
const SERDE_FILE_EDGES_BLOCK_COUNT: &str = "graph_edges_block_count";
const SERDE_FILE_EDGES_LEN: &str = "graph_edges_len";

pub const DEFAULT_SERDE_THREADS: usize = 6;
const MIN_BLOCK_SIZE: usize = 500;

pub fn serialize(
    graph: &Graph,
    bin_dir: &str,
    thread_count: usize,
    block_size: Option<usize>,
) -> Result<(), GraphSurgeError> {
    let res = thread::scope(|s| {
        // Vertices.
        let vertices_thread = s.spawn(|r| {
            serialize_blocks(
                r,
                bin_dir,
                thread_count,
                block_size,
                &graph.vertices,
                "vertices",
                SERDE_FILE_VERTICES,
                SERDE_FILE_VERTICES_BLOCK_COUNT,
                SERDE_FILE_VERTICES_LEN,
                MIN_BLOCK_SIZE,
            )
        });

        // Edges.
        let edges_thread = s.spawn(|r| {
            serialize_blocks(
                r,
                bin_dir,
                thread_count,
                block_size,
                &graph.edges,
                "edges",
                SERDE_FILE_EDGES,
                SERDE_FILE_EDGES_BLOCK_COUNT,
                SERDE_FILE_EDGES_LEN,
                MIN_BLOCK_SIZE,
            )
        });

        vertices_thread.join().expect("Error joining vertices_thread")?;
        edges_thread.join().expect("Error joining edges_thread")?;

        Ok(())
    })
    .expect("Error ending serialization scope");
    info!("Serialized {} vertices and {} edges", graph.vertices.len(), graph.edges.len());
    res
}

pub fn serialize_blocks<'a, T: Serialize + Sync>(
    r: &Scope<'a>,
    bin_dir: &'a str,
    thread_count: usize,
    user_block_size: Option<usize>,
    object: &'a [T],
    object_name: &'a str,
    block_file: &'a str,
    blocks_count_file: &'a str,
    len_file: &'a str,
    min_block_size: usize,
) -> Result<(), GraphSurgeError> {
    // Default block size is calculated based on number of available threads.
    let default_block_size = std::cmp::max(object.len() / thread_count, min_block_size);
    let block_size = user_block_size.unwrap_or(default_block_size);
    let mut blocks_count = object.len() / block_size;
    if object.len() % block_size != 0 {
        blocks_count += 1;
    }
    info!(
        "Serializing {} to {} blocks of size {} using {} threads",
        object_name, blocks_count, block_size, thread_count
    );

    serialize_object(&bin_dir, blocks_count_file, &blocks_count)?;
    serialize_object(&bin_dir, len_file, &object.len())?;

    // Serialize blocks in chunks of available threads.
    // This is a poor man's thread pool implementation.
    // TODO: investigate actual threadpool that supports scopes.
    let mut threads = Vec::new();
    for block_ids in &(0..blocks_count).chunks(thread_count) {
        for block_id in block_ids {
            let file_name = get_block_file_name(block_file, block_id);
            let from_index = block_size * block_id;
            let to_index = std::cmp::min(block_size * (block_id + 1), object.len());
            threads.push(r.spawn(move |_| {
                serialize_object(
                    &bin_dir,
                    &file_name,
                    &(from_index, to_index, &object[from_index..to_index]),
                )
            }));
        }
        for thread in threads.drain(..) {
            thread.join().unwrap_or_else(|_| panic!("Error joining {}_thread", object_name))?;
        }
    }
    Ok(())
}

pub fn deserialize(
    graph: &mut Graph,
    bin_dir: &str,
    thread_count: usize,
) -> Result<(), GraphSurgeError> {
    let res = thread::scope(|s| {
        // Vertices.
        let vertices_thread = s.spawn(|r| {
            deserialize_blocks(
                r,
                bin_dir,
                thread_count,
                "vertices",
                SERDE_FILE_VERTICES,
                SERDE_FILE_VERTICES_BLOCK_COUNT,
                SERDE_FILE_VERTICES_LEN,
            )
        });

        // Edges.
        let edges_thread = s.spawn(|r| {
            deserialize_blocks(
                r,
                bin_dir,
                thread_count,
                "edges",
                SERDE_FILE_EDGES,
                SERDE_FILE_EDGES_BLOCK_COUNT,
                SERDE_FILE_EDGES_LEN,
            )
        });

        graph.vertices = vertices_thread.join().expect("Error joining vertices_thread")?;
        graph.edges = edges_thread.join().expect("Error joining edges_thread")?;

        Ok(())
    })
    .expect("Error ending serialization scope");
    info!("Deserialized {} vertices and {} edges", graph.vertices.len(), graph.edges.len());
    res
}

pub fn deserialize_blocks<'a, T: DeserializeOwned + Sync + Send + 'a>(
    r: &Scope<'a>,
    bin_dir: &'a str,
    thread_count: usize,
    object_name: &'a str,
    block_file: &'a str,
    blocks_count_file: &'a str,
    len_file: &'a str,
) -> Result<Vec<T>, GraphSurgeError> {
    let len = deserialize_object(&bin_dir, len_file)?;
    let blocks_count = deserialize_object(&bin_dir, blocks_count_file)?;
    info!("Deserializing {} using {} threads", object_name, thread_count);
    let mut chunks = Vec::new();
    let mut threads = Vec::new();
    for block_ids in &(0..blocks_count).chunks(thread_count) {
        for block_id in block_ids {
            let file_name = get_block_file_name(block_file, block_id);
            threads.push(r.spawn(move |_| deserialize_object(&bin_dir, &file_name)));
        }
        for thread in threads.drain(..) {
            chunks.push(
                thread.join().unwrap_or_else(|_| panic!("Error joining {}_thread", object_name))?,
            );
        }
    }
    Ok(join_chunks(chunks, len))
}

fn join_chunks<T>(mut chunks: Vec<(usize, usize, Vec<T>)>, len: usize) -> Vec<T> {
    chunks.sort_by_key(|x| x.0);
    assert_eq!(chunks[0].0, 0);
    for i in 0..chunks.len() - 1 {
        assert_eq!(chunks[i].1, chunks[i + 1].0)
    }
    assert_eq!(chunks[chunks.len() - 1].1, len);
    chunks.into_iter().flat_map(|(_, _, chunk)| chunk).collect()
}

fn get_block_file_name(block_file: &str, block_id: usize) -> String {
    format!("{}_block_{}", block_file, block_id)
}
