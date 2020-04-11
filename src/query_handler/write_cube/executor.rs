use crate::error::{create_cube_error, GraphSurgeError};
use crate::global_store::GlobalStore;
use crate::query_handler::write_cube::WriteCubeAst;
use crate::query_handler::GraphSurgeQuery;
use crate::util::io::GSWriter;
use crate::GraphSurgeResult;
use crossbeam_utils::thread;
use crossbeam_utils::thread::ScopedJoinHandle;
use log::info;

impl GraphSurgeQuery for WriteCubeAst {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GraphSurgeError> {
        if let Some(cube) = global_store.filtered_cube_store.cubes.get(&self.name) {
            info!("Writing cube to '{}'...", self.dir);
            let (initial, rest) = &cube.data.entries.split_at(1);
            if self.graphbolt_format {
                for (data, fname) in &[(initial, "initial"), (rest, "rest")] {
                    let file = format!("{}/{}.txt", self.dir, fname);
                    let mut writer = GSWriter::new(file)?;
                    writer.write_file_lines(
                        data.iter().flat_map(|(_, _, (_, diff_data), _)| diff_data).map(
                            |((src, dst), diff)| {
                                format!(
                                    "{}{} {}",
                                    if *fname == "initial" {
                                        ""
                                    } else if *diff == 1 {
                                        "a "
                                    } else if *diff == -1 {
                                        "d "
                                    } else {
                                        unreachable!(
                                            "Cannot write > |1| diffs: {},{},{}",
                                            src, dst, diff
                                        )
                                    },
                                    src,
                                    dst
                                )
                            },
                        ),
                    )?;
                }
            } else {
                thread::scope(|s| {
                    let mut threads = Vec::new();
                    let data = &cube.data.entries;
                    for chunk in data.chunks(std::cmp::max(1, data.len() / self.threads)) {
                        let thread: ScopedJoinHandle<Result<(), GraphSurgeError>> =
                            s.spawn(move |_| {
                                for (_, timestamp, (full_edges, diff_edges), _) in chunk {
                                    info!("Writing {}...", timestamp);
                                    let full_edges_path = format!(
                                        "{}/fcube-{}-full-{}.txt",
                                        self.dir,
                                        self.name,
                                        timestamp.get_str('_')
                                    );
                                    let mut writer = GSWriter::new(full_edges_path)?;
                                    writer.write_file_lines(
                                        full_edges
                                            .iter()
                                            .map(|(src, dst)| format!("{},{}", src, dst)),
                                    )?;

                                    let diff_edges_path = format!(
                                        "{}/fcube-{}-diff-{}.txt",
                                        self.dir,
                                        self.name,
                                        timestamp.get_str('_')
                                    );
                                    let mut writer = GSWriter::new(diff_edges_path)?;
                                    writer.write_file_lines(diff_edges.iter().map(
                                        |((src, dst), diff)| format!("{},{},{:+}", src, dst, diff),
                                    ))?;
                                }
                                Ok(())
                            });
                        threads.push(thread);
                    }
                    for thread in threads.drain(..) {
                        thread.join().unwrap_or_else(|_| panic!("Error joining thread"))?;
                    }
                    Ok(())
                })
                .expect("Error mapping results")?;
            }
            Ok(GraphSurgeResult::new(format!("Cube written to '{}'", self.dir)))
        } else {
            Err(create_cube_error(format!("Cube name '{}' does not exist in store", self.name)))
        }
    }
}
