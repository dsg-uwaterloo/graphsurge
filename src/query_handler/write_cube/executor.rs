use crate::error::GSError;
use crate::filtered_cubes::timestamp::GSTimestamp;
use crate::global_store::GlobalStore;
use crate::query_handler::write_cube::WriteCubeAst;
use crate::query_handler::GraphSurgeQuery;
use crate::util::io::GsWriter;
use crate::GraphSurgeResult;
use crossbeam_utils::thread;
use crossbeam_utils::thread::ScopedJoinHandle;
use log::info;

impl GraphSurgeQuery for WriteCubeAst {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GSError> {
        if let Some(cube) = global_store.filtered_cube_store.cubes.get(&self.name) {
            info!("Writing cube to '{}'...", self.dir);
            thread::scope(|s| {
                let mut threads = Vec::new();
                let data = &cube.data.entries;
                for chunk in data.chunks(std::cmp::max(1, data.len() / self.threads)) {
                    let thread: ScopedJoinHandle<Result<(), GSError>> = s.spawn(move |_| {
                        for (_, timestamp, (full_edges, diff_edges), _) in chunk {
                            info!("Writing {}...", timestamp);
                            let full_edges_path = format!(
                                "{}/fcube-{}-full-{}.txt",
                                self.dir,
                                self.name,
                                timestamp.get_str('_')
                            );
                            let mut writer = GsWriter::new(full_edges_path)?;
                            writer.write_file_lines(
                                full_edges.iter().map(|(src, dst)| format!("{},{}", src, dst)),
                            )?;

                            let diff_edges_path = format!(
                                "{}/fcube-{}-diff-{}.txt",
                                self.dir,
                                self.name,
                                timestamp.get_str('_')
                            );
                            let mut writer = GsWriter::new(diff_edges_path)?;
                            writer.write_file_lines(diff_edges.iter().map(
                                |((src, dst), diff)| {
                                    if self.graphbolt_format && timestamp == &GSTimestamp::new(&[0])
                                    {
                                        format!("{} {}", src, dst)
                                    } else {
                                        format!("{} {} {:+}", src, dst, diff)
                                    }
                                },
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
            Ok(GraphSurgeResult::new(format!("Cube written to '{}'", self.dir)))
        } else {
            Err(GSError::CollectionMissing(self.name.clone()))
        }
    }
}
