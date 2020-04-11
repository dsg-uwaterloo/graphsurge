use crate::error::{gs_error, GraphSurgeError};
use crate::global_store::GlobalStore;
use crate::query_handler::set_threads::SetThreads;
use crate::query_handler::GraphSurgeQuery;
use crate::query_handler::GraphSurgeResult;
use std::num::NonZeroUsize;
use std::string::ToString;

impl GraphSurgeQuery for SetThreads {
    fn execute(&self, global_store: &mut GlobalStore) -> Result<GraphSurgeResult, GraphSurgeError> {
        global_store.threads = NonZeroUsize::new(self.0)
            .ok_or_else(|| gs_error("Threads should be non-zero".to_string()))?;
        global_store.process_id = self.1;
        Ok(GraphSurgeResult::new("Done".to_string()))
    }
}
