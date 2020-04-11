//! Graphsurge is an in-memory graph OLAP system. It supports creating aggregated and filtered views
//! of a base graph. It further supports creating N-dimensional cubes representing different views
//! of the base graph. To optimally execute the same graph computation on all cells of a cube,
//! Graphsurge uses *differential dataflow* that can reuse computation results across the cube.

// Enable warnings for all clippy lints. This automatically enables new lints shipped with new rust
// versions.
#![warn(
    clippy::correctness,
    clippy::style,
    clippy::complexity,
    clippy::perf,
    clippy::pedantic,
    clippy::cargo,
    clippy::restriction
)]
// Now selectively disable unneeded lints.
#![allow(
    clippy::indexing_slicing,               // Allow `vec[i]` indexing.
    clippy::module_name_repetitions,        // Allow.
    clippy::use_debug,                      // Allow.
    clippy::float_arithmetic,               // Allow.
    clippy::integer_arithmetic,             // Allow.
    clippy::integer_division,               // Allow.
    clippy::implicit_return,                // Allow.
    clippy::too_many_arguments,             // Allow.
    clippy::use_self,                       // Allow.
    clippy::shadow_same,                    // Allow.
    clippy::too_many_lines,                 // Allow.
    clippy::multiple_crate_versions,        // Disabled.
    clippy::missing_docs_in_private_items,  // Disabled.
    clippy::missing_errors_doc,             // Disabled.
    clippy::missing_inline_in_public_items, // Disabled.
    clippy::unknown_clippy_lints,           // To enable naming new lints added to nightly.
    clippy::cognitive_complexity,           // Disabled.
    clippy::result_expect_used,             // Should use `expect` rather than `unwrap`.
    clippy::option_expect_used,             // Should use `expect` rather than `unwrap`.
    clippy::panic,                          // Allow.
    clippy::unreachable,                    // Allow.
    clippy::todo,                           // Allow.
    clippy::must_use_candidate,             // Allow.
    clippy::inline_always,                  // Allow.
    clippy::as_conversions,                 // Allow but only when absolutely necessary.
    clippy::implicit_hasher                 // Default hasher is fine for now.
)]
// Do not allow print statements. Use `log::info!()` or equivalent instead.
#![deny(clippy::print_stdout)]

pub mod computations;
pub mod error;
pub mod filtered_cubes;
pub mod global_store;
pub mod graph;
mod parser;
pub mod query_handler;
pub mod util;

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate derive_new;

use crate::error::GraphSurgeError;
use crate::global_store::GlobalStore;
use crate::parser::GraphSurgeParser;
use crate::query_handler::GraphSurgeResult;

/// Executes the query given in `query_string` using the `GlobalStore` state.
pub fn process_query(
    global_store: &mut GlobalStore,
    query_string: &mut String,
) -> Result<String, GraphSurgeError> {
    // Parse the query string.
    let parser = GraphSurgeParser::new(&global_store.key_store);
    let query = parser.parse_query(&query_string)?;

    // Execute the parsed query.
    let result = query.execute(global_store)?;

    // Stringify the result.
    Ok(result.result)
}
