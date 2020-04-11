// Enable warnings for all clippy lints.
#![warn(
    clippy::correctness,
    clippy::style,
    clippy::complexity,
    clippy::perf,
    clippy::pedantic,
    clippy::cargo,
    clippy::restriction
)]
// Selectively disable warnings for some lints.
#![allow(
    clippy::indexing_slicing, // Allow `vec[i]` indexing.
    clippy::module_name_repetitions,  // Allow name repetitions in module and type names.
    clippy::use_debug, // Debug formatting is useful.
    clippy::float_arithmetic, // Needed.
    clippy::integer_arithmetic, // Needed.
    clippy::integer_division, // Needed.
    clippy::multiple_crate_versions, // Beyond our control.
    clippy::missing_docs_in_private_items, // Disabled.
    clippy::missing_inline_in_public_items, // Not considered for now.
    clippy::implicit_return, // Allow.
    clippy::too_many_arguments, // Allow.
    clippy::use_self, // Too pedantic.
    clippy::shadow_same,
    clippy::result_expect_used,
    clippy::unknown_clippy_lints,
    clippy::exit
)]
// Mark some lints as errors.
#![deny(clippy::print_stdout)]

use clap::{arg_enum, value_t, App, Arg, ArgMatches};
use graphsurge::error::GraphSurgeError;
use graphsurge::global_store::GlobalStore;
use graphsurge::util::io::get_file_lines;
use graphsurge::util::logger::init_logger_with_level;
use graphsurge::util::timer::GSTimer;
use log::{info, Level};
use rustyline::error::ReadlineError;
use rustyline::Config;
use rustyline::Editor;

struct QueryState {
    prompt: &'static str,
    continued_query: bool,
}

impl QueryState {
    pub fn new() -> Self {
        Self { prompt: MAIN_PROMPT, continued_query: false }
    }

    pub fn set_continued(&mut self) {
        self.prompt = CONTINUATION_PROMPT;
        self.continued_query = true;
    }

    pub fn set_main(&mut self) {
        self.prompt = MAIN_PROMPT;
        self.continued_query = false;
    }

    pub fn is_continued(&self) -> bool {
        self.continued_query
    }

    pub fn get_prompt(&self) -> &str {
        self.prompt
    }
}

const HISTORY_FILE: &str = "/tmp/graphsurge_cli_history.txt";
const MAIN_PROMPT: &str = "graphsurge> ";
const CONTINUATION_PROMPT: &str = "... ";

arg_enum! {
    #[derive(PartialEq, Debug)]
    pub enum LogLevel {
        Error,
        Warn,
        Info,
        Debug,
        Trace,
    }
}

fn main() -> Result<(), GraphSurgeError> {
    // Parse command line arguments.
    let matches = App::new("graphsurge")
        .arg(
            Arg::from_usage("-l, --loglevel=[LEVEL] 'Set the log level'")
                .possible_values(&LogLevel::variants())
                .case_insensitive(true),
        )
        .args_from_usage("[query_file] 'Reads queries from a file'")
        .get_matches();

    setup_logger(&matches)?;

    // Reads queries from either an input file or stdin.
    let mut file_queries = if let Some(file_path) = matches.value_of("query_file") {
        Some(get_file_lines(file_path)?)
    } else {
        None
    };

    let mut global_store = GlobalStore::default();

    let mut query_string = String::new();
    let mut query_state = QueryState::new();

    let config = Config::builder().history_ignore_dups(true).max_history_size(5000).build();
    let mut rl = Editor::<()>::with_config(config);
    rl.load_history(HISTORY_FILE).unwrap_or_default();

    'cli_loop: loop {
        let line_result = if let Some(ref mut lines) = file_queries {
            lines.next().clone().ok_or(ReadlineError::Eof)
        } else {
            rl.readline(query_state.get_prompt())
        };
        match line_result {
            Ok(query) => {
                if query.is_empty() {
                    continue 'cli_loop;
                }

                if query_state.is_continued() {
                    query_string.push(' ');
                } else {
                    query_string.clear();
                };
                query_string.push_str(query.trim());

                let last_char = &query_string[query_string.len() - 1..];
                if last_char == ";" {
                    query_state.set_main();
                } else {
                    query_state.set_continued();
                    continue 'cli_loop;
                }

                rl.add_history_entry(&query_string.replace("\n", " "));
                rl.save_history(HISTORY_FILE).unwrap_or_default();

                if file_queries.is_some() {
                    info!("[Query] {}", query_string);
                }

                let timer = GSTimer::now();
                let result = graphsurge::process_query(&mut global_store, &mut query_string);
                match result {
                    Ok(query_result) => {
                        info!(
                            "[Success][{}] {}",
                            timer.elapsed().to_seconds_string(),
                            query_result
                        );
                    }
                    Err(e) => {
                        info!("[Error][{}] {}", timer.elapsed().to_seconds_string(), e.to_string());
                        if file_queries.is_some() {
                            break 'cli_loop;
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Reset.
                query_state.set_main();
            }
            Err(ReadlineError::Eof) => {
                if query_state.is_continued() {
                    info!("Was expecting more input but found EOF, exiting.");
                }
                break;
            }
            Err(err) => {
                info!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}

fn setup_logger(matches: &ArgMatches) -> Result<(), GraphSurgeError> {
    // Set log level.
    let log_level = match value_t!(matches, "loglevel", LogLevel).unwrap_or(LogLevel::Info) {
        LogLevel::Error => Level::Error,
        LogLevel::Warn => Level::Warn,
        LogLevel::Info => Level::Info,
        LogLevel::Debug => Level::Debug,
        LogLevel::Trace => Level::Trace,
    };
    init_logger_with_level(log_level)
}
