use crate::computations::bfs::Bfs;
use crate::computations::dataflow_arranged_adaptive::differential_run_adaptive;
use crate::computations::dataflow_differential_2_stages::differential_run_2_stage;
use crate::computations::dataflow_differential_basic::differential_run_basic;
use crate::computations::scc::Scc;
use crate::computations::wcc::Wcc;
use crate::computations::{
    differential_diff_execute, process_results, Computation, DifferentialResults, SplitIndices,
};
use crate::filtered_cubes::materialise::DiffIteratorPointer;
use crate::filtered_cubes::timestamp::GSTimestamp;
use crate::filtered_cubes::CubePointer;
use crate::global_store::GlobalStore;
use crate::process_query;
use gs_analytics_api::{
    ComputationRuntimeData, ComputationType, ComputationTypes, DiffCount, MaterializeResults,
};
use hashbrown::{HashMap, HashSet};

#[cfg(feature = "nd-timestamps")]
#[test]
fn test_bfs_2d() {
    let expected_diff_results = vec![
        (
            GSTimestamp::new(&[0]),
            vec![
                ((4, 1), 1),
                ((3, 3), 1),
                ((9, 3), 1),
                ((8, 4), 1),
                ((6, 0), 1),
                ((2, 2), 1),
                ((5, 2), 1),
            ],
        ),
        (
            GSTimestamp::new(&[0, 1]),
            vec![((5, 2), -1), ((9, 3), -1), ((5, 1), 1), ((9, 4), 1), ((8, 5), 1), ((8, 4), -1)],
        ),
        (GSTimestamp::new(&[1, 0]), vec![((5, 2), -1)]),
        (
            GSTimestamp::new(&[1, 1]),
            vec![
                ((8, 4), 1),
                ((8, 5), -1),
                ((3, 3), -1),
                ((3, 4), 1),
                ((9, 4), -1),
                ((9, 3), 1),
                ((5, 2), 1),
            ],
        ),
    ];

    let expected_full_results = vec![
        (
            GSTimestamp::new(&[0]),
            vec![
                ((6, 0), 1),
                ((4, 1), 1),
                ((2, 2), 1),
                ((5, 2), 1),
                ((3, 3), 1),
                ((9, 3), 1),
                ((8, 4), 1),
            ],
        ),
        (
            GSTimestamp::new(&[0, 1]),
            vec![
                ((6, 0), 1),
                ((4, 1), 1),
                ((5, 1), 1),
                ((2, 2), 1),
                ((3, 3), 1),
                ((9, 4), 1),
                ((8, 5), 1),
            ],
        ),
        (
            GSTimestamp::new(&[1, 0]),
            vec![((6, 0), 1), ((4, 1), 1), ((2, 2), 1), ((3, 3), 1), ((9, 3), 1), ((8, 4), 1)],
        ),
        (
            GSTimestamp::new(&[1, 1]),
            vec![
                ((6, 0), 1),
                ((4, 1), 1),
                ((5, 1), 1),
                ((2, 2), 1),
                ((9, 3), 1),
                ((3, 4), 1),
                ((8, 4), 1),
            ],
        ),
    ];

    let computation = Bfs::new(6);
    create_cube_and_assert(
        &computation,
        2,
        2,
        "data/test_data/bfs/2d_small",
        &expected_diff_results,
        &expected_full_results,
        10000,
    );
}

#[test]
fn test_bfs_1d() {
    let expected_diff_results = vec![
        (
            GSTimestamp::new(&[0]),
            vec![
                ((4, 1), 1),
                ((3, 3), 1),
                ((9, 3), 1),
                ((8, 4), 1),
                ((6, 0), 1),
                ((2, 2), 1),
                ((5, 2), 1),
            ],
        ),
        (GSTimestamp::new(&[1]), vec![((5, 2), -1)]),
        (GSTimestamp::new(&[2]), vec![((5, 1), 1), ((3, 4), 1), ((3, 3), -1)]),
        (
            GSTimestamp::new(&[3]),
            vec![((8, 4), -1), ((9, 4), 1), ((8, 5), 1), ((9, 3), -1), ((3, 3), 1), ((3, 4), -1)],
        ),
    ];

    let expected_full_results = vec![
        (
            GSTimestamp::new(&[0]),
            vec![
                ((6, 0), 1),
                ((4, 1), 1),
                ((2, 2), 1),
                ((5, 2), 1),
                ((3, 3), 1),
                ((9, 3), 1),
                ((8, 4), 1),
            ],
        ),
        (
            GSTimestamp::new(&[3]),
            vec![
                ((6, 0), 1),
                ((4, 1), 1),
                ((5, 1), 1),
                ((2, 2), 1),
                ((3, 3), 1),
                ((9, 4), 1),
                ((8, 5), 1),
            ],
        ),
        (
            GSTimestamp::new(&[1]),
            vec![((6, 0), 1), ((4, 1), 1), ((2, 2), 1), ((3, 3), 1), ((9, 3), 1), ((8, 4), 1)],
        ),
        (
            GSTimestamp::new(&[2]),
            vec![
                ((6, 0), 1),
                ((4, 1), 1),
                ((5, 1), 1),
                ((2, 2), 1),
                ((9, 3), 1),
                ((3, 4), 1),
                ((8, 4), 1),
            ],
        ),
    ];

    let computation = Bfs::new(6);
    create_cube_and_assert(
        &computation,
        1,
        4,
        "data/test_data/bfs/1d_small",
        &expected_diff_results,
        &expected_full_results,
        10010,
    );
}

#[cfg(feature = "nd-timestamps")]
#[test]
fn test_wcc_2d() {
    let expected_diff_results = vec![
        (
            GSTimestamp::new(&[0, 0]),
            vec![
                ((4, 2), 1),
                ((3, 2), 1),
                ((9, 2), 1),
                ((8, 2), 1),
                ((6, 2), 1),
                ((2, 2), 1),
                ((5, 2), 1),
            ],
        ),
        (
            GSTimestamp::new(&[1, 0]),
            vec![((9, 2), -1), ((9, 3), 1), ((8, 2), -1), ((8, 3), 1), ((3, 2), -1), ((3, 3), 1)],
        ),
        (GSTimestamp::new(&[0, 1]), vec![((6, 2), -1), ((6, 4), 1), ((4, 2), -1), ((4, 4), 1)]),
        (GSTimestamp::new(&[1, 1]), vec![]),
    ];

    let expected_full_results = vec![
        (
            GSTimestamp::new(&[0]),
            vec![
                ((4, 2), 1),
                ((3, 2), 1),
                ((9, 2), 1),
                ((8, 2), 1),
                ((6, 2), 1),
                ((2, 2), 1),
                ((5, 2), 1),
            ],
        ),
        (
            GSTimestamp::new(&[1, 0]),
            vec![
                ((4, 2), 1),
                ((3, 3), 1),
                ((9, 3), 1),
                ((8, 3), 1),
                ((6, 2), 1),
                ((2, 2), 1),
                ((5, 2), 1),
            ],
        ),
        (
            GSTimestamp::new(&[0, 1]),
            vec![
                ((4, 4), 1),
                ((3, 2), 1),
                ((9, 2), 1),
                ((8, 2), 1),
                ((6, 4), 1),
                ((2, 2), 1),
                ((5, 2), 1),
            ],
        ),
        (
            GSTimestamp::new(&[1, 1]),
            vec![
                ((4, 4), 1),
                ((3, 3), 1),
                ((9, 3), 1),
                ((8, 3), 1),
                ((6, 4), 1),
                ((2, 2), 1),
                ((5, 2), 1),
            ],
        ),
    ];

    let computation = Wcc {};
    create_cube_and_assert(
        &computation,
        2,
        2,
        "data/test_data/wcc/2d_small",
        &expected_diff_results,
        &expected_full_results,
        10020,
    );

    let computation = Wcc {};
    create_cube_and_assert(
        &computation,
        2,
        2,
        "data/test_data/wcc/2d_small",
        &expected_diff_results,
        &expected_full_results,
        10030,
    );
}

#[test]
fn test_wcc_1d() {
    let expected_diff_results = vec![
        (
            GSTimestamp::new(&[0]),
            vec![
                ((4, 2), 1),
                ((3, 2), 1),
                ((9, 2), 1),
                ((8, 2), 1),
                ((6, 2), 1),
                ((2, 2), 1),
                ((5, 2), 1),
            ],
        ),
        (
            GSTimestamp::new(&[1]),
            vec![((9, 2), -1), ((9, 3), 1), ((8, 2), -1), ((8, 3), 1), ((3, 2), -1), ((3, 3), 1)],
        ),
        (GSTimestamp::new(&[2]), vec![((6, 4), 1), ((4, 2), -1), ((4, 4), 1), ((6, 2), -1)]),
        (
            GSTimestamp::new(&[3]),
            vec![((9, 3), -1), ((3, 2), 1), ((8, 2), 1), ((9, 2), 1), ((8, 3), -1), ((3, 3), -1)],
        ),
    ];

    let expected_full_results = vec![
        (
            GSTimestamp::new(&[0]),
            vec![
                ((4, 2), 1),
                ((3, 2), 1),
                ((9, 2), 1),
                ((8, 2), 1),
                ((6, 2), 1),
                ((2, 2), 1),
                ((5, 2), 1),
            ],
        ),
        (
            GSTimestamp::new(&[1]),
            vec![
                ((4, 2), 1),
                ((3, 3), 1),
                ((9, 3), 1),
                ((8, 3), 1),
                ((6, 2), 1),
                ((2, 2), 1),
                ((5, 2), 1),
            ],
        ),
        (
            GSTimestamp::new(&[3]),
            vec![
                ((4, 4), 1),
                ((3, 2), 1),
                ((9, 2), 1),
                ((8, 2), 1),
                ((6, 4), 1),
                ((2, 2), 1),
                ((5, 2), 1),
            ],
        ),
        (
            GSTimestamp::new(&[2]),
            vec![
                ((4, 4), 1),
                ((3, 3), 1),
                ((9, 3), 1),
                ((8, 3), 1),
                ((6, 4), 1),
                ((2, 2), 1),
                ((5, 2), 1),
            ],
        ),
    ];

    let computation = Wcc {};
    create_cube_and_assert(
        &computation,
        1,
        4,
        "data/test_data/wcc/1d_small",
        &expected_diff_results,
        &expected_full_results,
        10040,
    );

    let computation = Wcc {};
    create_cube_and_assert(
        &computation,
        1,
        4,
        "data/test_data/wcc/1d_small",
        &expected_diff_results,
        &expected_full_results,
        10050,
    );
}

#[test]
fn test_scc_1() {
    let expected_results = vec![(
        GSTimestamp::new(&[0]),
        vec![
            ((2, 5), 1),
            ((7, 6), 1),
            ((4, 3), 1),
            ((1, 2), 1),
            ((5, 1), 1),
            ((4, 8), 1),
            ((8, 4), 1),
            ((3, 4), 1),
            ((6, 7), 1),
        ],
    )];

    let computation = Scc {};
    create_cube_and_assert(
        &computation,
        1,
        1,
        "data/test_data/scc/1",
        &expected_results,
        &expected_results,
        10060,
    );
}

#[test]
fn test_scc_2() {
    let expected_results = vec![(
        GSTimestamp::new(&[0]),
        vec![
            ((4, 5), 1),
            ((4, 6), 1),
            ((3, 0), 1),
            ((5, 4), 1),
            ((8, 9), 1),
            ((2, 3), 1),
            ((0, 2), 1),
            ((6, 5), 1),
            ((1, 3), 1),
            ((9, 8), 1),
            ((0, 1), 1),
        ],
    )];

    let computation = Scc {};
    create_cube_and_assert(
        &computation,
        1,
        1,
        "data/test_data/scc/2",
        &expected_results,
        &expected_results,
        10070,
    );
}

#[test]
fn test_scc_3() {
    let expected_results = vec![(
        GSTimestamp::new(&[0]),
        vec![
            ((4, 3), 1),
            ((6, 8), 1),
            ((2, 4), 1),
            ((1, 0), 1),
            ((2, 1), 1),
            ((5, 6), 1),
            ((14, 12), 1),
            ((9, 10), 1),
            ((6, 7), 1),
            ((3, 2), 1),
            ((0, 2), 1),
            ((7, 8), 1),
            ((8, 5), 1),
            ((13, 14), 1),
            ((10, 9), 1),
            ((12, 13), 1),
        ],
    )];

    let computation = Scc {};
    create_cube_and_assert(
        &computation,
        1,
        1,
        "data/test_data/scc/3",
        &expected_results,
        &expected_results,
        10080,
    );
}

#[test]
fn test_scc_adaptive() {
    let computation = Scc {};

    for (index, (splits, expected_results, expect_splits)) in vec![
        (
            Some(HashSet::new()),
            vec![
                (
                    GSTimestamp::new(&[0]),
                    vec![
                        ((4, 5), 1),
                        ((4, 6), 1),
                        ((3, 0), 1),
                        ((5, 4), 1),
                        ((8, 9), 1),
                        ((2, 3), 1),
                        ((0, 2), 1),
                        ((6, 5), 1),
                        ((1, 3), 1),
                        ((9, 8), 1),
                        ((0, 1), 1),
                    ],
                ),
                (
                    GSTimestamp::new(&[1]),
                    vec![
                        ((1, 3), -1),
                        ((6, 5), -1),
                        ((2, 5), 1),
                        ((0, 2), -1),
                        ((7, 6), 1),
                        ((6, 7), 1),
                        ((4, 5), -1),
                        ((4, 8), 1),
                        ((2, 3), -1),
                        ((4, 3), 1),
                        ((3, 0), -1),
                        ((8, 4), 1),
                        ((5, 4), -1),
                        ((8, 9), -1),
                        ((0, 1), -1),
                        ((5, 1), 1),
                        ((4, 6), -1),
                        ((1, 2), 1),
                        ((3, 4), 1),
                        ((9, 8), -1),
                    ],
                ),
                (GSTimestamp::new(&[2]), vec![((7, 6), -1), ((6, 7), -1)]),
                (
                    GSTimestamp::new(&[3]),
                    vec![
                        ((8, 9), 1),
                        ((4, 6), 1),
                        ((2, 5), -1),
                        ((4, 8), -1),
                        ((4, 5), 1),
                        ((4, 3), -1),
                        ((5, 4), 1),
                        ((3, 0), 1),
                        ((8, 4), -1),
                        ((5, 1), -1),
                        ((0, 2), 1),
                        ((9, 8), 1),
                        ((1, 3), 1),
                        ((0, 1), 1),
                        ((6, 5), 1),
                        ((1, 2), -1),
                        ((3, 4), -1),
                        ((2, 3), 1),
                    ],
                ),
            ],
            HashSet::new(),
        ),
        (
            Some(vec![2, 3].into_iter().collect()),
            vec![
                (
                    GSTimestamp::new(&[0]),
                    vec![
                        ((4, 5), 1),
                        ((4, 6), 1),
                        ((3, 0), 1),
                        ((5, 4), 1),
                        ((8, 9), 1),
                        ((2, 3), 1),
                        ((0, 2), 1),
                        ((6, 5), 1),
                        ((1, 3), 1),
                        ((9, 8), 1),
                        ((0, 1), 1),
                    ],
                ),
                (
                    GSTimestamp::new(&[1]),
                    vec![
                        ((1, 3), -1),
                        ((6, 5), -1),
                        ((2, 5), 1),
                        ((0, 2), -1),
                        ((7, 6), 1),
                        ((6, 7), 1),
                        ((4, 5), -1),
                        ((4, 8), 1),
                        ((2, 3), -1),
                        ((4, 3), 1),
                        ((3, 0), -1),
                        ((8, 4), 1),
                        ((5, 4), -1),
                        ((8, 9), -1),
                        ((0, 1), -1),
                        ((5, 1), 1),
                        ((4, 6), -1),
                        ((1, 2), 1),
                        ((3, 4), 1),
                        ((9, 8), -1),
                    ],
                ),
                (
                    GSTimestamp::new(&[2]),
                    vec![
                        ((1, 2), 1),
                        ((2, 5), 1),
                        ((3, 4), 1),
                        ((4, 3), 1),
                        ((8, 4), 1),
                        ((4, 8), 1),
                        ((5, 1), 1),
                    ],
                ),
                (
                    GSTimestamp::new(&[3]),
                    vec![
                        ((5, 4), 1),
                        ((0, 2), 1),
                        ((4, 5), 1),
                        ((8, 9), 1),
                        ((1, 3), 1),
                        ((2, 3), 1),
                        ((9, 8), 1),
                        ((0, 1), 1),
                        ((4, 6), 1),
                        ((6, 5), 1),
                        ((3, 0), 1),
                    ],
                ),
            ],
            vec![2, 3].into_iter().collect(),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        create_cube_and_assert_adaptive(
            &computation,
            1,
            4,
            "data/test_data/scc/4",
            &expected_results,
            &splits,
            &expect_splits,
            index,
        );
    }
}

type ExpectedResults<C> = (GSTimestamp, Vec<(<C as ComputationTypes>::Result, DiffCount)>);

fn create_cube_and_assert<C: Computation>(
    computation: &C,
    m: usize,
    n: usize,
    dir: &str,
    expected_diff_results: &[ExpectedResults<C>],
    expected_full_results: &[ExpectedResults<C>],
    port_offset: usize,
) {
    let mut global_store = GlobalStore::default();

    let cube_name = "my_cube";
    let mut cube_query = format!(
        "load cube {name} {m} {n} from '{dir}' with prefix 'batch-0_';",
        name = cube_name,
        m = m,
        n = n,
        dir = dir,
    );
    process_query(&mut global_store, &mut cube_query).expect("Cube not loaded");

    let cube = global_store.filtered_cube_store.cubes.get_mut(cube_name).expect("Expected cube");

    cube.prepare_differential_data();

    let expected_diff_results = expected_diff_results
        .iter()
        .map(|(ts, values)| (*ts, values.iter().copied().collect::<HashSet<_>>()))
        .collect::<HashMap<_, _>>();

    let expected_full_results = expected_full_results
        .iter()
        .map(|(ts, values)| (*ts, values.iter().copied().collect::<HashSet<_>>()))
        .collect::<HashMap<_, _>>();

    let mut runtime_data = ComputationRuntimeData {
        total_vertices: global_store.graph.vertex_count(),
        c_type: ComputationType::Basic,
        materialize_results: MaterializeResults::Full,
        ..ComputationRuntimeData::default()
    };

    for &threads in &[1, 4] {
        runtime_data.threads = threads;
        let results = differential_run_basic(
            DiffIteratorPointer::new(
                &cube.differential_data.as_ref().expect("Data expected").cube_diff_iterators,
            ),
            GSTimestamp::get_zeroth_timestamp(),
            computation.clone(),
            &runtime_data,
        )
        .expect("Computation failed");
        let (computed_results, _, _) =
            process_results::<C>(results, true).expect("Processing failed");

        assert_results_match::<C>(
            computed_results,
            &expected_diff_results,
            &format!("Failed to match results of computation using {} threads", threads),
        );

        let results = differential_run_2_stage(
            CubePointer::new(&cube.data),
            computation.clone(),
            GSTimestamp::get_zeroth_timestamp(),
            &runtime_data,
        )
        .expect("Computation failed");
        let (computed_results_arranged, _, _) =
            process_results::<C>(results, true).expect("Processing failed");

        assert_results_match::<C>(
            computed_results_arranged,
            &expected_diff_results,
            &format!("Failed to match results of arranged computation using {} threads", threads),
        );
    }

    runtime_data.threads = 2;
    runtime_data.hosts =
        vec![format!("localhost:{}", port_offset), format!("localhost:{}", port_offset + 1)];
    let cdata = cube.data.clone();
    let comp = computation.clone();
    let mut runtime_data2 = runtime_data.clone();

    let thread = std::thread::spawn(move || {
        runtime_data2.process_id = 1;
        differential_run_2_stage(
            CubePointer::new(&cdata),
            comp,
            GSTimestamp::get_zeroth_timestamp(),
            &runtime_data2,
        )
        .expect("Computation failed");
    });
    let results = differential_run_2_stage(
        CubePointer::new(&cube.data),
        computation.clone(),
        GSTimestamp::get_zeroth_timestamp(),
        &runtime_data,
    )
    .expect("Computation failed");
    thread.join().expect("Thread error");

    let (computed_results_arranged_multiprocess, _, _) =
        process_results::<C>(results, true).expect("Processing failed");
    assert_results_match::<C>(
        computed_results_arranged_multiprocess,
        &expected_diff_results,
        &format!(
            "Failed to match results of arranged computation using {} threads and 2 processes",
            runtime_data.threads
        ),
    );

    runtime_data.hosts.clear();
    runtime_data.threads = 1;
    let full_results =
        differential_diff_execute(cube, computation, &runtime_data).expect("Computation failed");
    assert_eq!(
        full_results
            .into_iter()
            .map(|(_, (data, timestamp))| (timestamp, data.into_iter().collect::<HashSet<_>>()))
            .collect::<HashMap<_, _>>(),
        expected_full_results
    );
}

fn create_cube_and_assert_adaptive<C: Computation>(
    computation: &C,
    m: usize,
    n: usize,
    dir: &str,
    expected_diff_results: &[ExpectedResults<C>],
    specified_splits: &Option<SplitIndices>,
    expected_splits: &SplitIndices,
    index: usize,
) {
    let mut global_store = GlobalStore::default();

    let cube_name = "my_cube";
    let mut cube_query = format!(
        "load cube {name} {m} {n} from '{dir}' with prefix 'batch-0_';",
        name = cube_name,
        m = m,
        n = n,
        dir = dir,
    );
    process_query(&mut global_store, &mut cube_query).expect("Cube not loaded");

    let cube = global_store.filtered_cube_store.cubes.get_mut(cube_name).expect("Expected cube");

    cube.prepare_differential_data();

    let expected_diff_results = expected_diff_results
        .iter()
        .map(|(ts, values)| (*ts, values.iter().copied().collect::<HashSet<_>>()))
        .collect::<HashMap<_, _>>();

    let mut runtime_data = ComputationRuntimeData {
        materialize_results: MaterializeResults::Diff,
        splits: specified_splits.clone(),
        ..ComputationRuntimeData::default()
    };

    for &threads in &[1, 4] {
        runtime_data.threads = threads;
        runtime_data.batch_size = Some(2);
        let results = differential_run_adaptive(
            CubePointer::new(&cube.data),
            computation.clone(),
            &runtime_data,
        )
        .expect("Computation failed");
        let (computed_results_arranged, _, obtained_splits) =
            process_results::<C>(results, true).expect("Processing failed");

        assert_results_match::<C>(
            computed_results_arranged,
            &expected_diff_results,
            &format!(
                "Failed to match results of arranged computation using {} threads in {}",
                threads, index
            ),
        );

        assert_eq!(&obtained_splits, expected_splits);
    }
}

fn assert_results_match<C: Computation>(
    obtained: DifferentialResults<C, GSTimestamp>,
    expected: &HashMap<GSTimestamp, HashSet<(C::Result, DiffCount)>>,
    msg: &str,
) {
    assert_eq!(
        &obtained
            .into_iter()
            .map(|(ts, values)| (ts, values.into_iter().collect::<HashSet<_>>()))
            .collect::<HashMap<_, _>>(),
        expected,
        "{}",
        msg,
    );
}
