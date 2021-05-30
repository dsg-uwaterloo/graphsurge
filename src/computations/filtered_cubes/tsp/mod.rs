mod eulerian_tour;
mod mst;
mod mwpm;
mod union_find;

use crate::computations::filtered_cubes::tsp::eulerian_tour::find_eulerian_tour;
use crate::computations::filtered_cubes::tsp::mst::minimum_spanning_tree;
use crate::computations::filtered_cubes::tsp::mwpm::minimum_weight_perfect_matching;
use crate::computations::filtered_cubes::{DimensionOrder, MatrixRow};
use crate::filtered_cubes::timestamp::DimensionId;
use gs_analytics_api::VertexId;
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use log::info;
use rand::seq::IteratorRandom;
use std::convert::TryFrom;

type EdgeWeight = usize;
type MST = HashMap<VertexId, Vec<(VertexId, EdgeWeight)>>;
type EulerianTour = Vec<VertexId>;
type HamiltonianCircuit = DimensionOrder;

pub fn tsp(matrix: &[MatrixRow]) -> DimensionOrder {
    info!("Asserting triangle inequality...");
    let mut rng = rand::thread_rng();
    let vertex_count = matrix.len();
    for combination in (0..vertex_count).choose_multiple(&mut rng, 30).into_iter().combinations(3) {
        let u = combination[0];
        let v = combination[1];
        let w = combination[2];
        assert!(matrix[u][v] + matrix[v][w] >= matrix[w][u], "Triangle equality not satisfied");
    }
    info!("Generating MST...");
    let mut mst = minimum_spanning_tree(matrix);
    info!("Detecting odd vertices...");
    let odd_vertices = find_odd_vertices(&mst);
    assert_eq!(odd_vertices.len() % 2, 0, "Expected that number of odd vertices will be even");
    info!("Generating MWPM...");
    minimum_weight_perfect_matching(&mut mst, matrix, odd_vertices);
    info!("Finding Eulerian Tour...");
    let eulerian_tour = find_eulerian_tour(mst);
    info!("Finding Hamiltonian Circuit...");
    let hamiltonian_circuit = create_hamiltonian_circuit(eulerian_tour);
    assert_eq!(
        hamiltonian_circuit.iter().copied().collect::<HashSet<_>>().len(),
        vertex_count,
        "Expected that all vertices will appear in the hamiltonian circuit"
    );
    hamiltonian_circuit
}

fn find_odd_vertices(mst: &MST) -> Vec<VertexId> {
    mst.iter()
        .filter_map(|(v, neighbors)| if neighbors.len() % 2 == 1 { Some(*v) } else { None })
        .collect_vec()
}

fn create_hamiltonian_circuit(eulerian_tour: EulerianTour) -> HamiltonianCircuit {
    let mut visited = HashSet::new();
    eulerian_tour
        .into_iter()
        .filter_map(|vertex| {
            if visited.insert(vertex) {
                Some(DimensionId::try_from(vertex).expect("Vertex to DimensionId overflow"))
            } else {
                None
            }
        })
        .collect_vec()
}

#[cfg(test)]
mod tests {
    use crate::computations::filtered_cubes::tsp::tsp;
    use crate::computations::filtered_cubes::{DimensionOrder, Matrix, MatrixRow};
    use crate::filtered_cubes::timestamp::DimensionId;
    use crate::util::io::get_file_lines;
    use itertools::Itertools;
    use std::convert::TryFrom;

    // Disabled because these don't satisfy triangle inequality.
    //    #[test]
    //    fn test_tsp_fri() {
    //        let size = 26;
    //        let matrix = get_graph_matrix("data/tsp/fri26.txt", size);
    //        let known_optimal_order = get_zero_based_optimal_order(vec![
    //            1, 25, 24, 23, 26, 22, 21, 17, 18, 20, 19, 16, 11, 12, 13, 15, 14, 10, 9, 8, 7, 5, 6,
    //            4, 3, 2,
    //        ]);
    //        let known_optimal_length = 937;
    //        assert_tsp(&matrix, size, &known_optimal_order, known_optimal_length);
    //    }
    //
    //    #[test]
    //    fn test_tsp_bays() {
    //        let size = 29;
    //        let matrix = get_graph_matrix("data/tsp/bays29.txt", size);
    //        let known_optimal_order = get_zero_based_optimal_order(vec![
    //            1, 28, 6, 12, 9, 5, 26, 29, 3, 2, 20, 10, 4, 15, 18, 17, 14, 22, 11, 19, 25, 7, 23, 27,
    //            8, 24, 16, 13, 21,
    //        ]);
    //        let known_optimal_length = get_optimal_length(&known_optimal_order, &matrix);
    //        assert_tsp(&matrix, size, &known_optimal_order, known_optimal_length);
    //    }

    #[test]
    fn test_tsp_att() {
        let size = 48;
        let matrix = get_graph_matrix("data/tsp/att48.txt", size);
        let known_optimal_order = get_zero_based_optimal_order(vec![
            1, 8, 38, 31, 44, 18, 7, 28, 6, 37, 19, 27, 17, 43, 30, 36, 46, 33, 20, 47, 21, 32, 39,
            48, 5, 42, 24, 10, 45, 35, 4, 26, 2, 29, 34, 41, 16, 22, 3, 23, 14, 25, 13, 11, 12, 15,
            40, 9,
        ]);
        let known_optimal_length = 33551;
        assert_tsp(&matrix, size, &known_optimal_order, known_optimal_length);
    }

    #[allow(clippy::cast_precision_loss)]
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    fn assert_tsp(
        matrix: &[MatrixRow],
        size: usize,
        known_optimal_order: &[DimensionId],
        known_optimal_length: usize,
    ) {
        let computed_order = tsp(&matrix);
        assert_eq!(computed_order.len(), size, "Computed order should contain all vertices");
        assert_eq!(known_optimal_order.len(), size, "Known order should contain all vertices");
        assert_eq!(
            get_optimal_length(&known_optimal_order, &matrix),
            known_optimal_length,
            "Optimal tour length should match"
        );
        let obtained_optimal_length = get_optimal_length(&computed_order, &matrix);
        assert!(
            obtained_optimal_length <= (known_optimal_length as f64 * 1.5) as usize,
            "Approximate length should be within 1.5 times of optimal length"
        );
    }

    fn get_graph_matrix(file: &str, size: usize) -> Matrix {
        let matrix = get_file_lines(file)
            .expect("IO Error")
            .map(|line| {
                let row = line
                    .split(',')
                    .map(|num_str| num_str.parse::<usize>().expect("Could not parse num"))
                    .collect_vec();
                assert_eq!(row.len(), size, "Unexpected matrix column size");
                row
            })
            .collect_vec();
        assert_eq!(matrix.len(), size, "Unexpected matrix row count");
        matrix
    }

    fn get_zero_based_optimal_order(data: Vec<usize>) -> DimensionOrder {
        data
        .into_iter()
            .map(|v| DimensionId::try_from(v - 1).expect("Overflow")) // Change to zero-based index.
            .collect_vec()
    }

    fn get_optimal_length(optimal_order: &[DimensionId], matrix: &[MatrixRow]) -> usize {
        (0..optimal_order.len() - 1).fold(0, |acc, index| {
            acc + matrix[optimal_order[index] as usize][optimal_order[index + 1] as usize]
        }) + matrix[optimal_order[optimal_order.len() - 1] as usize][0]
    }
}
