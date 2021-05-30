use crate::computations::filtered_cubes::tsp::union_find::UnionFind;
use crate::computations::filtered_cubes::tsp::MST;
use crate::computations::filtered_cubes::MatrixRow;
use gs_analytics_api::VertexId;
use hashbrown::HashMap;
use itertools::Itertools;
use std::convert::TryFrom;

// Minimum spanning tree using Kruskal's algorithm.
pub fn minimum_spanning_tree(matrix: &[MatrixRow]) -> MST {
    let mut tree = HashMap::new();
    let mut subtrees = UnionFind::default();
    for (w, u, v) in (0..matrix.len())
        .flat_map(|u| (0..matrix.len()).map(move |v| (matrix[u][v], u, v)))
        .sorted_by_key(|v| v.0)
    {
        if subtrees.get(u) != subtrees.get(v) {
            tree.entry(VertexId::try_from(u).expect("Overflow"))
                .or_insert_with(Vec::new)
                .push((VertexId::try_from(v).expect("Overflow"), w));
            tree.entry(VertexId::try_from(v).expect("Overflow"))
                .or_insert_with(Vec::new)
                .push((VertexId::try_from(u).expect("Overflow"), w));
            subtrees.union(u, v);
        }
    }
    tree
}

#[cfg(test)]
pub(super) mod tests {
    use crate::computations::filtered_cubes::tsp::mst::minimum_spanning_tree;
    use crate::computations::filtered_cubes::tsp::{EdgeWeight, MST};
    use crate::computations::filtered_cubes::Matrix;
    use gs_analytics_api::VertexId;
    use hashbrown::{HashMap, HashSet};
    use itertools::Itertools;
    use std::convert::TryFrom;
    use std::iter;
    use std::iter::FromIterator;

    #[test]
    fn test1() {
        let matrix =
            matrix_from_edges(vec![(0, 1, 10), (0, 2, 6), (0, 3, 5), (1, 3, 15), (2, 3, 4)], 4);
        let mst_map = minimum_spanning_tree(&matrix);
        let expected_map = get_expected_map(vec![(0, 3, 5), (0, 1, 10), (2, 3, 4)]);
        assert_eq!(vec_to_map(mst_map), vec_to_map(expected_map));
    }

    #[test]
    fn test2() {
        let matrix = matrix_from_edges(
            vec![(0, 1, 2), (0, 3, 6), (1, 2, 3), (1, 3, 8), (1, 4, 5), (2, 4, 7), (3, 4, 9)],
            5,
        );
        let mst_map = minimum_spanning_tree(&matrix);
        let expected_map = get_expected_map(vec![(0, 1, 2), (1, 2, 3), (0, 3, 6), (1, 4, 5)]);
        assert_eq!(vec_to_map(mst_map), vec_to_map(expected_map));
    }

    #[test]
    fn test3() {
        let matrix = matrix_from_edges(
            vec![
                (0, 1, 7),
                (0, 2, 8),
                (1, 2, 3),
                (1, 3, 6),
                (2, 3, 4),
                (2, 4, 3),
                (3, 4, 2),
                (3, 5, 5),
                (4, 5, 2),
            ],
            6,
        );
        let mst_map = minimum_spanning_tree(&matrix);
        let expected_map =
            get_expected_map(vec![(0, 1, 7), (2, 4, 3), (1, 2, 3), (3, 4, 2), (4, 5, 2)]);
        assert_eq!(vec_to_map(mst_map), vec_to_map(expected_map));
    }

    #[test]
    fn test4() {
        let mst_map = minimum_spanning_tree(&get_test_matrix());
        let expected_map =
            get_expected_map(vec![(0, 3, 10), (1, 3, 9), (2, 3, 6), (3, 4, 9), (4, 5, 8)]);
        assert_eq!(vec_to_map(mst_map), vec_to_map(expected_map));
    }

    fn vec_to_map(map: MST) -> HashMap<VertexId, HashSet<(VertexId, EdgeWeight)>> {
        map.into_iter().map(|(vertex, neighbors)| (vertex, HashSet::from_iter(neighbors))).collect()
    }

    // Used in multiple tests.
    pub fn get_test_matrix() -> Matrix {
        matrix_from_edges(
            vec![
                (0, 1, 11),
                (1, 2, 10),
                (0, 2, 14),
                (0, 4, 10),
                (0, 3, 10),
                (0, 5, 15),
                (1, 3, 9),
                (1, 4, 15),
                (1, 5, 16),
                (3, 4, 9),
                (3, 5, 10),
                (2, 5, 11),
                (2, 4, 13),
                (4, 5, 8),
                (2, 3, 6),
            ],
            6,
        )
    }

    fn matrix_from_edges(edges: Vec<(usize, usize, usize)>, size: usize) -> Matrix {
        // Used to replace zero edge weights when creating the complete graph.
        const MAX: usize = usize::max_value();
        let mut vec = (0..size).map(|_| iter::repeat(MAX).take(size).collect_vec()).collect_vec();
        for (s, d, v) in edges {
            vec[s][d] = v;
            vec[d][s] = v;
        }
        vec
    }

    pub fn get_expected_map(data: Vec<(usize, usize, usize)>) -> MST {
        let mut expected_map = HashMap::new();
        data.into_iter().for_each(|(s, d, v)| {
            expected_map
                .entry(VertexId::try_from(s).expect("Overflow"))
                .or_insert_with(Vec::new)
                .push((VertexId::try_from(d).expect("Overflow"), v));
            expected_map
                .entry(VertexId::try_from(d).expect("Overflow"))
                .or_insert_with(Vec::new)
                .push((VertexId::try_from(s).expect("Overflow"), v));
        });
        expected_map
    }
}
