use crate::computations::filtered_cubes::tsp::MST;
use crate::computations::filtered_cubes::MatrixRow;
use gs_analytics_api::VertexId;

pub fn minimum_weight_perfect_matching(
    mst: &mut MST,
    matrix: &[MatrixRow],
    mut odd_vertices: Vec<VertexId>,
) {
    while let Some(v) = odd_vertices.pop() {
        let mut closest = 0;
        let mut length = VertexId::max_value() as usize;
        for u in odd_vertices.iter().copied() {
            let value = matrix[u as usize][v as usize];
            if u != v && value < length {
                length = value;
                closest = u;
            }
        }
        mst.entry(v).or_insert_with(Vec::new).push((closest, length));
        mst.entry(closest).or_insert_with(Vec::new).push((v, length));
        odd_vertices.retain(|&v| v != closest);
    }
}

//#[cfg(test)]
//mod tests {
//    use crate::computations::filtered_cubes::tsp::find_odd_vertices;
//    use crate::computations::filtered_cubes::tsp::mst::minimum_spanning_tree;
//    use crate::computations::filtered_cubes::tsp::mst::tests::{get_expected_map, get_test_matrix};
//    use crate::computations::filtered_cubes::tsp::mwpm::minimum_weight_perfect_matching;
//
//    #[test]
//    fn test1() {
//        let matrix = get_test_matrix();
//        let mut mst = minimum_spanning_tree(&matrix);
//        println!("{:?}", mst);
//        let odd_vertices = find_odd_vertices(&mst);
//        println!("{:?}", odd_vertices);
//        minimum_weight_perfect_matching(&mut mst, &matrix, odd_vertices);
//        let expected_map = get_expected_map(vec![
//            (0, 3, 10),
//            (1, 3, 9),
//            (2, 3, 6),
//            (3, 4, 9),
//            (4, 5, 8),
//            (0, 1, 11),
//            (2, 5, 11),
//        ]);
//        assert_eq!(mst, expected_map);
//    }
//}
