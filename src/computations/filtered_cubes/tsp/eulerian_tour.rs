use crate::computations::filtered_cubes::tsp::{EulerianTour, MST};
use itertools::Itertools;
use std::collections::VecDeque;
use std::convert::TryInto;

pub fn find_eulerian_tour(mut mst: MST) -> EulerianTour {
    let edge_count = mst.values_mut().fold(0, |sum, neighbors| {
        assert_eq!(neighbors.len() % 2, 0, "All vertices should have even degree");
        // Sort adjacency lists by descending neighbor ids, to maintain user defined order as
        // much as possible.
        neighbors.sort_unstable_by_key(|&(id, _)| {
            0_usize.overflowing_sub(id.try_into().expect("Overflow")).0
        });
        sum + neighbors.len()
    });
    let start_vertex = 0;
    let mut stack = VecDeque::new();
    let mut tour = VecDeque::new();
    stack.push_back(start_vertex);
    while let Some(mut current_vertex_id) = stack.pop_back() {
        while let Some((neighbor_id, weight)) =
            mst.get_mut(&current_vertex_id).expect("current_vertex_id should be in the MST").pop()
        {
            // Store next vertex to be processed.
            stack.push_back(current_vertex_id);
            // Remove `current_vertex_id` from the `neighbor_id`'s adjacency list.
            let adj_list = mst.get_mut(&neighbor_id).expect("neighbor_id should be in the MST");
            adj_list
                .iter()
                .position(|&item| item == (current_vertex_id, weight))
                .map(|i| adj_list.remove(i))
                .expect("Mirror edge should exists");
            // Follow the trail.
            current_vertex_id = neighbor_id;
        }
        tour.push_front(current_vertex_id);
    }
    assert_eq!(tour.len(), edge_count / 2 + 1, "Tour should use exactly all edges in MST");
    tour.into_iter().collect_vec()
}

#[cfg(test)]
mod tests {
    use crate::computations::filtered_cubes::tsp::eulerian_tour::find_eulerian_tour;
    use crate::computations::filtered_cubes::tsp::MST;
    use gs_analytics_api::VertexId;
    use hashbrown::HashMap;
    use std::convert::TryFrom;

    type Edges = Vec<(VertexId, VertexId)>;

    #[test]
    fn test1() {
        let (map, edges) = map_from_edges(vec![
            (0, 1),
            (1, 5),
            (1, 7),
            (4, 5),
            (4, 8),
            (1, 6),
            (3, 7),
            (5, 9),
            (2, 4),
            (0, 4),
            (2, 5),
            (3, 6),
            (8, 9),
        ]);
        assert_tour(map, edges);
    }

    #[test]
    fn test2() {
        let vec = vec![
            vec![0, 1, 0, 0, 1],
            vec![1, 0, 1, 1, 1],
            vec![0, 1, 0, 1, 0],
            vec![0, 1, 1, 0, 0],
            vec![1, 1, 0, 0, 0],
        ];
        let (map, edges) = map_from_matrix(vec);
        assert_tour(map, edges);
    }

    #[test]
    fn test3() {
        let (map, edges) = map_from_edges(vec![
            (0, 1),
            (1, 3),
            (0, 3),
            (0, 2),
            (1, 2),
            (2, 3),
            (1, 4),
            (3, 4),
            (2, 4),
            (0, 5),
            (4, 5),
        ]);
        assert_tour(map, edges);
    }

    #[test]
    fn test4() {
        let (map, edges) = map_from_edges(vec![
            (0, 9),
            (0, 4),
            (0, 2),
            (0, 5),
            (0, 4),
            (0, 4),
            (0, 3),
            (0, 1),
            (1, 7),
            (2, 4),
            (2, 4),
            (2, 7),
            (3, 8),
            (4, 9),
            (4, 5),
            (4, 9),
            (5, 5),
            (5, 5),
            (6, 9),
            (6, 9),
            (8, 9),
        ]);
        assert_tour(map, edges);
    }

    fn assert_tour(map: MST, mut edges: Edges) {
        let tour = find_eulerian_tour(map);
        for index in 0..tour.len() - 1 {
            let (s, d) = (tour[index], tour[index + 1]);
            edges
                .iter()
                .position(|&item| item == (s, d))
                .map(|i| edges.remove(i))
                .expect("Edge should exist");
            edges
                .iter()
                .position(|&item| item == (d, s))
                .map(|i| edges.remove(i))
                .expect("Edge should exist");
        }
        assert!(edges.is_empty());
    }

    fn map_from_matrix(matrix: Vec<Vec<usize>>) -> (MST, Edges) {
        let mut map = HashMap::new();
        let mut edges = Vec::new();
        for (src, row) in matrix.into_iter().enumerate() {
            for (dst, val) in row.into_iter().enumerate() {
                if val == 1 {
                    map.entry(VertexId::try_from(src).expect("Overflow"))
                        .or_insert_with(Vec::new)
                        .push((VertexId::try_from(dst).expect("Overflow"), 0));
                    edges.push((
                        VertexId::try_from(src).expect("Overflow"),
                        VertexId::try_from(dst).expect("Overflow"),
                    ));
                }
            }
        }
        (map, edges)
    }

    fn map_from_edges(edges: Edges) -> (MST, Edges) {
        let mut map = HashMap::new();
        let mut new_edges = Vec::new();
        for (src, dst) in edges {
            map.entry(src).or_insert_with(Vec::new).push((dst, 0));
            map.entry(dst).or_insert_with(Vec::new).push((src, 0));
            new_edges.push((src, dst));
            new_edges.push((dst, src));
        }
        (map, new_edges)
    }
}
