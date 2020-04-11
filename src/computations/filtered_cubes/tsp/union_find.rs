use hashbrown::HashMap;

#[derive(Default)]
pub struct UnionFind {
    parents: HashMap<usize, usize>,
    ranks: HashMap<usize, usize>,
}

impl UnionFind {
    pub fn get(&mut self, value: usize) -> usize {
        if let Some(parent) = self.parents.get(&value) {
            let mut root = *parent;

            // Find path of objects leading to the root.
            let mut path = vec![value];
            while root != path[path.len() - 1] {
                path.push(root);
                root = self.parents[&root];
            }

            // Compress the path and return.
            for ancestor in path {
                if let Some(v) = self.parents.get_mut(&ancestor) {
                    *v = root;
                }
            }
            root
        } else {
            self.parents.insert(value, value);
            self.ranks.insert(value, 1);
            value
        }
    }

    pub fn union(&mut self, u: usize, v: usize) {
        let roots = vec![self.get(u), self.get(v)];
        let heaviest = roots
            .iter()
            .map(|r| (self.ranks[r], r))
            .max_by_key(|k| k.0)
            .map(|(_, r)| r)
            .expect("Should be present");
        for r in &roots {
            if r != heaviest {
                let val = self.ranks[r];
                if let Some(v) = self.ranks.get_mut(heaviest) {
                    *v += val;
                }
                if let Some(v) = self.parents.get_mut(r) {
                    *v = *heaviest;
                }
            }
        }
    }
}
