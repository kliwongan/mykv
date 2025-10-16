use std::sync::Arc;
use std::sync::RwLock;

// Thread safe and reasonably optimized in-memory B+ tree implementation
struct BTree<K, V> {
    root: Arc<RwLock<Box<Node<K, V>>>>,
    bsize: usize,
    b: i32,
    page_size: usize,
}

struct Node<K, V> {
    parent: Option<Arc<RwLock<Box<Node<K, V>>>>>,
    children: Option<Vec<Arc<RwLock<Box<Node<K, V>>>>>>,
    keys: Option<Arc<RwLock<Vec<Box<K>>>>>,
    values: Option<Arc<RwLock<Vec<Box<K>>>>>,
    _next: Option<Arc<RwLock<Box<Node<K, V>>>>>,
    _prev: Option<Arc<RwLock<Box<Node<K, V>>>>>,
}

impl<K: PartialOrd + PartialEq, V> BTree<K, V> {
    fn leaf_search(
        &self,
        key: &K,
        node: Arc<RwLock<Box<Node<K, V>>>>,
    ) -> Option<Arc<RwLock<Box<Node<K, V>>>>> {
        if let Some(children) = &node.read().unwrap().children {
            if let Some(keys) = &node.read().unwrap().keys {
                for i in 1..children.len() {
                    if *key <= *keys[i] {
                        return self.leaf_search(&key, children[i].clone());
                    }
                }
            }

            if children.len() > 0 {
                return self.leaf_search(&key, children[children.len() - 1].clone());
            }
        }
        None
    }

    fn search(&self, key: &K) -> bool {
        if let Some(leaf) = self.leaf_search(&key, self.root.clone()) {
            if let Some(leaf_keys) = &leaf.read().unwrap().keys?.read().unwrap() {
                for k in leaf_keys {
                    if *key == **k {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn get_value(&self, key: &K) -> Option<Arc<Box<V>>> {
        if let Some(leaf) = self.leaf_search(&key, self.root.clone()) {
            if let Some(leaf_keys) = &leaf.read().unwrap().keys?.read().unwrap() {
                for (i, k) in leaf_keys.iter().enumerate() {
                    if *key == **k {
                        if let Some(values) = &leaf.read().unwrap().values {
                            return Some(values[i].clone());
                        }
                    }
                }
            }
        }
        None
    }

    fn check_node_full(&self, node: Arc<RwLock<Box<Node<K, V>>>>) -> bool {
        // for now split based on order of nodes
        if let Some(keys) = &node.read().unwrap().keys?.read().unwrap() {
            return keys.len() >= self.b;
        } else {
            false
        }
    }

    fn insert(&self, key: K, value: V) {
        // find location to insert to
        if let Some(leaf) = self.leaf_search(&key, self.root.clone()) {
            if !self.check_node_full(leaf) {
                // add the record
            } else {
                // do some splitting

            }
        }
    }
}

impl<K: PartialOrd + PartialEq, V> Node<K, V> {
    
}
