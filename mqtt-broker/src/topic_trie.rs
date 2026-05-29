use std::collections::HashMap;
use mqtt_core::QoS;

#[derive(Debug, Clone)]
pub struct Subscription {
    pub client_id: String,
    pub filter: String,
    pub qos: QoS,
    // MQTT 5.0 subscription options
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: u8, // From RetainHandling enum
    pub subscription_identifier: Option<u32>,
}

#[derive(Default)]
struct TrieNode {
    // Exact child levels
    children: HashMap<String, TrieNode>,
    // Wildcard child (+)
    plus_child: Option<Box<TrieNode>>,
    // Wildcard child (#)
    hash_child: Option<Box<TrieNode>>,
    // Active regular subscriptions at this node: client_id -> Subscription
    subscriptions: HashMap<String, Subscription>,
    // Active shared subscriptions at this node: share_name -> Group subscriptions
    // A group subscription is a map of client_id -> Subscription
    shared_subscriptions: HashMap<String, HashMap<String, Subscription>>,
}

pub struct TopicTrie {
    root: TrieNode,
}

impl TopicTrie {
    pub fn new() -> Self {
        Self {
            root: TrieNode::default(),
        }
    }

    /// Insert or update a subscription
    pub fn insert(&mut self, sub: Subscription) {
        use mqtt_core::topic::parse_shared_subscription;

        let filter = sub.filter.clone();
        if let Some((share_name, bare_filter)) = parse_shared_subscription(&filter) {
            let levels: Vec<&str> = bare_filter.split('/').collect();
            let mut node = &mut self.root;
            for level in levels {
                node = get_or_create_child(node, level);
            }
            let group = node.shared_subscriptions.entry(share_name.to_string()).or_default();
            group.insert(sub.client_id.clone(), sub);
        } else {
            let levels: Vec<&str> = filter.split('/').collect();
            let mut node = &mut self.root;
            for level in levels {
                node = get_or_create_child(node, level);
            }
            node.subscriptions.insert(sub.client_id.clone(), sub);
        }
    }

    /// Remove a subscription by topic filter and client ID
    pub fn remove(&mut self, filter: &str, client_id: &str) -> bool {
        use mqtt_core::topic::parse_shared_subscription;

        if let Some((share_name, bare_filter)) = parse_shared_subscription(filter) {
            let levels: Vec<&str> = bare_filter.split('/').collect();
            let mut path = Vec::new();
            let mut current = &mut self.root;
            
            // Navigate and track path for pruning
            path.push(current as *mut TrieNode);
            for level in levels {
                if level == "+" {
                    if let Some(ref mut child) = current.plus_child {
                        current = child.as_mut();
                    } else {
                        return false;
                    }
                } else if level == "#" {
                    if let Some(ref mut child) = current.hash_child {
                        current = child.as_mut();
                    } else {
                        return false;
                    }
                } else if let Some(child) = current.children.get_mut(level) {
                    current = child;
                } else {
                    return false;
                }
                path.push(current as *mut TrieNode);
            }

            let mut removed = false;
            if let Some(group) = current.shared_subscriptions.get_mut(share_name) {
                removed = group.remove(client_id).is_some();
                if group.is_empty() {
                    current.shared_subscriptions.remove(share_name);
                }
            }

            // Pruning could be implemented here but in-memory is fine for v1
            removed
        } else {
            let levels: Vec<&str> = filter.split('/').collect();
            let mut current = &mut self.root;
            for level in levels {
                if level == "+" {
                    if let Some(ref mut child) = current.plus_child {
                        current = child.as_mut();
                    } else {
                        return false;
                    }
                } else if level == "#" {
                    if let Some(ref mut child) = current.hash_child {
                        current = child.as_mut();
                    } else {
                        return false;
                    }
                } else if let Some(child) = current.children.get_mut(level) {
                    current = child;
                } else {
                    return false;
                }
            }

            current.subscriptions.remove(client_id).is_some()
        }
    }

    /// Find all matching subscriptions for a publish topic.
    /// Returns:
    /// 1. A vector of non-shared subscriptions
    /// 2. A HashMap of group_name -> Vec of shared subscriptions (for round-robin selection)
    pub fn matches(&self, topic: &str) -> (Vec<Subscription>, HashMap<String, Vec<Subscription>>) {
        let levels: Vec<&str> = topic.split('/').collect();
        let mut matched_subs = Vec::new();
        let mut matched_shared = HashMap::new();
        
        let is_system = topic.starts_with('$');
        
        self.match_node(
            &self.root,
            &levels,
            0,
            is_system,
            &mut matched_subs,
            &mut matched_shared,
        );

        (matched_subs, matched_shared)
    }

    fn match_node(
        &self,
        node: &TrieNode,
        levels: &[&str],
        index: usize,
        is_system: bool,
        matched_subs: &mut Vec<Subscription>,
        matched_shared: &mut HashMap<String, Vec<Subscription>>,
    ) {
        // 1. # matches all remaining levels
        if let Some(ref hash_node) = node.hash_child {
            // System topics cannot be matched by wildcards at first level
            if !(is_system && index == 0) {
                for sub in hash_node.subscriptions.values() {
                    matched_subs.push(sub.clone());
                }
                for (group_name, group_subs) in &hash_node.shared_subscriptions {
                    let list = matched_shared.entry(group_name.clone()).or_default();
                    for sub in group_subs.values() {
                        list.push(sub.clone());
                    }
                }
            }
        }

        // 2. If we've reached the end of the topic levels
        if index == levels.len() {
            for sub in node.subscriptions.values() {
                matched_subs.push(sub.clone());
            }
            for (group_name, group_subs) in &node.shared_subscriptions {
                let list = matched_shared.entry(group_name.clone()).or_default();
                for sub in group_subs.values() {
                    list.push(sub.clone());
                }
            }
            return;
        }

        let current_level = levels[index];

        // 3. Exact match
        if let Some(child) = node.children.get(current_level) {
            self.match_node(child, levels, index + 1, is_system, matched_subs, matched_shared);
        }

        // 4. + wildcard match
        if let Some(ref plus_node) = node.plus_child {
            if !(is_system && index == 0) {
                self.match_node(
                    plus_node,
                    levels,
                    index + 1,
                    is_system,
                    matched_subs,
                    matched_shared,
                );
            }
        }
    }
}

fn get_or_create_child<'a>(node: &'a mut TrieNode, level: &str) -> &'a mut TrieNode {
    if level == "+" {
        if node.plus_child.is_none() {
            node.plus_child = Some(Box::new(TrieNode::default()));
        }
        node.plus_child.as_mut().unwrap()
    } else if level == "#" {
        if node.hash_child.is_none() {
            node.hash_child = Some(Box::new(TrieNode::default()));
        }
        node.hash_child.as_mut().unwrap()
    } else {
        node.children.entry(level.to_string()).or_default()
    }
}
