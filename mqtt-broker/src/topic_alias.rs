use std::collections::HashMap;

pub struct TopicAliasMap {
    max_aliases: u16,
    alias_to_topic: HashMap<u16, String>,
}

impl TopicAliasMap {
    pub fn new(max_aliases: u16) -> Self {
        Self {
            max_aliases,
            alias_to_topic: HashMap::new(),
        }
    }

    pub fn get(&self, alias: u16) -> Option<&str> {
        self.alias_to_topic.get(&alias).map(|s| s.as_str())
    }

    pub fn insert(&mut self, alias: u16, topic: String) -> bool {
        if alias == 0 || alias > self.max_aliases {
            return false;
        }
        self.alias_to_topic.insert(alias, topic);
        true
    }
}
