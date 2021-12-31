use super::structure::*;
use std::collections::HashMap;

pub struct TopicTree {
    queues: HashMap<String, Queue>,
}

impl TopicTree {
    fn parse_topic_insert(topic: String) -> Result<Vec<String>, String> {
        if topic.contains(&['#', '+'][..]) {
            return Err(format!("Topic cannot contain wildcard on insert {}", topic));
        }
        if topic.len() == 0 || topic == "/" {
            return Err(format!("Topic cannot be empty or /"));
        }
        TopicTree::split_topic_paths(topic)
    }

    fn parse_topic_subscribe(topic: String) {}

    fn split_topic_paths(topic: String) -> Result<Vec<String>, String> {
        let mut curr = if topic.starts_with("/") {
            "/".to_string()
        } else {
            String::new()
        };
        let mut v = vec![];
        let mut skipped_first = false;
        for s in topic.split("/").filter(|s| s.len() > 0) {
            if skipped_first {
                curr.push_str(&"/".to_string());
            }
            skipped_first = true;
            curr.push_str(s);
            v.push(curr.clone());
        }
        Ok(v)
    }

    pub fn new() -> TopicTree {
        TopicTree {
            queues: HashMap::new(),
        }
    }

    pub fn ensure_topic_queue(&mut self, topic: String) -> &mut Queue {
        println!(
            "PARSED TOPIC {:?}",
            TopicTree::parse_topic_insert(topic.clone())
        );
        self.queues.entry(topic.clone()).or_insert(Queue {
            name: topic,
            subscribers: Vec::new(),
        })
    }

    pub fn add_subscription(&mut self, topic_pattern: String, sub: Subscription) {
        if topic_pattern.contains(&['+', '#'][..]) {
            return self.add_wildcard_subscription(topic_pattern, sub);
        }
        let queue = self.ensure_topic_queue(topic_pattern);
        queue.subscribers.push(sub);
    }

    pub fn add_wildcard_subscription(&mut self, topic_pattern: String, sub: Subscription) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::{BufRead, BufReader, Error, Write};

    #[test]
    fn regular_insert_path() {
        let result = TopicTree::parse_topic_insert("finance/stocks".to_string());
        assert_eq!(result.is_ok(), true);
        let split = result.unwrap();
        assert_eq!(split, ["finance", "finance/stocks"]);
    }

    #[test]
    fn prefixed_insert_path() {
        let result = TopicTree::parse_topic_insert("/finance/stocks".to_string());
        assert_eq!(result.is_ok(), true);
        let split = result.unwrap();
        assert_eq!(split, ["/finance", "/finance/stocks"]);
    }

    #[test]
    fn invalid_path() {
        let result = TopicTree::parse_topic_insert("finance/+".to_string());
        assert_eq!(result.is_err(), true);

        let result = TopicTree::parse_topic_insert("/".to_string());
        assert_eq!(result.is_err(), true);

        let result = TopicTree::parse_topic_insert("".to_string());
        assert_eq!(result.is_err(), true);
    }
}
