use crate::queue::queue::new_queue;
use crate::structure::*;
use lunatic::process;
use std::rc::Rc;

#[derive(Default)]
pub struct TopicTree {
    queues: Vec<Queue>,
}

struct WildcardMatcher<'a> {
    cursor: usize,
    state: MatcherState,
    prev: Option<u8>,
    topic_bytes: &'a [u8],
    topic: &'a str,
    wildcard: &'a str,
}

#[derive(PartialEq, Debug)]
enum MatcherState {
    Initial,
    Literal(u8),
    NotSeparator,
    Terminal,
    Invalid,
}

// TODO: add UTF-8 support to topic names/patterns
// TODO: reject message if Unicode \x0000 is encountered
// TODO: reject invalid patterns (e.g. hello+, "a/#" would now match "a//")
impl<'a> WildcardMatcher<'a> {
    pub fn new(wildcard: &'a str, topic: &'a str) -> WildcardMatcher<'a> {
        let mut r = WildcardMatcher {
            state: MatcherState::Initial,
            topic,
            topic_bytes: topic.as_bytes(),
            cursor: 0,
            wildcard,
            prev: None,
        };
        // enable / prefixing
        if let ("+", "/", x) = (&wildcard[..1], &topic[..1], &wildcard.get(..2)) {
            r.cursor += 1;
            if let Some("+/") = x {
                r.wildcard = &wildcard[2..];
            }
        }
        r
    }

    fn to_next_state(&mut self, i: usize, w: u8) -> bool {
        let WildcardMatcher { prev, wildcard, .. } = *self;
        let next_wildcard = &wildcard[i..];
        let (has_next, state) = match w {
            b'#' => {
                // since we excluded the wildcard "#" above, the wildcard needs
                // to be prepended by a "/" and be the last character in the path
                (
                    false,
                    if let "/#" = next_wildcard {
                        MatcherState::Terminal
                    } else if prev == Some(b'/') || i < wildcard.len() {
                        MatcherState::Terminal
                    } else {
                        MatcherState::Invalid
                    },
                )
            }
            b'+' => (
                prev == None || prev == Some(b'/'),
                MatcherState::NotSeparator,
            ),
            b'/' => {
                if "/#" == next_wildcard {
                    (false, MatcherState::Terminal)
                } else {
                    (true, MatcherState::Literal(b'/'))
                }
            }
            b => (true, MatcherState::Literal(b)),
        };
        println!("SELECTING STATE {:?} -> {:?}", self.state, state);
        self.state = state;
        has_next
    }

    pub fn matches_topic(wildcard: &str, topic: &str) -> bool {
        let mut matcher = WildcardMatcher::new(wildcard, topic);
        let WildcardMatcher {
            topic,
            wildcard,
            mut cursor,
            // mut state,
            // mut prev,
            topic_bytes,
            ..
        } = matcher;
        if topic.contains(&['+', '#'][..]) || wildcard.is_empty() {
            return false;
        } else if wildcard == "#" {
            return true;
        }
        for (i, w) in wildcard.bytes().enumerate() {
            if let false = matcher.to_next_state(i, w) {
                break;
            }
            println!("PICKED STATE {} {:?}", w, matcher.state);
            let next_wildcard = &wildcard[i..];
            let success = match matcher.state {
                MatcherState::Literal(b) => {
                    println!("MATCHING LITERAL {} {} {:?}", b, cursor, next_wildcard);
                    if topic.len() <= cursor || topic_bytes[cursor] != b {
                        cursor = 0;
                        "/#" == next_wildcard
                    } else {
                        cursor += 1;
                        true
                    }
                }
                MatcherState::NotSeparator => {
                    if topic_bytes[cursor] != b'/' {
                        cursor += 1;
                    }
                    for c in topic_bytes[cursor..].iter() {
                        if *c == b'/' {
                            break;
                        }
                        cursor += 1;
                    }
                    true
                }
                _ => false,
            };
            if cursor == 0 {
                return success;
            }
            // println!("CONSUMED {} {} | {}", wildcard, success, c);
            if !success {
                return false;
            }
            // move to next state
            matcher.prev = Some(w);
        }
        println!(
            "FINISHED WITH {:?} {} {}\n\n",
            matcher.state,
            cursor,
            topic.len()
        );
        match matcher.state {
            MatcherState::Terminal => true,
            MatcherState::Invalid => false,
            MatcherState::NotSeparator | MatcherState::Literal(_) => cursor == topic.len(),
            _ => false,
        }
    }
}

impl TopicTree {
    pub fn new() -> TopicTree {
        TopicTree { queues: Vec::new() }
    }

    pub fn ensure_topic_queue(&mut self, topic: &str) -> Queue {
        if let Some(found) = self.queues.iter().position(|q| q.name == topic) {
            self.queues.get(found).unwrap().clone()
        } else {
            let queue = Queue {
                name: topic.to_string(),
                process: process::spawn_with(topic.to_string(), new_queue).unwrap(),
            };
            self.queues.push(queue);
            let len = self.queues.len();
            self.queues.get(len - 1).unwrap().clone()
        }
    }

    pub fn get_matching_queues(&mut self, topic_pattern: &str) -> Vec<Queue> {
        if topic_pattern.contains(&['+', '#'][..]) {
            return self.match_wildcard_subscriptions(topic_pattern);
        }
        vec![self.ensure_topic_queue(topic_pattern)]
    }

    pub fn match_wildcard_subscriptions(&mut self, wildcard: &str) -> Vec<Queue> {
        let keys: Vec<String> = self
            .queues
            .iter()
            .filter_map(|q| {
                if WildcardMatcher::matches_topic(wildcard, &q.name) {
                    Some(q.name.clone())
                } else {
                    None
                }
            })
            .collect();
        keys.iter()
            .map(|key| self.ensure_topic_queue(key))
            .collect::<Vec<Queue>>()
    }
}

#[cfg(test)]
mod simple_tests {
    use super::*;

    // Matching subscriptions with literals and wildcards
    #[test]
    fn literal_subscription() {
        let subscription_matcher = "finance";
        let prefix_subscription_matcher = "/finance";
        assert_eq!(
            WildcardMatcher::matches_topic(subscription_matcher, "finance"),
            true
        );
        assert_eq!(
            WildcardMatcher::matches_topic(subscription_matcher, "finances"),
            false
        );
        assert_eq!(
            WildcardMatcher::matches_topic(subscription_matcher, "finance/"),
            false
        );
        assert_eq!(
            WildcardMatcher::matches_topic(subscription_matcher, "/finance"),
            false
        );
        assert_eq!(
            WildcardMatcher::matches_topic(prefix_subscription_matcher, "/finance"),
            true
        );
    }

    #[test]
    fn single_level_wildcard() {
        let subscription_matcher = "finance/+";
        assert_eq!(
            WildcardMatcher::matches_topic(subscription_matcher, "finance/stocks"),
            true
        );
        assert_eq!(
            WildcardMatcher::matches_topic(subscription_matcher, "finance/commodities"),
            true
        );
        // should not match these
        assert_eq!(
            WildcardMatcher::matches_topic(subscription_matcher, "finance/commodities/oil"),
            false
        );
        assert_eq!(
            WildcardMatcher::matches_topic(subscription_matcher, "/finance/stocks"),
            false
        );
        assert_eq!(
            WildcardMatcher::matches_topic(subscription_matcher, "finance"),
            false
        );
    }

    #[test]
    fn complex_cases() {
        let subscription_matcher = "users/+/device/+/permissions/#";
        assert!(WildcardMatcher::matches_topic(
            subscription_matcher,
            "users/john_123/device/samsung_galaxy/permissions"
        ));
        assert!(WildcardMatcher::matches_topic(
            subscription_matcher,
            "users/john_123/device/samsung galaxy/permissions/account/can_delete"
        ));
        assert!(WildcardMatcher::matches_topic(
            subscription_matcher,
            "users/john_123/device/samsung_galaxy/permissions/is_admin!!"
        ));
        // should not match these
        assert_eq!(
            WildcardMatcher::matches_topic(
                subscription_matcher,
                "users/john_123/device/samsung_galaxy/permission"
            ),
            false
        );
        assert_eq!(
            WildcardMatcher::matches_topic(
                subscription_matcher,
                "users/john_123/4/device/samsung_galaxy/permissions/is_admin!!"
            ),
            false
        );
        assert_eq!(
            WildcardMatcher::matches_topic(
                subscription_matcher,
                "users/john_123/device/permissions/is_admin!!"
            ),
            false
        );
    }

    #[test]
    fn special_cases() {
        let subscription_matcher = "#";
        assert!(WildcardMatcher::matches_topic(
            subscription_matcher,
            "users/john_123/device/samsung_galaxy/permissions"
        ));
        assert!(WildcardMatcher::matches_topic(
            subscription_matcher,
            "users"
        ));
        assert!(WildcardMatcher::matches_topic(
            subscription_matcher,
            "/users"
        ));
        // handle +
        let subscription_matcher = "+";
        assert_eq!(
            WildcardMatcher::matches_topic(subscription_matcher, "users"),
            true
        );
        assert_eq!(
            WildcardMatcher::matches_topic(subscription_matcher, "users/john_123"),
            false
        );
        // handle +/+
        let subscription_matcher = "+/+";
        assert!(WildcardMatcher::matches_topic(
            subscription_matcher,
            "users/bob"
        ));
        assert!(WildcardMatcher::matches_topic(
            subscription_matcher,
            "/users"
        ));
        // handle /+
        let subscription_matcher = "/+";
        assert!(WildcardMatcher::matches_topic(
            subscription_matcher,
            "/users"
        ));
        assert_eq!(
            WildcardMatcher::matches_topic(subscription_matcher, "users"),
            false
        );
    }
}
