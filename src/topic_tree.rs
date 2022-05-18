// use crate::queue::queue::new_queue;
use crate::client::WriterProcess;
use crate::structure::*;
use lunatic::process::ProcessRef;
use mqtt_packet_3_5::QoS;
use std::collections::HashMap;

#[derive(Default, Debug)]
pub struct TopicTree {
    counter: u128,
    queues: HashMap<String, Queue>,
    // subscriptions: Vec<(u8, u16, String, WriterProcess)>,
}

#[derive(PartialEq, Debug)]
pub enum State {
    Initial,
    Prefix,
    PrefixAny,
    PrefixSingle,
    Any,
    Literal,
    Separator,
    Single,
    Terminal,
    MaybeSkip,
    PrefixSingleSkip,
}

#[derive(PartialEq, Debug, Clone)]
pub enum WildcardToken {
    Literal(u8),
    Any,
    Single,
    Separator,
    None,
}

#[derive(PartialEq, Debug)]
pub enum TopicToken {
    Literal(u8),
    Separator,
    None,
    Invalid,
}

struct WildcardIter<'a> {
    src: std::slice::Iter<'a, u8>,
    retained: Option<&'a u8>,
}

impl<'a> WildcardIter<'a> {
    pub fn new(src: &'a str) -> WildcardIter<'a> {
        WildcardIter {
            src: src.as_bytes().iter(),
            retained: None,
        }
    }

    pub fn peek(&mut self) -> WildcardToken {
        if self.retained.is_none() {
            self.retained = self.src.next();
        }
        WildcardIter::char_to_token(self.retained)
    }

    pub fn lock(&mut self) {
        if self.retained.is_none() {
            self.retained = self.src.next();
        }
    }

    pub fn unlock(&mut self) {
        if self.retained.is_some() {
            self.retained = None;
        }
    }

    pub fn is_locked(&self) -> bool {
        self.retained.is_some()
    }

    fn char_to_token(char: Option<&u8>) -> WildcardToken {
        match char {
            Some(b'/') => WildcardToken::Separator,
            Some(b'#') => WildcardToken::Any,
            Some(b'+') => WildcardToken::Single,
            Some(l) => WildcardToken::Literal(*l),
            None => WildcardToken::None,
        }
    }
}

impl<'a> Iterator for WildcardIter<'a> {
    type Item = WildcardToken;

    fn next(&mut self) -> Option<Self::Item> {
        if self.retained.is_some() {
            let ret = self.retained;
            self.retained = None;
            return Some(WildcardIter::char_to_token(ret));
        }
        Some(WildcardIter::char_to_token(self.src.next()))
    }
}

struct TopicIter<'a> {
    src: std::slice::Iter<'a, u8>,
}

impl<'a> TopicIter<'a> {
    pub fn new(src: &'a str) -> TopicIter<'a> {
        TopicIter {
            src: src.as_bytes().iter(),
        }
    }
}

impl<'a> Iterator for TopicIter<'a> {
    type Item = TopicToken;

    fn next(&mut self) -> Option<Self::Item> {
        Some(match self.src.next() {
            Some(b'/') => TopicToken::Separator,
            Some(b'#') => TopicToken::Invalid,
            Some(b'+') => TopicToken::Invalid,
            Some(l) => TopicToken::Literal(*l),
            None => TopicToken::None,
        })
    }
}

impl State {
    pub fn match_topic(wildcard: &str, topic: &str) -> bool {
        let mut state = State::Initial;
        let mut w = WildcardIter::new(wildcard);
        let mut t = TopicIter::new(topic);
        for _ in 0..topic.len() * 2 {
            let n_w = if w.is_locked() {
                w.peek()
            } else {
                w.next().unwrap()
            };
            let n_t = t.next().unwrap();
            state = match (state, n_w.clone(), n_t) {
                (State::Initial, WildcardToken::Single, TopicToken::Separator) => {
                    State::PrefixSingle
                }
                (State::Initial, WildcardToken::Any, TopicToken::Separator) => State::PrefixAny,
                (State::Initial, WildcardToken::Any, TopicToken::Literal(_)) => State::Any,
                (State::Initial, WildcardToken::Separator, TopicToken::Separator) => State::Prefix,
                (State::Initial, WildcardToken::Single, TopicToken::Literal(_)) => {
                    w.lock();
                    State::Single
                }
                // drop first "+/" if pattern present
                (State::PrefixSingle, WildcardToken::Separator, TopicToken::Literal(_)) => {
                    w.lock();
                    State::PrefixSingleSkip
                }
                (State::PrefixSingle, WildcardToken::Literal(l), TopicToken::Literal(x)) => {
                    w.unlock();
                    if l == x {
                        State::Literal
                    } else {
                        return false;
                    }
                }
                (State::PrefixSingle, WildcardToken::None, TopicToken::Literal(_)) => State::Single,
                (State::PrefixSingleSkip, WildcardToken::Single, TopicToken::Literal(_)) => {
                    w.unlock();
                    State::Single
                }
                (State::PrefixSingleSkip, WildcardToken::Literal(l), TopicToken::Literal(x)) => {
                    w.unlock();
                    if l == x {
                        State::Literal
                    } else {
                        return false;
                    }
                }
                (State::PrefixSingleSkip, WildcardToken::Any, TopicToken::Literal(_)) => {
                    w.unlock();
                    State::Any
                }
                (State::Prefix, WildcardToken::Literal(l), TopicToken::Literal(x)) => {
                    if l == x {
                        State::Literal
                    } else {
                        return false;
                    }
                }
                (State::Prefix, WildcardToken::Single, TopicToken::Literal(_)) => State::Single,
                (State::Initial, WildcardToken::Literal(l), TopicToken::Literal(x)) => {
                    if l == x {
                        State::Literal
                    } else {
                        return false;
                    }
                }
                (State::Literal, WildcardToken::Literal(l), TopicToken::Literal(x)) => {
                    if l == x {
                        State::Literal
                    } else {
                        return false;
                    }
                }
                (State::Literal, WildcardToken::Separator, TopicToken::Separator) => {
                    State::Separator
                }
                (State::Literal, WildcardToken::Separator, TopicToken::None) => State::MaybeSkip,
                (State::MaybeSkip, WildcardToken::Any, TopicToken::Literal(_)) => State::Any,
                (State::MaybeSkip, WildcardToken::Any, TopicToken::None) => State::Any,
                (State::Separator, WildcardToken::Literal(l), TopicToken::Literal(x)) => {
                    if l == x {
                        State::Literal
                    } else {
                        return false;
                    }
                }
                (State::Separator, WildcardToken::Any, TopicToken::Literal(_)) => State::Any,
                (State::Separator, WildcardToken::Single, TopicToken::Literal(_)) => {
                    w.lock();
                    State::Single
                }
                (State::Single, WildcardToken::None, TopicToken::Literal(_)) => State::Single,
                // dirty hack to avoid implementing a proper TM tape
                (State::Single, WildcardToken::Separator, TopicToken::Separator) => {
                    w.unlock();
                    State::Separator
                }
                (State::Single, WildcardToken::Separator, TopicToken::Literal(_)) => State::Single,
                (State::PrefixAny, WildcardToken::None, TopicToken::Literal(_)) => State::Any,
                (State::Any, WildcardToken::None, TopicToken::Literal(_)) => State::Any,
                (State::Any, WildcardToken::None, TopicToken::Separator) => State::Separator,
                (State::Separator, WildcardToken::None, TopicToken::Literal(_)) => State::Any,
                (State::PrefixAny, WildcardToken::None, TopicToken::None) => State::Separator,

                // transitions to terminal
                (State::Any, WildcardToken::None, TopicToken::None) => State::Terminal,
                (State::Single, WildcardToken::None, TopicToken::None) => State::Terminal,
                (State::Literal, WildcardToken::None, TopicToken::None) => State::Terminal,

                (_, _, _) => return false,
            };
            if state == State::Terminal {
                println!(
                    "\n\nMATCHING TOPIC to wildcard: {} | {} | {:?}",
                    wildcard, topic, state
                );
                return true;
            }
        }
        println!(
            "\n\nMATCHING TOPIC to wildcard: {} | {} | {:?}",
            wildcard, topic, false
        );
        false
    }
}

impl TopicTree {
    pub fn ensure_topic_queue(&mut self, topic: &str) -> String {
        if !self.queues.contains_key(topic) {
            let id = self.counter;
            self.counter += 1;
            let queue = Queue {
                id,
                name: topic.to_string(),
                subscribers: Vec::new(),
            };
            self.queues.insert(topic.to_string(), queue);
            // add subscribers with matching topics/wildcards
        }
        topic.to_string()
    }

    pub fn get_by_name(&mut self, topic: String) -> Queue {
        println!("[TopicTree] getting by topic {}", topic);
        self.ensure_topic_queue(&topic);
        self.queues.get(&topic).unwrap().clone()
    }

    pub fn get_by_id(&mut self, queue_id: u128) -> &mut Queue {
        println!("[TopicTree] getting by id {}", queue_id);
        self.queues
            .values_mut()
            .filter(|q| q.id == queue_id)
            .next()
            .unwrap()
    }

    pub fn get_matching_queue_names(&mut self, topic_pattern: &str) -> Vec<String> {
        if topic_pattern.contains(&['+', '#'][..]) {
            return self.match_wildcard_subscriptions(topic_pattern);
        }
        vec![self.ensure_topic_queue(topic_pattern)]
    }

    pub fn match_wildcard_subscriptions(&mut self, wildcard: &str) -> Vec<String> {
        self.queues
            .iter()
            .filter_map(|(_, q)| {
                if State::match_topic(wildcard, &q.name) {
                    Some(q.name.clone())
                } else {
                    None
                }
            })
            .collect()
        // keys.iter()
        //     .map(|key| self.ensure_topic_queue(key))
        //     .collect::<Vec<Queue>>()
    }

    pub fn add_subscriptions(&mut self, topic: String, writer: ProcessRef<WriterProcess>) {
        for q in self.get_matching_queue_names(&topic) {
            self.queues
                .get_mut(&q)
                .unwrap()
                .subscribers
                .push(writer.clone());
        }
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
        assert_eq!(State::match_topic(subscription_matcher, "finance"), true);
        assert_eq!(State::match_topic(subscription_matcher, "finances"), false);
        assert_eq!(State::match_topic(subscription_matcher, "finance/"), false);
        assert_eq!(State::match_topic(subscription_matcher, "/finance"), false);
        assert_eq!(
            State::match_topic(prefix_subscription_matcher, "/finance"),
            true
        );
    }

    #[test]
    fn single_level_wildcard() {
        let subscription_matcher = "finance/+";
        assert_eq!(
            State::match_topic(subscription_matcher, "finance/stocks"),
            true
        );
        assert_eq!(
            State::match_topic(subscription_matcher, "finance/commodities"),
            true
        );
        // should not match these
        assert_eq!(
            State::match_topic(subscription_matcher, "finance/commodities/oil"),
            false
        );
        assert_eq!(
            State::match_topic(subscription_matcher, "/finance/stocks"),
            false
        );
        assert_eq!(State::match_topic(subscription_matcher, "finance"), false);
    }

    #[test]
    fn complex_cases() {
        let subscription_matcher = "users/+/device/+/permissions/#";
        assert!(State::match_topic(
            subscription_matcher,
            "users/john_123/device/samsung_galaxy/permissions"
        ));
        assert!(State::match_topic(
            subscription_matcher,
            "users/john_123/device/samsung galaxy/permissions/account/can_delete"
        ));
        assert!(State::match_topic(
            subscription_matcher,
            "users/john_123/device/samsung_galaxy/permissions/is_admin!!"
        ));
        // should not match these
        assert_eq!(
            State::match_topic(
                subscription_matcher,
                "users/john_123/device/samsung_galaxy/permission"
            ),
            false
        );
        assert_eq!(
            State::match_topic(
                subscription_matcher,
                "users/john_123/4/device/samsung_galaxy/permissions/is_admin!!"
            ),
            false
        );
        assert_eq!(
            State::match_topic(
                subscription_matcher,
                "users/john_123/device/permissions/is_admin!!"
            ),
            false
        );
    }

    #[test]
    fn special_cases() {
        let subscription_matcher = "#";
        assert!(State::match_topic(
            subscription_matcher,
            "users/john_123/device/samsung_galaxy/permissions"
        ));
        assert!(State::match_topic(subscription_matcher, "users"));
        assert!(State::match_topic(subscription_matcher, "/users"));
        // handle +
        let subscription_matcher = "+";
        assert_eq!(State::match_topic(subscription_matcher, "users"), true);
        assert_eq!(
            State::match_topic(subscription_matcher, "users/john_123"),
            false
        );
        // handle +/+
        let subscription_matcher = "+/+";
        assert!(State::match_topic(subscription_matcher, "users/bob"));
        assert!(State::match_topic(subscription_matcher, "/users"));
        // handle /+
        let subscription_matcher = "/+";
        assert!(State::match_topic(subscription_matcher, "/users"));
        assert_eq!(State::match_topic(subscription_matcher, "users"), false);
        // handle other combinations
        assert!(State::match_topic("+/#", "users/bob"));
        assert!(State::match_topic("+/bob/#", "users/bob"));
        // not sure if this should even match
        // assert!(State::match_topic("+/bob/+", "/users/bob/x"));
    }

    #[test]
    fn invalid_wildcards() {
        assert_eq!(State::match_topic("//+", "finance"), false);
        assert_eq!(State::match_topic("#+", "finances"), false);
        assert_eq!(State::match_topic("#/+", "finance"), false);
        assert_eq!(State::match_topic("+/#e", "finance"), false);
        assert_eq!(State::match_topic("#/#", "/finance/other"), false);
        assert_eq!(State::match_topic("/finance/+/", "/finance/hello"), false);
    }

    #[test]
    fn invalid_topics() {
        assert_eq!(State::match_topic("#", "++"), false);
        assert_eq!(State::match_topic("#", "#/#"), false);
        assert_eq!(State::match_topic("#", "finance/"), false);
        assert_eq!(State::match_topic("#", "/finance/"), false);
        assert_eq!(State::match_topic("#", "/finance/hello+"), false);
    }
}
