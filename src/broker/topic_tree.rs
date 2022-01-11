use crate::queue::queue::new_queue;
use crate::structure::*;
use lunatic::process;
use std::cell::RefCell;
use std::collections::HashMap;
use std::mem;
use std::rc::Rc;

#[derive(Default)]
pub struct TopicTree {
    queues: Vec<Queue>,
    wildcard_subscriptions: Vec<WildcardSubscription>,
}

pub struct WildcardSubscription {
    matcher: Rc<RefCell<State>>,
    sub: Subscription,
}

#[derive(Debug, PartialEq, Clone)]
pub struct State {
    // id: u32,
    transitions: HashMap<StateTransition, Rc<RefCell<State>>>,
    is_terminal: bool,
}

impl Default for State {
    fn default() -> State {
        State::new()
    }
}

// TODO: add UTF-8 support to topic names/patterns
// TODO: reject message if Unicode \x0000 is encountered
// TODO: reject invalid patterns (e.g. hello+)
// TODO: maybe use an array-based automata structure?
impl State {
    pub fn new() -> State {
        // let id = counter;
        // counter += 1;
        State {
            transitions: HashMap::new(),
            is_terminal: false,
        }
    }

    pub fn new_wrapped() -> Rc<RefCell<State>> {
        // let id = counter;
        // counter += 1;
        Rc::new(RefCell::new(State::new()))
    }

    pub fn from_topic(topic: String) -> Rc<RefCell<State>> {
        let initial = State::new_wrapped();
        let mut curr = Rc::clone(&initial);
        let mut prev = Rc::clone(&initial);
        let mut is_first = true;
        for s in topic.as_bytes() {
            if *s == b'+' {
                let mut c = curr.borrow_mut();
                let state = c
                    .transitions
                    .entry(StateTransition::Not(b'/'))
                    .or_insert_with(State::new_wrapped);
                // println!("\n\nSTATE POINTING TO &STATE -> {:?}\n\n", state);
                state
                    .borrow_mut()
                    .transitions
                    .insert(StateTransition::Not(b'/'), Rc::clone(state));
                let state = Rc::clone(state);
                mem::drop(c);
                prev = curr;
                curr = Rc::clone(&state);
            } else if *s == b'#' {
                if !is_first {
                    let mut p = prev.borrow_mut();
                    p.is_terminal = true;
                }
                let mut c = curr.borrow_mut();

                let state = c
                    .transitions
                    .entry(StateTransition::Any)
                    .or_insert_with(State::new_wrapped);
                state
                    .borrow_mut()
                    .transitions
                    .insert(StateTransition::Any, Rc::clone(state));
                state.borrow_mut().is_terminal = true;
                let state = Rc::clone(state);
                mem::drop(c);
                prev = curr;
                curr = Rc::clone(&state);
            } else {
                let state = State::new_wrapped();
                curr.borrow_mut()
                    .transitions
                    .insert(StateTransition::Literal(*s), Rc::clone(&state));
                prev = curr;
                curr = state;
            }
            is_first = false;
        }
        // println!("AFTER CREATION: {} {:?} | {:?}", topic, curr, prev);
        if topic == "+/+" {
            let mut init = initial.borrow_mut();
            let slash_state = {
                let x = init
                    .transitions
                    .get(&StateTransition::Not(b'/'))
                    .unwrap()
                    .borrow();
                Rc::clone(x.transitions.get(&StateTransition::Literal(b'/')).unwrap())
            };
            init.transitions
                .entry(StateTransition::Literal(b'/'))
                .or_insert(slash_state);
        }
        curr.borrow_mut().is_terminal = true;
        initial
    }

    pub fn matches_topic(initial: Rc<RefCell<State>>, topic: String) -> bool {
        let mut curr = Rc::clone(&initial);
        for c in topic.as_bytes() {
            curr = {
                let t = curr.borrow();
                // println!("Matching topic: {} | {:?}", c, t.transitions.keys());
                if let Some(trans) = t.transitions.get(&StateTransition::Literal(*c)) {
                    Rc::clone(trans)
                } else if let Some(trans) = t.transitions.get(&StateTransition::Any) {
                    Rc::clone(trans)
                } else if let Some(trans) = t.transitions.get(&StateTransition::Not(b'/')) {
                    if *c == b'/' {
                        return false;
                    }
                    Rc::clone(trans)
                } else {
                    return false;
                }
            }
        }
        let b = curr.borrow();
        b.is_terminal
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum StateTransition {
    Literal(u8),
    Any,
    Not(u8),
}

impl TopicTree {
    pub fn new() -> TopicTree {
        TopicTree {
            queues: Vec::new(),
            wildcard_subscriptions: Vec::new(),
        }
    }

    pub fn ensure_topic_queue(&mut self, topic: String) -> Queue {
        if let Some(found) = self.queues.iter().position(|q| q.name == topic) {
            self.queues.get(found).unwrap().clone()
        } else {
            let queue = Queue {
                name: topic.clone(),
                process: process::spawn_with(topic, new_queue).unwrap(),
            };
            self.queues.push(queue);
            let len = self.queues.len();
            self.queues.get(len - 1).unwrap().clone()
        }
    }

    pub fn get_matched_queues(&mut self, topic_pattern: String, sub: Subscription) -> Vec<Queue> {
        if topic_pattern.contains(&['+', '#'][..]) {
            return self.match_wildcard_subscriptions(topic_pattern, sub);
        }
        vec![self.ensure_topic_queue(topic_pattern)]
    }

    // pub fn get_subscribers(&self, topic: String) -> Vec<Subscription> {
    //     self.queues
    //         .iter()
    //         .flat_map(|queue| {
    //             if queue.name == topic {
    //                 return queue.subscribers.clone();
    //             }
    //             self.wildcard_subscriptions
    //                 .iter()
    //                 .filter(|wildcard| {
    //                     State::matches_topic(Rc::clone(&wildcard.matcher), topic.clone())
    //                 })
    //                 .map(|wildcard| wildcard.sub.clone())
    //                 .collect::<Vec<Subscription>>()
    //             // None
    //         })
    //         .collect()
    // }

    pub fn match_wildcard_subscriptions(
        &mut self,
        topic_pattern: String,
        sub: Subscription,
    ) -> Vec<Queue> {
        let wildcard = State::from_topic(topic_pattern);
        self.wildcard_subscriptions.push(WildcardSubscription {
            matcher: Rc::clone(&wildcard),
            sub: sub.clone(),
        });
        let keys: Vec<String> = self
            .queues
            .iter()
            .filter_map(|q| {
                if State::matches_topic(Rc::clone(&wildcard), q.name.clone()) {
                    Some(q.name.clone())
                } else {
                    None
                }
            })
            .collect();
        keys.iter()
            .map(|key| self.ensure_topic_queue(key.to_string()))
            .collect::<Vec<Queue>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Matching subscriptions with literals and wildcards
    #[test]
    fn literal_subscription() {
        let subscription_matcher = State::from_topic("finance".to_string());
        let prefix_subscription_matcher = State::from_topic("/finance".to_string());
        assert_eq!(
            State::matches_topic(subscription_matcher.clone(), "finance".to_string()),
            true
        );
        assert_eq!(
            State::matches_topic(subscription_matcher.clone(), "finances".to_string()),
            false
        );
        assert_eq!(
            State::matches_topic(subscription_matcher.clone(), "finance/".to_string()),
            false
        );
        assert_eq!(
            State::matches_topic(subscription_matcher, "/finance".to_string()),
            false
        );
        assert_eq!(
            State::matches_topic(prefix_subscription_matcher, "/finance".to_string()),
            true
        );
    }

    #[test]
    fn single_level_wildcard() {
        let subscription_matcher = State::from_topic("finance/+".to_string());
        assert_eq!(
            State::matches_topic(subscription_matcher.clone(), "finance/stocks".to_string()),
            true
        );
        assert_eq!(
            State::matches_topic(
                subscription_matcher.clone(),
                "finance/commodities".to_string()
            ),
            true
        );
        // should not match these
        assert_eq!(
            State::matches_topic(
                subscription_matcher.clone(),
                "finance/commodities/oil".to_string()
            ),
            false
        );
        assert_eq!(
            State::matches_topic(subscription_matcher.clone(), "/finance/stocks".to_string()),
            false
        );
        assert_eq!(
            State::matches_topic(subscription_matcher, "finance".to_string()),
            false
        );
    }

    #[test]
    fn complex_cases() {
        let subscription_matcher = State::from_topic("users/+/device/+/permissions/#".to_string());
        assert!(State::matches_topic(
            subscription_matcher.clone(),
            "users/john_123/device/samsung_galaxy/permissions".to_string()
        ));
        assert!(State::matches_topic(
            subscription_matcher.clone(),
            "users/john_123/device/samsung galaxy/permissions/account/can_delete".to_string()
        ));
        assert!(State::matches_topic(
            subscription_matcher.clone(),
            "users/john_123/device/samsung_galaxy/permissions/is_admin!!".to_string()
        ));
        // should not match these
        assert_eq!(
            State::matches_topic(
                subscription_matcher.clone(),
                "users/john_123/device/samsung_galaxy/permission".to_string()
            ),
            false
        );
        assert_eq!(
            State::matches_topic(
                subscription_matcher.clone(),
                "users/john_123/4/device/samsung_galaxy/permissions/is_admin!!".to_string()
            ),
            false
        );
        assert_eq!(
            State::matches_topic(
                subscription_matcher,
                "users/john_123/device/permissions/is_admin!!".to_string()
            ),
            false
        );
    }

    #[test]
    fn special_cases() {
        let subscription_matcher = State::from_topic("#".to_string());
        assert!(State::matches_topic(
            subscription_matcher.clone(),
            "users/john_123/device/samsung_galaxy/permissions".to_string()
        ));
        assert!(State::matches_topic(
            subscription_matcher.clone(),
            "users".to_string()
        ));
        assert!(State::matches_topic(
            subscription_matcher,
            "/users".to_string()
        ));
        // handle +
        let subscription_matcher = State::from_topic("+".to_string());
        assert!(State::matches_topic(
            subscription_matcher.clone(),
            "users".to_string()
        ));
        assert_eq!(
            State::matches_topic(subscription_matcher, "users/john_123".to_string()),
            false
        );
        // handle +/+
        let subscription_matcher = State::from_topic("+/+".to_string());
        assert!(State::matches_topic(
            subscription_matcher.clone(),
            "users/bob".to_string()
        ));
        assert!(State::matches_topic(
            subscription_matcher,
            "/users".to_string()
        ));
        // handle /+
        let subscription_matcher = State::from_topic("/+".to_string());
        assert!(State::matches_topic(
            subscription_matcher.clone(),
            "/users".to_string()
        ));
        assert_eq!(
            State::matches_topic(subscription_matcher, "users".to_string()),
            false
        );
    }
}
