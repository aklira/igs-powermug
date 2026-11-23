//! The transition table of a command (SPECIFICATIONS 9.1, [GW] 11.3): checked on every pair
//! of states, so that no transition is taken that the table does not name.

use std::collections::BTreeSet;

use powermug_command::{transitions, State};

#[test]
fn every_pair_of_states_is_allowed_exactly_when_the_table_names_it() {
    for from in State::ALL {
        for to in State::ALL {
            assert_eq!(
                from.may_move_to(to),
                transitions(from).contains(&to),
                "{} -> {}",
                from.code(),
                to.code()
            );
        }
    }
}

#[test]
fn terminal_states_are_absorbing() {
    for state in State::ALL {
        if state.is_terminal() {
            assert!(transitions(state).is_empty(), "{} leaves", state.code());
            for next in State::ALL {
                assert!(
                    !state.may_move_to(next),
                    "{} -> {}",
                    state.code(),
                    next.code()
                );
            }
        }
    }
}

#[test]
fn no_state_moves_to_itself() {
    for state in State::ALL {
        assert!(!state.may_move_to(state), "{} loops", state.code());
    }
}

#[test]
fn every_state_is_reachable_from_received() {
    let mut reached = BTreeSet::from([State::Received]);
    let mut frontier = vec![State::Received];
    while let Some(state) = frontier.pop() {
        for next in transitions(state) {
            if reached.insert(*next) {
                frontier.push(*next);
            }
        }
    }
    assert_eq!(
        reached.len(),
        State::ALL.len(),
        "unreachable: {:?}",
        State::ALL
            .iter()
            .filter(|s| !reached.contains(s))
            .collect::<Vec<_>>()
    );
}

#[test]
fn codes_round_trip_and_are_unique() {
    let codes: BTreeSet<&str> = State::ALL.iter().map(|s| s.code()).collect();
    assert_eq!(codes.len(), State::ALL.len());
    for state in State::ALL {
        assert_eq!(State::from_code(state.code()), Some(state));
    }
    assert_eq!(State::from_code("NOT_A_STATE"), None);
}
