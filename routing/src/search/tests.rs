use super::*;
use crate::network::NetworkInput;

/// Two identical timetabled trips: first incidence entry must win exact ties.
fn input() -> NetworkInput {
    NetworkInput {
        nstops: 2,
        late_base: vec![100, 200, 100, 200],
        expected: vec![100, 200, 100, 200],
        depart: vec![100, 200, 100, 200],
        range_ids: vec![-1; 4],
        trip_rows: vec![0, 2],
        trip_pattern: vec![0, 0],
        cumulative: vec![0.0, 100.0, 0.0, 100.0],
        patterns: vec![vec![0, 1]],
        pattern_alights: vec![vec![0, 1]],
        pattern_prefix: vec![vec![0.0, 100.0]],
        incidence: vec![vec![(0, 0, vec![100, 100], vec![0, 1])], vec![]],
        footpaths: vec![vec![], vec![]],
        envelopes: vec![],
    }
}
fn query(data: NetworkInput, budget: usize) -> Arc<Query> {
    Arc::new(Query::new(Arc::new(Network::new(data).unwrap()), vec![0.0; 2], budget).unwrap())
}
fn state_input() -> StateInput {
    StateInput {
        origins: vec![0],
        access: vec![],
        origin_walks: vec![],
        targets: vec![(1, -1)],
        has_egress: false,
        horizon_seconds: 10800,
    }
}
fn state() -> State {
    State::new(query(input(), 4), state_input()).unwrap()
}

#[test]
fn bounds_are_strict_and_nonincreasing_in_vehicle_count() {
    let mut b = Bounds::default();
    b.lower(2, 100);
    b.lower(4, 90);
    b.lower(0, 95);
    assert_eq!(b.0, [95, 95, 95, 95, 90, 90]);
    b.lower(0, 95);
    assert_eq!(b.0, [95, 95, 95, 95, 90, 90]);
}

#[test]
fn ordered_replacement_does_not_move_an_entry() {
    let mut entries = Ordered::default();
    entries.set(0, -1, 0);
    entries.set(1, -1, 1);
    entries.set(0, -1, 2);
    assert_eq!(
        entries
            .entries
            .iter()
            .map(|e| (e.stop, e.label))
            .collect::<Vec<_>>(),
        [(0, 2), (1, 1)]
    );
}

#[test]
fn ties_and_predecessors_survive_later_runs_without_arena() {
    let mut state = state();
    let paths = state.run(100, None).unwrap();
    assert_eq!(
        paths,
        vec![vec![
            (100, 0, -1, -1, -1, -1, -1, 0),
            (200, 1, 0, 0, 1, -1, -1, 0)
        ]]
    );
    assert!(state.labels.is_empty());
    for after in (0..100).rev() {
        assert!(state.run(after, None).unwrap().is_empty());
    }
    assert_eq!(paths[0][1].2, 0);
    assert!(state.peak_labels > 0);
    assert_eq!(state.stats()[3].1, 0);
}

#[test]
fn latest_departure_must_include_boarding_and_horizon_is_strict() {
    assert!(!state().run(100, None).unwrap().is_empty());
    assert!(state().run(101, None).unwrap().is_empty());
    for (horizon, count) in [(99, 0), (100, 0), (101, 1)] {
        let mut data = state_input();
        data.horizon_seconds = horizon;
        assert_eq!(
            State::new(query(input(), 0), data)
                .unwrap()
                .run(100, None)
                .unwrap()
                .len(),
            count
        );
    }
    assert!(state().run(0, Some(&[])).unwrap().is_empty());
}

#[test]
fn time_addition_and_late_conversion_are_checked() {
    assert_eq!(
        add(i64::MAX, 1),
        Err(Error::Overflow("time addition overflow"))
    );
    assert_eq!(
        add(i64::MIN, -1),
        Err(Error::Overflow("time addition overflow"))
    );
    assert_eq!(add(i64::MIN, i64::MAX), Ok(-1));
    let mut data = input();
    data.late_base[0] = 0;
    data.expected[1] = 0;
    data.depart[0] = 0;
    data.cumulative[1] = 9223372036854775808.0;
    let net = Network::new(data).unwrap();
    assert!(matches!(net.late(0, 1), Err(Error::Overflow(_))));
    assert!(matches!(net.late(-1, 1), Err(Error::Index(_))));
    assert!(matches!(net.late(0, 4), Err(Error::Index(_))));
    let mut data = input();
    data.late_base[0] = 0;
    data.expected[1] = 0;
    data.depart[0] = 0;
    // The largest float BELOW 2^63 must not be rejected as though it were 2^63.
    data.cumulative[1] = f64::from_bits(9223372036854775808.0_f64.to_bits() - 1);
    assert_eq!(Network::new(data).unwrap().late(0, 1), Ok(i64::MAX - 1023));
    let mut data = input();
    data.cumulative[1] = f64::MAX;
    data.range_ids[0] = 0;
    data.envelopes = vec![(vec![f64::INFINITY], vec![2.0], vec![0.0])];
    assert!(matches!(
        Network::new(data).unwrap().late(0, 1),
        Err(Error::Overflow(_))
    ));
}

#[test]
fn ieee_ceil_and_envelope_bucket_boundaries_are_exact() {
    for (duration, expected) in [
        (100.0, 300),
        (f64::from_bits(100.0_f64.to_bits() + 1), 300),
        (201.0, 301),
    ] {
        let mut data = input();
        data.cumulative[1] = duration;
        data.range_ids[0] = 0;
        data.envelopes = vec![(vec![100.0, f64::INFINITY], vec![2.0, 1.0], vec![0.0, 200.0])];
        assert_eq!(Network::new(data).unwrap().late(0, 1), Ok(expected));
    }
    let mut data = input();
    data.cumulative[1] = 100.0000001;
    assert_eq!(Network::new(data).unwrap().late(0, 1), Ok(201));
    let mut data = input();
    data.cumulative[1] = -10.0;
    assert_eq!(Network::new(data).unwrap().late(0, 1), Ok(200));
    // Separate Python operations ceil to zero; contracting into FMA would ceil to one.
    let duration: f64 = 1000.0 / 1.2;
    assert_eq!((duration * 1.2 - 1000.0).ceil(), 0.0);
    assert_eq!(duration.mul_add(1.2, -1000.0).ceil(), 1.0);
    let mut data = input();
    data.late_base[0] = -1000;
    data.expected[1] = 0;
    data.depart[0] = 0;
    data.cumulative[1] = duration;
    data.range_ids[0] = 0;
    data.envelopes = vec![(vec![f64::INFINITY], vec![1.2], vec![0.0])];
    assert_eq!(Network::new(data).unwrap().late(0, 1), Ok(0));
}

#[test]
fn failure_clears_arena_and_invalidates_bounds() {
    let mut data = state_input();
    data.origin_walks = vec![(0, 1, i64::MAX)];
    let mut s = State::new(query(input(), 0), data).unwrap();
    assert!(matches!(s.run(100, None), Err(Error::Overflow(_))));
    assert!(s.labels.is_empty());
    assert!(s.peak_labels > 0);
    assert!(matches!(s.run(0, None), Err(Error::Runtime(_))));
    let mut s = state();
    assert!(matches!(s.run(1_i64 << 40, None), Err(Error::Value(_))));
    assert!(matches!(s.run(0, None), Err(Error::Runtime(_))));
    let mut s = state();
    assert!(matches!(s.run(0, Some(&[-1])), Err(Error::Index(_))));
    assert!(s.labels.is_empty());
    let mut s = state();
    s.invalidate();
    assert!(matches!(s.run(0, None), Err(Error::Runtime(_))));
}

#[test]
fn all_walk_kinds_use_owned_paths_and_checked_egress_addition() {
    let mut data = state_input();
    data.origins.clear();
    data.access = vec![(0, 30)];
    data.targets = vec![(1, 40)];
    data.has_egress = true;
    let mut s = State::new(query(input(), 0), data).unwrap();
    let paths = s.run(70, None).unwrap();
    assert_eq!(
        paths[0].iter().map(|r| (r.0, r.1)).collect::<Vec<_>>(),
        [(70, 0), (100, 3), (200, 1), (240, 4)]
    );
    let mut data = state_input();
    data.targets = vec![(1, i64::MAX)];
    let mut s = State::new(query(input(), 0), data).unwrap();
    assert!(matches!(s.run(0, None), Err(Error::Overflow(_))));
    assert!(s.labels.is_empty());
    let mut data = state_input();
    data.origins.clear();
    data.origin_walks = vec![(1, 0, 30)];
    let mut s = State::new(query(input(), 0), data).unwrap();
    assert_eq!(
        s.run(70, None).unwrap()[0][1],
        (100, 2, -1, -1, -1, 1, 0, 30)
    );
}

#[test]
fn equivalent_walk_winner_keeps_its_own_insertion_position() {
    let mut s = state();
    let a = s.label(200, None, (0, -1, -1, -1, -1, -1, 0));
    let b = s.label(100, None, (0, -1, -1, -1, -1, -1, 0));
    let c = s.label(100, None, (0, -1, -1, -1, -1, -1, 0));
    let mut entries = Ordered::default();
    entries.set(1, 0, a);
    entries.set(0, UNRESTRICTED, b);
    entries.set(1, 1, c);
    let result = s.prune(entries, &mut Permissions::default());
    assert_eq!(result.iter().map(|e| e.label).collect::<Vec<_>>(), [b, c]);
    let mut entries = Ordered::default();
    entries.set(1, 0, b);
    entries.set(1, 1, c);
    let result = s.prune(entries, &mut Permissions::default());
    assert_eq!(result.iter().map(|e| e.label).collect::<Vec<_>>(), [b]);
}

#[test]
fn permissions_are_multiword_bounded_and_fifo_evicted() {
    let mut data = input();
    data.incidence[0] = vec![(0, 0, vec![100], vec![0]); 65];
    for budget in [0, 1, 2, 3, usize::MAX] {
        let q = query(
            NetworkInput {
                incidence: data.incidence.clone(),
                ..input()
            },
            budget,
        );
        let mut cache = Permissions::default();
        assert_eq!(q.mask(&mut cache, 0, 0), [u64::MAX, 1]);
        assert_eq!(q.mask(&mut cache, 0, 1), [u64::MAX, 1]);
        assert!(q.peak_permission_words() <= budget);
        if budget < 2 {
            assert_eq!(cache.words, 0);
        }
        if budget == 2 || budget == 3 {
            assert!(!cache.entries.contains_key(&(0, 0)));
            assert!(cache.entries.contains_key(&(0, 1)));
            assert_eq!(cache.words, 2);
        }
        assert_eq!(
            State::new(q, state_input()).unwrap().run(0, None).unwrap()[0][1].2,
            0
        );
    }
}

#[test]
fn first_board_keeps_first_incidence_at_repeated_stop() {
    let mut data = input();
    data.patterns = vec![vec![0, 1, 0, 1]];
    data.pattern_alights = vec![vec![1, 3]];
    data.pattern_prefix = vec![vec![0.0, 100.0, 200.0, 300.0]];
    data.trip_rows = vec![0];
    data.trip_pattern = vec![0];
    data.incidence[0] = vec![(0, 0, vec![100], vec![0]), (0, 2, vec![100], vec![0])];
    let net = Network::new(data).unwrap();
    assert_eq!(net.board_position(0, 0, 99), 0);
    assert_eq!(net.board_position(1, 0, 99), 99);
}

#[test]
fn malformed_networks_queries_and_states_return_errors() {
    let mut cases: Vec<NetworkInput> = Vec::new();
    let mut data = input();
    data.expected.pop();
    cases.push(data);
    let mut data = input();
    data.cumulative[0] = f64::NAN;
    cases.push(data);
    let mut data = input();
    data.trip_pattern[0] = -1;
    cases.push(data);
    let mut data = input();
    data.trip_rows[0] = i32::MAX;
    cases.push(data);
    let mut data = input();
    data.pattern_alights.pop();
    cases.push(data);
    let mut data = input();
    data.pattern_prefix[0][1] = -1.0;
    cases.push(data);
    let mut data = input();
    data.pattern_alights[0].reverse();
    cases.push(data);
    let mut data = input();
    data.patterns[0][0] = -1;
    cases.push(data);
    let mut data = input();
    data.incidence[0][0].3.pop();
    cases.push(data);
    let mut data = input();
    data.incidence[0][0].3[0] = 99;
    cases.push(data);
    let mut data = input();
    data.incidence[0][0].0 = 99;
    cases.push(data);
    let mut data = input();
    data.incidence[0][0].1 = 1;
    cases.push(data);
    let mut data = input();
    data.incidence[0][0].2 = vec![101, 100];
    cases.push(data);
    let mut data = input();
    data.incidence.pop();
    cases.push(data);
    let mut data = input();
    data.footpaths[0] = vec![(1, -1)];
    cases.push(data);
    let mut data = input();
    data.range_ids[0] = 0;
    cases.push(data);
    let mut data = input();
    data.envelopes = vec![(vec![0.0], vec![1.0], vec![0.0])];
    cases.push(data);
    let mut data = input();
    data.envelopes = vec![(vec![f64::INFINITY], vec![f64::INFINITY], vec![0.0])];
    cases.push(data);
    for case in cases {
        assert!(Network::new(case).is_err());
    }
    for target in [vec![0.0], vec![-1.0, 0.0], vec![f64::NAN, 0.0]] {
        assert!(Query::new(Arc::new(Network::new(input()).unwrap()), target, 0).is_err());
    }
    assert!(
        Query::new(
            Arc::new(Network::new(input()).unwrap()),
            vec![f64::INFINITY, 0.0],
            0
        )
        .is_ok()
    );
    let mut data = state_input();
    data.origins = vec![-1];
    assert!(State::new(query(input(), 0), data).is_err());
    let mut data = state_input();
    data.targets = vec![(1, -2)];
    assert!(State::new(query(input(), 0), data).is_err());
    let mut data = state_input();
    data.horizon_seconds = -1;
    assert!(State::new(query(input(), 0), data).is_err());
}

#[test]
fn query_shared_across_concurrent_windows_not_numeric_bounds() {
    let q = query(input(), 2);
    let handles = (0..4)
        .map(|_| {
            let q = Arc::clone(&q);
            std::thread::spawn(move || {
                let mut s = State::new(q, state_input()).unwrap();
                let result = s.run(100, None).unwrap();
                assert!(s.run(0, None).unwrap().is_empty());
                result
            })
        })
        .collect::<Vec<_>>();
    for h in handles {
        assert_eq!(h.join().unwrap()[0][1].2, 0);
    }
    assert_eq!(
        State::new(q, state_input()).unwrap().run(0, None).unwrap()[0][1].0,
        200
    );
}
