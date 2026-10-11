//! The existing round search: persistent numeric bounds, call-local predecessor arena.
use crate::error::{Error, Result, add, index, require};
use crate::network::{Footpath, Incidence, Key, Network, OriginWalk};
use rustc_hash::FxHashMap;
use std::collections::VecDeque;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub const INF: i64 = 2147483647;
pub const COUNTS: usize = 6;
const UNRESTRICTED: i32 = -2;

#[derive(Debug, Clone)]
struct Bounds([i64; COUNTS]);
impl Default for Bounds {
    fn default() -> Self {
        Self([INF; COUNTS])
    }
}
impl Bounds {
    fn at(&self, count: usize) -> i64 {
        self.0[count]
    }
    fn lower(&mut self, count: usize, time: i64) {
        for value in &mut self.0[count..] {
            if time >= *value {
                break;
            }
            *value = time;
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Marked {
    stop: usize,
    source: i32,
    label: usize,
}
#[derive(Default)]
struct Ordered {
    entries: Vec<Marked>,
    index: FxHashMap<Key, usize>,
}
impl Ordered {
    fn set(&mut self, stop: usize, source: i32, label: usize) {
        let next = self.entries.len();
        let slot = *self.index.entry((stop as i32, source)).or_insert(next);
        if slot == next {
            self.entries.push(Marked {
                stop,
                source,
                label,
            });
        } else {
            self.entries[slot].label = label;
        }
    }
}

#[derive(Default)]
struct Permissions {
    words: usize,
    entries: FxHashMap<Key, Vec<u64>>,
    fifo: VecDeque<Key>,
}

pub struct Query {
    pub network: Arc<Network>,
    to_target: Vec<f64>,
    budget: usize,
    permissions: Mutex<Permissions>,
    peak_words: AtomicUsize,
}
impl Query {
    pub fn new(network: Arc<Network>, to_target: Vec<f64>, budget: usize) -> Result<Self> {
        require(to_target.len() == network.nstops, "target bound length")?;
        require(
            to_target.iter().all(|&d| !d.is_nan() && d >= 0.0),
            "invalid target bound",
        )?;
        Ok(Self {
            network,
            to_target,
            budget,
            permissions: Mutex::default(),
            peak_words: AtomicUsize::new(0),
        })
    }
    pub fn peak_permission_words(&self) -> usize {
        self.peak_words.load(Ordering::Relaxed)
    }

    fn mask(&self, cache: &mut Permissions, stop: usize, source: i32) -> Vec<u64> {
        let key = (stop as i32, source);
        if let Some(value) = cache.entries.get(&key) {
            return value.clone();
        }
        let net = &self.network;
        let begin = net.incidence_offsets[stop];
        let end = net.incidence_offsets[stop + 1];
        let mut value = vec![0; (end - begin).div_ceil(64).max(1)];
        for (i, e) in net.incidences[begin..end].iter().enumerate() {
            if net.board_position(source, e.pattern, e.pos) >= e.pos {
                value[i / 64] |= 1_u64 << (i % 64);
            }
        }
        while value.len() > 1 && value.last() == Some(&0) {
            value.pop();
        }
        if value.len() <= self.budget {
            // Subtraction avoids overflowing words + value.len() for a usize::MAX budget.
            while cache.words > self.budget - value.len() {
                if let Some(old) = cache.fifo.pop_front() {
                    if let Some(evicted) = cache.entries.remove(&old) {
                        cache.words -= evicted.len();
                    }
                } else {
                    break;
                }
            }
            cache.entries.insert(key, value.clone());
            cache.fifo.push_back(key);
            cache.words += value.len();
            self.peak_words.fetch_max(cache.words, Ordering::Relaxed);
        }
        value
    }
}

/// Plain values returned at the Python boundary. Parent indices never escape the arena.
pub type Record = (i64, i32, i32, i32, i32, i32, i32, i64);
pub type Paths = Vec<Vec<Record>>;
#[derive(Debug, Clone, Copy)]
struct Label {
    record: Record,
    parent: Option<usize>,
}
impl Label {
    fn time(&self) -> i64 {
        self.record.0
    }
}

pub struct StateInput {
    pub origins: Vec<i32>,
    pub access: Vec<(i32, i64)>,
    pub origin_walks: Vec<(i32, i32, i64)>,
    pub targets: Vec<(i32, i64)>,
    pub has_egress: bool,
    pub horizon_seconds: i64,
}

pub struct State {
    pub query: Arc<Query>,
    best: Vec<Bounds>,
    ride: Vec<Bounds>,
    scans: Vec<Bounds>,
    target_best: Bounds,
    walking: FxHashMap<Key, Bounds>,
    origins: Vec<usize>,
    origin_board: Vec<usize>,
    access: Vec<Footpath>,
    origin_walks: Vec<OriginWalk>,
    targets: Vec<bool>,
    egress: Vec<i64>,
    has_egress: bool,
    horizon_seconds: i64,
    labels: Vec<Label>,
    failed: bool,
    peak_labels: usize,
    peak_label_capacity: usize,
    peak_walk_states: usize,
}
impl State {
    pub fn new(query: Arc<Query>, input: StateInput) -> Result<Self> {
        require(input.horizon_seconds >= 0, "negative journey horizon")?;
        let net = &query.network;
        let n = net.nstops;
        let mut origins = Vec::with_capacity(input.origins.len());
        let mut origin_board = vec![usize::MAX; net.patterns.len()];
        for stop in input.origins {
            let stop = index(stop, n, "origin stop index")?;
            origins.push(stop);
            for e in &net.incidences[net.incidence_offsets[stop]..net.incidence_offsets[stop + 1]] {
                origin_board[e.pattern] =
                    origin_board[e.pattern].min(net.board_position(stop as i32, e.pattern, e.pos));
            }
        }
        let access = input
            .access
            .into_iter()
            .map(|(dest, seconds)| {
                let dest = index(dest, n, "access stop index")?;
                require(seconds >= 0, "negative access walk")?;
                Ok(Footpath { dest, seconds })
            })
            .collect::<Result<Vec<_>>>()?;
        let origin_walks = input
            .origin_walks
            .into_iter()
            .map(|(source, dest, seconds)| {
                let source = index(source, n, "initial walk source index")?;
                let dest = index(dest, n, "initial walk destination index")?;
                require(seconds >= 0, "negative initial walk")?;
                Ok(OriginWalk {
                    source,
                    dest,
                    seconds,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut targets = vec![false; n];
        let mut egress = vec![-1; n];
        for (dest, seconds) in input.targets {
            let dest = index(dest, n, "target stop index")?;
            require(seconds >= -1, "invalid egress")?;
            targets[dest] = true;
            egress[dest] = seconds;
        }
        let best = vec![Bounds::default(); n];
        let ride = best.clone();
        let scans = vec![Bounds::default(); net.incidences.len()];
        Ok(Self {
            query,
            best,
            ride,
            scans,
            target_best: Bounds::default(),
            walking: FxHashMap::default(),
            origins,
            origin_board,
            access,
            origin_walks,
            targets,
            egress,
            has_egress: input.has_egress,
            horizon_seconds: input.horizon_seconds,
            labels: Vec::new(),
            failed: false,
            peak_labels: 0,
            peak_label_capacity: 0,
            peak_walk_states: 0,
        })
    }
    fn walk_bound(&self, key: Key, count: usize) -> i64 {
        self.walking.get(&key).map_or(INF, |b| b.at(count))
    }
    fn lower_walk(&mut self, key: Key, count: usize, time: i64) {
        self.walking.entry(key).or_default().lower(count, time);
    }
    fn label(
        &mut self,
        time: i64,
        parent: Option<usize>,
        leg: (i32, i32, i32, i32, i32, i32, i64),
    ) -> usize {
        let (kind, trip, board, alight, source, dest, seconds) = leg;
        self.labels.push(Label {
            record: (time, kind, trip, board, alight, source, dest, seconds),
            parent,
        });
        self.labels.len() - 1
    }
    fn deadline(&self, e: &Incidence, count: usize, target: i64) -> f64 {
        let net = &self.query.network;
        let p = &net.patterns[e.pattern];
        let at = p.prefix[e.pos];
        let mut answer = f64::NEG_INFINITY;
        for &pos in &p.alights[e.first_alight..] {
            let dest = p.stops[pos] as usize;
            let seconds = p.prefix[pos] - at;
            let reach = self.query.to_target[dest] + seconds;
            let mut bound = self.ride[dest].at(count) as f64 - seconds;
            if bound <= answer {
                continue;
            }
            let target_bound = target as f64 - reach;
            if target_bound < bound {
                bound = target_bound;
            }
            if bound > answer {
                answer = bound;
            }
        }
        answer
    }
    fn prune(&self, improved: Ordered, cache: &mut Permissions) -> Vec<Marked> {
        let mut winners: FxHashMap<(usize, Vec<u64>), usize> = FxHashMap::default();
        let mut keep = vec![false; improved.entries.len()];
        for (i, entry) in improved.entries.iter().enumerate() {
            if entry.source == UNRESTRICTED {
                keep[i] = true;
                continue;
            }
            let group = (entry.stop, self.query.mask(cache, entry.stop, entry.source));
            match winners.entry(group) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(i);
                    keep[i] = true;
                }
                std::collections::hash_map::Entry::Occupied(mut v) => {
                    let previous = *v.get();
                    if self.labels[entry.label].time()
                        < self.labels[improved.entries[previous].label].time()
                    {
                        keep[previous] = false;
                        keep[i] = true;
                        v.insert(i);
                    }
                }
            }
        }
        // Filter in each winner's OWN insertion position, not its earlier loser's slot.
        improved
            .entries
            .into_iter()
            .zip(keep)
            .filter_map(|(e, keep)| keep.then_some(e))
            .collect()
    }

    /// Caller must detach from Python before entering: the shared query mutex may block.
    pub fn run(&mut self, after: i64, boarding: Option<&[i32]>) -> Result<Paths> {
        if self.failed {
            return Err(Error::Runtime(
                "native window cannot be reused after a failed run",
            ));
        }
        let query = Arc::clone(&self.query);
        let result = (|| {
            let mut cache = query
                .permissions
                .lock()
                .map_err(|_| Error::Runtime("query permission lock poisoned"))?;
            let finals = self.run_inner(after, boarding, &mut cache)?;
            finals
                .into_iter()
                .map(|id| self.path(id))
                .collect::<Result<Paths>>()
        })();
        self.peak_labels = self.peak_labels.max(self.labels.len());
        self.peak_label_capacity = self.peak_label_capacity.max(self.labels.capacity());
        self.peak_walk_states = self.peak_walk_states.max(self.walking.len());
        self.labels.clear();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub fn invalidate(&mut self) {
        self.failed = true;
        self.labels.clear();
    }

    fn path(&self, mut id: usize) -> Result<Vec<Record>> {
        let mut path = Vec::new();
        loop {
            let label = self
                .labels
                .get(id)
                .ok_or(Error::Runtime("invalid arena label"))?;
            path.push(label.record);
            match label.parent {
                None => break,
                Some(parent) if parent < id => id = parent,
                Some(_) => return Err(Error::Runtime("invalid arena label parent")),
            }
        }
        path.reverse();
        Ok(path)
    }

    fn run_inner(
        &mut self,
        after: i64,
        boarding: Option<&[i32]>,
        cache: &mut Permissions,
    ) -> Result<Vec<usize>> {
        require(i32::try_from(after).is_ok(), "request time outside int32")?;
        let horizon = add(after, self.horizon_seconds)?;
        // Local Arc keeps immutable metadata independent of mutable state borrows.
        let network = Arc::clone(&self.query.network);
        let net = &network;
        let mut allowed = vec![boarding.is_none(); net.nstops];
        if let Some(stops) = boarding {
            for &stop in stops {
                allowed[index(stop, net.nstops, "boarding stop index")?] = true;
            }
        }
        self.labels.clear();
        let mut initial = Ordered::default();
        let origin_id = self.label(after, None, (0, -1, -1, -1, -1, -1, 0));
        for i in 0..self.access.len() {
            let w = self.access[i];
            let arrival = add(after, w.seconds)?;
            let key = (w.dest as i32, -1);
            if arrival < self.walk_bound(key, 0) {
                self.lower_walk(key, 0, arrival);
                self.best[w.dest].lower(0, arrival);
                if allowed[w.dest] {
                    let id = self.label(
                        arrival,
                        Some(origin_id),
                        (3, -1, -1, -1, -1, w.dest as i32, w.seconds),
                    );
                    initial.set(w.dest, -1, id);
                }
            }
        }
        for i in 0..self.origins.len() {
            let stop = self.origins[i];
            if after < self.ride[stop].at(0) {
                self.ride[stop].lower(0, after);
                self.best[stop].lower(0, after);
                if allowed[stop] {
                    initial.set(stop, UNRESTRICTED, origin_id);
                }
            }
        }
        for i in 0..self.origin_walks.len() {
            let w = self.origin_walks[i];
            let arrival = add(after, w.seconds)?;
            let key = (w.dest as i32, -1);
            if arrival < self.walk_bound(key, 0) && arrival < self.ride[w.dest].at(0) {
                self.lower_walk(key, 0, arrival);
                self.best[w.dest].lower(0, arrival);
                if allowed[w.dest] {
                    let id = self.label(
                        arrival,
                        Some(origin_id),
                        (2, -1, -1, -1, w.source as i32, w.dest as i32, w.seconds),
                    );
                    initial.set(w.dest, -1, id);
                }
            }
        }
        let mut marked = initial.entries;
        let mut results = Vec::new();
        for vehicles in 1..COUNTS {
            if marked.is_empty() {
                break;
            }
            let mut target = horizon.min(self.target_best.at(vehicles));
            let mut reached = Vec::new();
            let mut rides = Ordered::default();
            let mut improved = Ordered::default();
            // sort_by_key is stable: equal arrival times retain Python dict insertion order.
            marked.sort_by_key(|m| self.labels[m.label].time());
            for previous in &marked {
                let stop = previous.stop;
                let previous_time = self.labels[previous.label].time();
                let remaining = target as f64 - self.query.to_target[stop];
                if previous_time as f64 >= remaining {
                    continue;
                }
                for slot in net.incidence_offsets[stop]..net.incidence_offsets[stop + 1] {
                    let e = &net.incidences[slot];
                    if previous.source == -1 && self.origin_board[e.pattern] < e.pos {
                        continue;
                    }
                    if previous.source >= 0
                        && net.board_position(previous.source, e.pattern, e.pos) < e.pos
                    {
                        continue;
                    }
                    let until = self.scans[slot].at(vehicles - 1);
                    if previous_time >= until {
                        continue;
                    }
                    self.scans[slot].lower(vehicles - 1, previous_time);
                    let begin = e.times.partition_point(|&t| i64::from(t) < previous_time);
                    let limit = (until as f64).min(remaining);
                    if begin == e.times.len() || f64::from(e.times[begin]) >= limit {
                        continue;
                    }
                    let end =
                        begin + 1 + e.times[begin + 1..].partition_point(|&t| f64::from(t) < limit);
                    let p = &net.patterns[e.pattern];
                    let mut bound = self.deadline(e, vehicles, target);
                    for ix in begin..end {
                        if f64::from(e.times[ix]) >= bound {
                            break;
                        }
                        let mut changed = false;
                        let trip = e.trips[ix];
                        let row = net.trip_rows[trip];
                        let board = row + e.pos;
                        let base = net.late_base[board];
                        let start = net.cumulative[board];
                        for &pos in &p.alights[e.first_alight..] {
                            let alight = row + pos;
                            let ride_s = net.cumulative[alight] - start;
                            let low = if ride_s > 0.0 {
                                f64::from(base) + ride_s
                            } else {
                                f64::from(base)
                            };
                            if low >= target as f64 {
                                break;
                            }
                            let dest = p.stops[pos] as usize;
                            if low >= self.ride[dest].at(vehicles) as f64
                                || low + self.query.to_target[dest] >= target as f64
                            {
                                continue;
                            }
                            let arrival = net.late_rows(board, alight)?;
                            if arrival >= target {
                                break;
                            }
                            if arrival < self.ride[dest].at(vehicles)
                                && arrival as f64 + self.query.to_target[dest] < target as f64
                            {
                                self.ride[dest].lower(vehicles, arrival);
                                changed = true;
                                let id = self.label(
                                    arrival,
                                    Some(previous.label),
                                    (1, trip as i32, board as i32, alight as i32, -1, -1, 0),
                                );
                                rides.set(dest, UNRESTRICTED, id);
                                improved.set(dest, UNRESTRICTED, id);
                                if arrival < self.best[dest].at(vehicles) {
                                    self.best[dest].lower(vehicles, arrival);
                                }
                                if self.targets[dest] {
                                    let seconds = self.egress[dest];
                                    let final_id = if seconds >= 0 {
                                        self.label(
                                            add(arrival, seconds)?,
                                            Some(id),
                                            (4, -1, -1, -1, -1, dest as i32, seconds),
                                        )
                                    } else {
                                        id
                                    };
                                    if self.labels[final_id].time() < target {
                                        target = self.labels[final_id].time();
                                        reached.push(final_id);
                                    }
                                }
                            }
                        }
                        if changed {
                            bound = self.deadline(e, vehicles, target);
                        }
                    }
                }
            }
            for previous in &rides.entries {
                let stop = previous.stop;
                for w in &net.footpaths[stop] {
                    let arrival = add(self.labels[previous.label].time(), w.seconds)?;
                    let key = (w.dest as i32, stop as i32);
                    if arrival < self.walk_bound(key, vehicles)
                        && arrival < self.ride[w.dest].at(vehicles)
                        && arrival as f64 + self.query.to_target[w.dest] < target as f64
                    {
                        self.lower_walk(key, vehicles, arrival);
                        self.best[w.dest].lower(vehicles, arrival);
                        let id = self.label(
                            arrival,
                            Some(previous.label),
                            (2, -1, -1, -1, stop as i32, w.dest as i32, w.seconds),
                        );
                        improved.set(w.dest, stop as i32, id);
                        if self.targets[w.dest] && !self.has_egress {
                            target = arrival;
                            reached.push(id);
                        }
                    }
                }
            }
            if let Some(&final_id) = reached.iter().min_by_key(|&&id| self.labels[id].time()) {
                self.target_best
                    .lower(vehicles, self.labels[final_id].time());
                results.push(final_id);
            }
            marked = self.prune(improved, cache);
        }
        Ok(results)
    }

    pub fn stats(&self) -> [(&'static str, usize); 7] {
        [
            ("peak_labels", self.peak_labels),
            ("peak_label_capacity", self.peak_label_capacity),
            (
                "peak_label_capacity_bytes",
                self.peak_label_capacity * size_of::<Label>(),
            ),
            ("live_labels_after_return", self.labels.len()),
            ("peak_permission_words", self.query.peak_permission_words()),
            ("peak_walk_states", self.peak_walk_states),
            ("state_payload_bytes", self.payload_bytes()),
        ]
    }
    fn payload_bytes(&self) -> usize {
        size_of::<Self>()
            + (self.best.capacity() + self.ride.capacity() + self.scans.capacity())
                * size_of::<Bounds>()
            + self.walking.len() * size_of::<(Key, Bounds)>()
            + self.labels.capacity() * size_of::<Label>()
            + (self.origins.capacity() + self.origin_board.capacity()) * size_of::<usize>()
            + self.targets.capacity() * size_of::<bool>()
            + self.egress.capacity() * size_of::<i64>()
            + self.access.capacity() * size_of::<Footpath>()
            + self.origin_walks.capacity() * size_of::<OriginWalk>()
    }
}

#[cfg(test)]
mod tests;
