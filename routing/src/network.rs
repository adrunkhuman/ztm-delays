//! Immutable, owned timetable metadata. All external indices are checked at construction.
use crate::error::{Error, Result, index, require};
use rustc_hash::FxHashMap;
use std::mem::size_of;

pub type Key = (i32, i32);
pub type RawIncidence = (i32, i32, Vec<i32>, Vec<i32>);
pub type RawEnvelope = (Vec<f64>, Vec<f64>, Vec<f64>);

#[derive(Debug)]
pub struct Pattern {
    pub stops: Vec<i32>,
    pub alights: Vec<usize>,
    pub prefix: Vec<f64>,
}
#[derive(Debug)]
pub struct Incidence {
    pub pattern: usize,
    pub pos: usize,
    pub times: Vec<i32>,
    pub trips: Vec<usize>,
    pub first_alight: usize,
}
#[derive(Debug, Clone, Copy)]
pub struct Footpath {
    pub dest: usize,
    pub seconds: i64,
}
#[derive(Debug, Clone, Copy)]
pub struct OriginWalk {
    pub source: usize,
    pub dest: usize,
    pub seconds: i64,
}
#[derive(Debug)]
pub struct Envelope {
    upper: Vec<f64>,
    ratio: Vec<f64>,
    floor: Vec<f64>,
}
#[derive(Debug)]
pub struct Network {
    pub nstops: usize,
    pub late_base: Vec<i32>,
    pub expected: Vec<i32>,
    pub depart: Vec<i32>,
    range_ids: Vec<i32>,
    pub trip_rows: Vec<usize>,
    pub trip_pattern: Vec<usize>,
    pub cumulative: Vec<f64>,
    pub patterns: Vec<Pattern>,
    pub incidences: Vec<Incidence>,
    pub incidence_offsets: Vec<usize>,
    pub footpaths: Vec<Vec<Footpath>>,
    envelopes: Vec<Envelope>,
    first_board: FxHashMap<Key, usize>,
}

pub struct NetworkInput {
    pub nstops: usize,
    pub late_base: Vec<i32>,
    pub expected: Vec<i32>,
    pub depart: Vec<i32>,
    pub range_ids: Vec<i32>,
    pub trip_rows: Vec<i32>,
    pub trip_pattern: Vec<i32>,
    pub cumulative: Vec<f64>,
    pub patterns: Vec<Vec<i32>>,
    pub pattern_alights: Vec<Vec<i32>>,
    pub pattern_prefix: Vec<Vec<f64>>,
    pub incidence: Vec<Vec<RawIncidence>>,
    pub footpaths: Vec<Vec<(i32, i64)>>,
    pub envelopes: Vec<RawEnvelope>,
}

fn sorted<T: PartialOrd>(values: &[T]) -> bool {
    values.windows(2).all(|w| w[0] <= w[1])
}

impl Network {
    pub fn new(input: NetworkInput) -> Result<Self> {
        let NetworkInput {
            nstops,
            late_base,
            expected,
            depart,
            range_ids,
            trip_rows,
            trip_pattern,
            cumulative,
            patterns,
            pattern_alights,
            pattern_prefix,
            incidence,
            footpaths,
            envelopes,
        } = input;
        require(nstops <= (i32::MAX - 2) as usize, "too many stops")?;
        let rows = late_base.len();
        require(rows <= i32::MAX as usize, "too many rows")?;
        require(
            expected.len() == rows
                && depart.len() == rows
                && range_ids.len() == rows
                && cumulative.len() == rows,
            "numeric column lengths",
        )?;
        require(trip_rows.len() == trip_pattern.len(), "trip column lengths")?;
        require(trip_rows.len() <= i32::MAX as usize, "too many trips")?;
        require(
            incidence.len() == nstops && footpaths.len() == nstops,
            "adjacency lengths",
        )?;
        require(
            cumulative.iter().all(|v| v.is_finite()),
            "nonfinite cumulative duration",
        )?;
        require(
            patterns.len() == pattern_alights.len() && patterns.len() == pattern_prefix.len(),
            "pattern column lengths",
        )?;
        require(
            patterns.len() < (i32::MAX - 2) as usize,
            "too many patterns",
        )?;
        let mut pats = Vec::with_capacity(patterns.len());
        for ((stops, alights), prefix) in patterns
            .into_iter()
            .zip(pattern_alights)
            .zip(pattern_prefix)
        {
            require(
                stops.len() <= i32::MAX as usize && stops.len() == prefix.len(),
                "pattern prefix length",
            )?;
            for &stop in &stops {
                index(stop, nstops, "pattern stop index")?;
            }
            require(sorted(&alights), "alighting order")?;
            let alights = alights
                .into_iter()
                .map(|p| index(p, stops.len(), "alight index"))
                .collect::<Result<Vec<_>>>()?;
            require(prefix.iter().all(|v| v.is_finite()), "nonfinite prefix")?;
            require(sorted(&prefix), "nonmonotone prefix")?;
            pats.push(Pattern {
                stops,
                alights,
                prefix,
            });
        }
        let trip_pattern = trip_pattern
            .into_iter()
            .map(|p| index(p, pats.len(), "trip pattern index"))
            .collect::<Result<Vec<_>>>()?;
        let mut native_rows = Vec::with_capacity(trip_rows.len());
        for (t, row) in trip_rows.into_iter().enumerate() {
            require(
                row >= 0
                    && (row as usize) <= rows
                    && pats[trip_pattern[t]].stops.len() <= rows - row as usize,
                "trip row range",
            )?;
            native_rows.push(row as usize);
        }
        let mut native_envelopes = Vec::with_capacity(envelopes.len());
        for (upper, ratio, floor) in envelopes {
            require(
                !upper.is_empty() && upper.len() == ratio.len() && upper.len() == floor.len(),
                "envelope lengths",
            )?;
            require(
                sorted(&upper) && upper.last() == Some(&f64::INFINITY),
                "envelope upper bounds",
            )?;
            require(
                upper.iter().all(|v| !v.is_nan())
                    && ratio.iter().all(|v| v.is_finite() && *v >= 1.0)
                    && floor.iter().all(|v| v.is_finite()),
                "invalid envelope",
            )?;
            native_envelopes.push(Envelope {
                upper,
                ratio,
                floor,
            });
        }
        for &cell in &range_ids {
            require(
                cell == -1 || (cell >= 0 && (cell as usize) < native_envelopes.len()),
                "envelope index",
            )?;
        }
        let mut incidences = Vec::new();
        let mut incidence_offsets = vec![0];
        let mut first_board = FxHashMap::default();
        for (stop, entries) in incidence.into_iter().enumerate() {
            for (pat, pos, times, trips) in entries {
                let pattern = index(pat, pats.len(), "incidence pattern index")?;
                let p = &pats[pattern];
                let pos = index(pos, p.stops.len(), "incidence position")?;
                require(p.stops[pos] as usize == stop, "incidence stop mismatch")?;
                require(
                    times.len() == trips.len(),
                    "incidence time/trip lengths differ",
                )?;
                require(sorted(&times), "unsorted boarding times")?;
                let trips = trips
                    .into_iter()
                    .map(|t| index(t, native_rows.len(), "incidence trip index"))
                    .collect::<Result<Vec<_>>>()?;
                require(
                    trips.iter().all(|&t| trip_pattern[t] == pattern),
                    "incidence trip pattern mismatch",
                )?;
                let first_alight = p.alights.partition_point(|&a| a <= pos);
                incidences.push(Incidence {
                    pattern,
                    pos,
                    times,
                    trips,
                    first_alight,
                });
                // Python board_at.setdefault keeps the FIRST position of a repeated stop.
                first_board.entry((stop as i32, pat)).or_insert(pos);
            }
            incidence_offsets.push(incidences.len());
        }
        let footpaths = footpaths
            .into_iter()
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|(dest, seconds)| {
                        let dest = index(dest, nstops, "footpath destination index")?;
                        require(seconds >= 0, "invalid footpath")?;
                        Ok(Footpath { dest, seconds })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            nstops,
            late_base,
            expected,
            depart,
            range_ids,
            trip_rows: native_rows,
            trip_pattern,
            cumulative,
            patterns: pats,
            incidences,
            incidence_offsets,
            footpaths,
            envelopes: native_envelopes,
            first_board,
        })
    }

    pub fn board_position(&self, source: i32, pattern: usize, fallback: usize) -> usize {
        self.first_board
            .get(&(source, pattern as i32))
            .copied()
            .unwrap_or(fallback)
    }

    pub fn late(&self, board: i32, alight: i32) -> Result<i64> {
        let board = index(board, self.late_base.len(), "boarding row index")?;
        let alight = index(alight, self.late_base.len(), "alighting row index")?;
        self.late_rows(board, alight)
    }

    pub fn late_rows(&self, board: usize, alight: usize) -> Result<i64> {
        let duration = 0.0_f64.max(self.cumulative[alight] - self.cumulative[board]);
        let mut high = duration;
        let cell = self.range_ids[board];
        if cell >= 0 {
            let e = &self.envelopes[cell as usize];
            let i = e.upper.partition_point(|&u| u < duration);
            high = e.floor[i].max(duration * e.ratio[i]);
        }
        // Keep multiplication and addition separate: no fast-math / fused multiply-add.
        let rounded = (f64::from(self.late_base[board]) + high).ceil();
        // i64::MAX as f64 rounds UP to 2^63. Check the exclusive boundary before casting.
        if !rounded.is_finite()
            || !(-9223372036854775808.0..9223372036854775808.0).contains(&rounded)
        {
            return Err(Error::Overflow("late arrival does not fit signed 64 bits"));
        }
        Ok((rounded as i64)
            .max(i64::from(self.expected[alight]))
            .max(i64::from(self.depart[board])))
    }

    /// Owned payload capacity; hash bucket/allocator overhead is deliberately excluded.
    pub fn payload_bytes(&self) -> usize {
        let mut bytes = size_of::<Self>();
        bytes += (self.late_base.capacity()
            + self.expected.capacity()
            + self.depart.capacity()
            + self.range_ids.capacity())
            * size_of::<i32>();
        bytes += (self.trip_rows.capacity()
            + self.trip_pattern.capacity()
            + self.incidence_offsets.capacity())
            * size_of::<usize>();
        bytes += self.cumulative.capacity() * size_of::<f64>();
        bytes += self.patterns.capacity() * size_of::<Pattern>();
        for p in &self.patterns {
            bytes += p.stops.capacity() * size_of::<i32>()
                + p.alights.capacity() * size_of::<usize>()
                + p.prefix.capacity() * size_of::<f64>();
        }
        bytes += self.incidences.capacity() * size_of::<Incidence>();
        for e in &self.incidences {
            bytes +=
                e.times.capacity() * size_of::<i32>() + e.trips.capacity() * size_of::<usize>();
        }
        bytes += self.footpaths.capacity() * size_of::<Vec<Footpath>>();
        bytes += self
            .footpaths
            .iter()
            .map(|f| f.capacity() * size_of::<Footpath>())
            .sum::<usize>();
        bytes += self.envelopes.capacity() * size_of::<Envelope>();
        for e in &self.envelopes {
            bytes +=
                (e.upper.capacity() + e.ratio.capacity() + e.floor.capacity()) * size_of::<f64>();
        }
        bytes + self.first_board.len() * size_of::<(Key, usize)>()
    }
}
