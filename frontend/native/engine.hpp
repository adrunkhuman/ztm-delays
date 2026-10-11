#pragma once
// Checked C++ routing state. Immutable Network, query-local permissions, window-local bounds. No Python containers or
// callbacks in run().
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <deque>
#include <limits>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace routing {
using Time = long long;
using Key = std::uint64_t;
constexpr Time INF = 2147483647;
constexpr int COUNTS = 6;
constexpr int UNRESTRICTED = -2;
constexpr std::size_t NONE = std::numeric_limits<std::size_t>::max();

inline void require(bool ok, const char *message) {
    if (!ok)
        throw std::invalid_argument(message);
}
inline Time add(Time a, Time b) {
    if ((b > 0 && a > std::numeric_limits<Time>::max() - b) || (b < 0 && a < std::numeric_limits<Time>::min() - b))
        throw std::overflow_error("time addition overflow");
    return a + b;
}
inline Key key(int stop, int source) {
    require(stop >= 0 && source >= -2, "invalid state key");
    return (Key(static_cast<unsigned>(stop)) << 32) | Key(Time(source) + 2);
}
struct Bounds {
    std::array<Time, COUNTS> values;
    Bounds() { values.fill(INF); }
    Time at(int k) const { return values.at(k); }
    void lower(int k, Time time) {
        for (; k < COUNTS; ++k) {
            if (time >= values.at(k))
                break;
            values.at(k) = time;
        }
    }
};
struct Pattern {
    std::size_t start, length, alight_start, alight_end;
};
struct Incidence {
    int pattern, pos;
    std::size_t start, end, first_alight;
};
struct Footpath {
    int dest;
    Time seconds;
};
struct OriginWalk {
    int source, dest;
    Time seconds;
};
struct Envelope {
    std::vector<double> upper, ratio, floor;
};
struct Label {
    Time time;
    std::size_t parent;
    int kind, trip, board, alight, source, dest;
    Time seconds;
};
struct Marked {
    int stop, source;
    std::size_t label;
};

// Updating a key never changes its original position, exactly like a Python dict.
struct Ordered {
    std::vector<Marked> entries;
    std::unordered_map<Key, std::size_t> index;
    void set(int stop, int source, std::size_t label) {
        const auto inserted = index.emplace(key(stop, source), entries.size());
        if (inserted.second)
            entries.push_back({stop, source, label});
        else
            entries.at(inserted.first->second).label = label;
    }
};

struct Network {
    std::size_t nstops;
    std::vector<int> late_base, expected, depart, range_ids, trip_rows, trip_pattern;
    std::vector<double> cumulative;
    std::vector<Pattern> patterns;
    std::vector<int> stops, alights;
    std::vector<double> prefix;
    std::vector<Incidence> incidences;
    std::vector<int> times, trips;
    std::vector<std::size_t> incidence_offsets, foot_offsets;
    std::vector<Footpath> footpaths;
    std::vector<Envelope> envelopes;
    std::unordered_map<Key, int> first_board;

    explicit Network(std::size_t n) : nstops(n) {
        require(n <= std::size_t(std::numeric_limits<int>::max() - 2), "too many stops");
        incidence_offsets.push_back(0);
        foot_offsets.push_back(0);
    }
    void column_int(int which, const int *data, std::size_t size) {
        std::vector<int> *column = nullptr;
        switch (which) {
        case 0:
            column = &late_base;
            break;
        case 1:
            column = &expected;
            break;
        case 2:
            column = &depart;
            break;
        case 3:
            column = &range_ids;
            break;
        case 4:
            column = &trip_rows;
            break;
        case 5:
            column = &trip_pattern;
            break;
        default:
            throw std::invalid_argument("unknown numeric column");
        }
        if (size)
            column->assign(data, data + size);
        else
            column->clear();
    }
    void column_double(const double *data, std::size_t size) {
        if (size)
            cumulative.assign(data, data + size);
        else
            cumulative.clear();
    }
    void pattern(const std::vector<int> &s, const std::vector<int> &a, const std::vector<double> &p) {
        require(s.size() == p.size(), "pattern prefix length");
        require(patterns.size() < std::size_t(std::numeric_limits<int>::max() - 2), "too many patterns");
        require(std::is_sorted(a.begin(), a.end()), "alighting order");
        for (int stop : s)
            require(stop >= 0 && std::size_t(stop) < nstops, "pattern stop index");
        for (int pos : a)
            require(pos >= 0 && std::size_t(pos) < s.size(), "alight index");
        for (double v : p)
            require(std::isfinite(v), "nonfinite prefix");
        require(std::is_sorted(p.begin(), p.end()), "nonmonotone prefix");
        patterns.push_back({stops.size(), s.size(), alights.size(), alights.size() + a.size()});
        stops.insert(stops.end(), s.begin(), s.end());
        alights.insert(alights.end(), a.begin(), a.end());
        prefix.insert(prefix.end(), p.begin(), p.end());
    }
    void incidence(int pat, int pos, const int *ts, const int *tr, std::size_t size) {
        const auto &p = patterns.at(pat);
        require(pos >= 0 && std::size_t(pos) < p.length, "incidence position");
        const auto stop = incidence_offsets.size() - 1;
        require(stop < nstops && stops.at(p.start + pos) == int(stop), "incidence stop mismatch");
        if (size)
            require(std::is_sorted(ts, ts + size), "unsorted boarding times");
        for (std::size_t i = 0; i < size; ++i) {
            require(tr[i] >= 0 && std::size_t(tr[i]) < trip_rows.size(), "incidence trip index");
            require(trip_pattern.at(tr[i]) == pat, "incidence trip pattern mismatch");
        }
        std::size_t first = p.alight_start;
        while (first < p.alight_end && alights.at(first) <= pos)
            ++first;
        incidences.push_back({pat, pos, times.size(), times.size() + size, first});
        if (size) {
            times.insert(times.end(), ts, ts + size);
            trips.insert(trips.end(), tr, tr + size);
        }
        const auto k = key(int(stop), pat);
        const auto inserted = first_board.emplace(k, pos);
        // Preserve board_at.setdefault, not the last incidence at a repeated stop.
        (void)inserted;
    }
    void end_stop() { incidence_offsets.push_back(incidences.size()); }
    void footpath(int dest, Time seconds) {
        require(dest >= 0 && std::size_t(dest) < nstops && seconds >= 0, "invalid footpath");
        footpaths.push_back({dest, seconds});
    }
    void end_foot_stop() { foot_offsets.push_back(footpaths.size()); }
    void envelope(const std::vector<double> &upper, const std::vector<double> &ratio,
                  const std::vector<double> &floor) {
        require(!upper.empty() && upper.size() == ratio.size() && upper.size() == floor.size(), "envelope lengths");
        require(std::is_sorted(upper.begin(), upper.end()) && upper.back() == INFINITY, "envelope upper bounds");
        for (std::size_t i = 0; i < upper.size(); ++i) {
            require(!std::isnan(upper.at(i)) && std::isfinite(ratio.at(i)) && ratio.at(i) >= 1 &&
                        std::isfinite(floor.at(i)),
                    "invalid envelope");
        }
        envelopes.push_back({upper, ratio, floor});
    }
    void finish() const {
        const auto rows = late_base.size();
        require(rows <= std::size_t(std::numeric_limits<int>::max()), "too many rows");
        require(expected.size() == rows && depart.size() == rows && range_ids.size() == rows &&
                    cumulative.size() == rows,
                "numeric column lengths");
        require(trip_rows.size() == trip_pattern.size(), "trip column lengths");
        require(incidence_offsets.size() == nstops + 1 && foot_offsets.size() == nstops + 1, "adjacency lengths");
        for (double value : cumulative)
            require(std::isfinite(value), "nonfinite cumulative duration");
        for (int cell : range_ids)
            require(cell == -1 || (cell >= 0 && std::size_t(cell) < envelopes.size()), "envelope index");
        for (std::size_t t = 0; t < trip_rows.size(); ++t) {
            const auto &pat = patterns.at(trip_pattern.at(t));
            const int row = trip_rows.at(t);
            require(row >= 0 && std::size_t(row) <= rows && pat.length <= rows - std::size_t(row), "trip row range");
        }
    }
    int board_position(int source, int pattern, int fallback) const {
        const auto found = first_board.find(key(source, pattern));
        return found == first_board.end() ? fallback : found->second;
    }
    Time late(int board, int alight) const {
        const double duration = std::max(0.0, cumulative.at(alight) - cumulative.at(board));
        double high = duration;
        const int cell = range_ids.at(board);
        if (cell >= 0) {
            const auto &e = envelopes.at(cell);
            const auto index =
                std::size_t(std::lower_bound(e.upper.begin(), e.upper.end(), duration) - e.upper.begin());
            high = std::max(e.floor.at(index), duration * e.ratio.at(index));
        }
        const double rounded = std::ceil(double(late_base.at(board)) + high);
        // Checking against 2^63 (not double(LLONG_MAX), which rounds up) precedes the cast.
        if (!std::isfinite(rounded) || rounded < -0x1p63 || rounded >= 0x1p63)
            throw std::overflow_error("late arrival does not fit signed 64 bits");
        return std::max(Time(rounded), std::max(Time(expected.at(alight)), Time(depart.at(board))));
    }
    std::size_t payload_bytes() const {
        std::size_t result = sizeof(*this);
        result += sizeof(int) * (late_base.capacity() + expected.capacity() + depart.capacity() + range_ids.capacity() +
                                 trip_rows.capacity() + trip_pattern.capacity() + stops.capacity() +
                                 alights.capacity() + times.capacity() + trips.capacity());
        result += sizeof(double) * (cumulative.capacity() + prefix.capacity());
        result += sizeof(Pattern) * patterns.capacity() + sizeof(Incidence) * incidences.capacity() +
                  sizeof(Footpath) * footpaths.capacity();
        result += sizeof(std::size_t) * (incidence_offsets.capacity() + foot_offsets.capacity());
        for (const auto &e : envelopes)
            result += sizeof(double) * (e.upper.capacity() + e.ratio.capacity() + e.floor.capacity());
        // Container buckets/node allocator overhead excluded and explicitly reported as a limitation.
        result += first_board.size() * sizeof(std::pair<const Key, int>);
        return result;
    }
};

struct Group {
    int stop;
    std::vector<Key> mask;
    bool operator==(const Group &other) const { return stop == other.stop && mask == other.mask; }
};
struct GroupHash {
    std::size_t operator()(const Group &g) const {
        std::size_t hash = std::hash<int>{}(g.stop);
        for (Key word : g.mask)
            hash ^= std::hash<Key>{}(word) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
        return hash;
    }
};
// Query-local masks have a strict word budget. Suffixes are scanned directly in
// the immutable pattern arrays: long patterns never allocate quadratic caches.
struct Query {
    const Network *net;
    std::vector<double> to_target;
    std::size_t budget, words = 0, peak_words = 0;
    std::unordered_map<Key, std::vector<Key>> permissions;
    std::deque<Key> fifo;
    Query(const Network *n, const std::vector<double> &target, std::size_t cap)
        : net(n), to_target(target), budget(cap) {
        require(target.size() == n->nstops, "target bound length");
        for (double d : target)
            require(!std::isnan(d) && d >= 0, "invalid target bound");
    }
    std::vector<Key> mask(int stop, int source) {
        const auto k = key(stop, source);
        const auto cached = permissions.find(k);
        if (cached != permissions.end())
            return cached->second;
        const auto begin = net->incidence_offsets.at(stop), end = net->incidence_offsets.at(stop + 1);
        std::vector<Key> value(std::max(std::size_t(1), (end - begin + 63) / 64), 0);
        for (std::size_t i = begin; i < end; ++i) {
            const auto &e = net->incidences.at(i);
            if (net->board_position(source, e.pattern, e.pos) >= e.pos)
                value.at((i - begin) / 64) |= Key(1) << ((i - begin) % 64);
        }
        while (value.size() > 1 && value.back() == 0)
            value.pop_back();
        if (value.size() <= budget) {
            while (!fifo.empty() && words + value.size() > budget) {
                const auto old = fifo.front();
                fifo.pop_front();
                words -= permissions.at(old).size();
                permissions.erase(old);
            }
            permissions.emplace(k, value);
            fifo.push_back(k);
            words += value.size();
            peak_words = std::max(peak_words, words);
        }
        return value;
    }
};

// Bounds survive decreasing departure times within one window; predecessors do
// not. The Cython boundary clears labels on both success and exceptions.
struct State {
    const Network *net;
    Query *query;
    std::vector<Bounds> best, ride, scans;
    Bounds target_best;
    std::unordered_map<Key, Bounds> walking;
    std::vector<int> origins, origin_board;
    std::vector<Footpath> access;
    std::vector<OriginWalk> origin_walks;
    std::vector<int> targets;
    std::vector<Time> egress;
    bool has_egress;
    Time horizon_seconds;
    std::vector<Label> labels;
    std::size_t peak_labels = 0, peak_label_capacity = 0, peak_walk_states = 0;

    State(const Network *n, Query *q, Time horizon)
        : net(n), query(q), best(n->nstops), ride(n->nstops), scans(n->incidences.size()),
          origin_board(n->patterns.size(), std::numeric_limits<int>::max()), targets(n->nstops, 0),
          egress(n->nstops, -1), has_egress(false), horizon_seconds(horizon) {
        require(horizon >= 0, "negative journey horizon");
    }
    void origin(int stop) {
        (void)best.at(stop);
        origins.push_back(stop);
        for (std::size_t i = net->incidence_offsets.at(stop); i < net->incidence_offsets.at(stop + 1); ++i) {
            const auto &e = net->incidences.at(i);
            origin_board.at(e.pattern) =
                std::min(origin_board.at(e.pattern), net->board_position(stop, e.pattern, e.pos));
        }
    }
    void access_walk(int dest, Time seconds) {
        (void)best.at(dest);
        require(seconds >= 0, "negative access walk");
        access.push_back({dest, seconds});
    }
    void initial_walk(int source, int dest, Time seconds) {
        (void)best.at(source);
        (void)best.at(dest);
        require(seconds >= 0, "negative initial walk");
        origin_walks.push_back({source, dest, seconds});
    }
    void target(int dest, Time seconds) {
        targets.at(dest) = 1;
        require(seconds >= -1, "invalid egress");
        egress.at(dest) = seconds;
        if (seconds >= 0)
            has_egress = true;
    }
    Time walk_bound(Key k, int count) const {
        const auto found = walking.find(k);
        return found == walking.end() ? INF : found->second.at(count);
    }
    void lower_walk(Key k, int count, Time time) { walking[k].lower(count, time); }
    std::size_t label(Time time, std::size_t parent, int kind, int trip = -1, int board = -1, int alight = -1,
                      int source = -1, int dest = -1, Time seconds = 0) {
        require(parent == NONE || parent < labels.size(), "invalid label parent");
        labels.push_back({time, parent, kind, trip, board, alight, source, dest, seconds});
        return labels.size() - 1;
    }
    double deadline(const Incidence &e, int count, Time target) const {
        const auto &p = net->patterns.at(e.pattern);
        const double at = net->prefix.at(p.start + e.pos);
        double answer = -INFINITY;
        for (std::size_t i = e.first_alight; i < p.alight_end; ++i) {
            const int pos = net->alights.at(i), dest = net->stops.at(p.start + pos);
            const double seconds = net->prefix.at(p.start + pos) - at;
            const double reach = query->to_target.at(dest) + seconds;
            double bound = double(ride.at(dest).at(count)) - seconds;
            if (bound <= answer)
                continue;
            const double target_bound = double(target) - reach;
            if (target_bound < bound)
                bound = target_bound;
            if (bound > answer)
                answer = bound;
        }
        return answer;
    }
    // Filter winners in their own insertion order: moving a later winner into
    // an earlier loser's slot changes equal-time itinerary ties.
    std::vector<Marked> prune(const Ordered &improved) {
        std::unordered_map<Group, std::size_t, GroupHash> winners;
        std::vector<unsigned char> keep(improved.entries.size(), 0);
        for (std::size_t i = 0; i < improved.entries.size(); ++i) {
            const auto &entry = improved.entries.at(i);
            if (entry.source == UNRESTRICTED) {
                keep.at(i) = 1;
                continue;
            }
            Group group{entry.stop, query->mask(entry.stop, entry.source)};
            const auto found = winners.emplace(std::move(group), i);
            if (found.second)
                keep.at(i) = 1;
            else {
                const auto previous = found.first->second;
                if (labels.at(entry.label).time < labels.at(improved.entries.at(previous).label).time) {
                    keep.at(previous) = 0;
                    keep.at(i) = 1;
                    found.first->second = i;
                }
            }
        }
        std::vector<Marked> result;
        result.reserve(improved.entries.size());
        for (std::size_t i = 0; i < improved.entries.size(); ++i)
            if (keep.at(i))
                result.push_back(improved.entries.at(i));
        return result;
    }
    std::vector<std::size_t> run(Time after, const std::vector<int> &boarding, bool unrestricted_boarding) {
        // Event columns are signed int32. Reject unsupported huge request times instead of narrowing silently.
        require(after >= std::numeric_limits<int>::min() && after <= std::numeric_limits<int>::max(),
                "request time outside int32");
        const Time horizon = add(after, horizon_seconds);
        std::vector<unsigned char> allowed(net->nstops, unrestricted_boarding ? 1 : 0);
        for (int stop : boarding)
            allowed.at(stop) = 1;
        labels.clear();
        Ordered initial;
        const auto origin_id = label(after, NONE, 0);
        for (const auto &w : access) {
            const Time arrival = add(after, w.seconds);
            const Key k = key(w.dest, -1);
            if (arrival < walk_bound(k, 0)) {
                lower_walk(k, 0, arrival);
                best.at(w.dest).lower(0, arrival);
                if (allowed.at(w.dest))
                    initial.set(w.dest, -1, label(arrival, origin_id, 3, -1, -1, -1, -1, w.dest, w.seconds));
            }
        }
        for (int stop : origins) {
            if (after < ride.at(stop).at(0)) {
                ride.at(stop).lower(0, after);
                best.at(stop).lower(0, after);
                if (allowed.at(stop))
                    initial.set(stop, UNRESTRICTED, origin_id);
            }
        }
        for (const auto &w : origin_walks) {
            const Time arrival = add(after, w.seconds);
            const Key k = key(w.dest, -1);
            if (arrival < walk_bound(k, 0) && arrival < ride.at(w.dest).at(0)) {
                lower_walk(k, 0, arrival);
                best.at(w.dest).lower(0, arrival);
                if (allowed.at(w.dest))
                    initial.set(w.dest, -1, label(arrival, origin_id, 2, -1, -1, -1, w.source, w.dest, w.seconds));
            }
        }
        std::vector<Marked> marked = std::move(initial.entries);
        std::vector<std::size_t> results;
        for (int vehicles = 1; vehicles < COUNTS && !marked.empty(); ++vehicles) {
            Time target = std::min(horizon, target_best.at(vehicles));
            std::vector<std::size_t> reached;
            Ordered rides, improved;
            std::stable_sort(marked.begin(), marked.end(), [this](const Marked &a, const Marked &b) {
                return labels.at(a.label).time < labels.at(b.label).time;
            });
            for (const auto &previous : marked) {
                const int stop = previous.stop;
                const Time previous_time = labels.at(previous.label).time;
                const double remaining = double(target) - query->to_target.at(stop);
                if (previous_time >= remaining)
                    continue;
                for (std::size_t slot = net->incidence_offsets.at(stop); slot < net->incidence_offsets.at(stop + 1);
                     ++slot) {
                    const auto &e = net->incidences.at(slot);
                    if (previous.source == -1 && origin_board.at(e.pattern) < e.pos)
                        continue;
                    if (previous.source >= 0 && net->board_position(previous.source, e.pattern, e.pos) < e.pos)
                        continue;
                    auto &bounds = scans.at(slot);
                    // Once allowed, downstream work no longer depends on walk history.
                    // Share the scanned interval, but keep each vehicle-count bound.
                    const Time until = bounds.at(vehicles - 1);
                    if (previous_time >= until)
                        continue;
                    bounds.lower(vehicles - 1, previous_time);
                    const auto begin_it =
                        std::lower_bound(net->times.begin() + e.start, net->times.begin() + e.end, previous_time);
                    const auto begin = std::size_t(begin_it - net->times.begin());
                    const double limit = std::min(double(until), remaining);
                    if (begin == e.end || net->times.at(begin) >= limit)
                        continue;
                    const auto end = std::size_t(std::lower_bound(begin_it + 1, net->times.begin() + e.end, limit) -
                                                 net->times.begin());
                    const auto &p = net->patterns.at(e.pattern);
                    double bound = deadline(e, vehicles, target);
                    for (std::size_t index = begin; index < end; ++index) {
                        if (net->times.at(index) >= bound)
                            break;
                        bool changed = false;
                        const int trip = net->trips.at(index), row = net->trip_rows.at(trip), board = row + e.pos;
                        const Time base = net->late_base.at(board);
                        const double start = net->cumulative.at(board);
                        for (std::size_t a = e.first_alight; a < p.alight_end; ++a) {
                            const int pos = net->alights.at(a), alight = row + pos;
                            const double ride_s = net->cumulative.at(alight) - start;
                            const double low = ride_s > 0 ? double(base) + ride_s : double(base);
                            if (low >= target)
                                break;
                            const int dest = net->stops.at(p.start + pos);
                            if (low >= ride.at(dest).at(vehicles) || low + query->to_target.at(dest) >= target)
                                continue;
                            const Time arrival = net->late(board, alight);
                            if (arrival >= target)
                                break;
                            if (arrival < ride.at(dest).at(vehicles) &&
                                double(arrival) + query->to_target.at(dest) < target) {
                                ride.at(dest).lower(vehicles, arrival);
                                changed = true;
                                const auto id = label(arrival, previous.label, 1, trip, board, alight);
                                rides.set(dest, UNRESTRICTED, id);
                                improved.set(dest, UNRESTRICTED, id);
                                if (arrival < best.at(dest).at(vehicles))
                                    best.at(dest).lower(vehicles, arrival);
                                if (targets.at(dest)) {
                                    const Time seconds = egress.at(dest);
                                    const auto final = seconds >= 0 ? label(add(arrival, seconds), id, 4, -1, -1, -1,
                                                                            -1, dest, seconds)
                                                                    : id;
                                    if (labels.at(final).time < target) {
                                        target = labels.at(final).time;
                                        reached.push_back(final);
                                    }
                                }
                            }
                        }
                        if (changed)
                            bound = deadline(e, vehicles, target);
                    }
                }
            }
            for (const auto &previous : rides.entries) {
                const int stop = previous.stop;
                for (std::size_t f = net->foot_offsets.at(stop); f < net->foot_offsets.at(stop + 1); ++f) {
                    const auto &w = net->footpaths.at(f);
                    const Time arrival = add(labels.at(previous.label).time, w.seconds);
                    const Key k = key(w.dest, stop);
                    if (arrival < walk_bound(k, vehicles) && arrival < ride.at(w.dest).at(vehicles) &&
                        double(arrival) + query->to_target.at(w.dest) < target) {
                        lower_walk(k, vehicles, arrival);
                        best.at(w.dest).lower(vehicles, arrival);
                        const auto id = label(arrival, previous.label, 2, -1, -1, -1, stop, w.dest, w.seconds);
                        improved.set(w.dest, stop, id);
                        if (targets.at(w.dest) && !has_egress) {
                            target = arrival;
                            reached.push_back(id);
                        }
                    }
                }
            }
            if (!reached.empty()) {
                const auto final =
                    *std::min_element(reached.begin(), reached.end(), [this](std::size_t a, std::size_t b) {
                        return labels.at(a).time < labels.at(b).time;
                    });
                target_best.lower(vehicles, labels.at(final).time);
                results.push_back(final);
            }
            marked = prune(improved);
        }
        peak_labels = std::max(peak_labels, labels.size());
        peak_label_capacity = std::max(peak_label_capacity, labels.capacity());
        peak_walk_states = std::max(peak_walk_states, walking.size());
        return results;
    }
    Label get_label(std::size_t index) const { return labels.at(index); }
    void clear_labels() { labels.clear(); }
    std::size_t live_labels() const { return labels.size(); }
    std::size_t payload_bytes() const {
        return sizeof(*this) + sizeof(Bounds) * (best.capacity() + ride.capacity() + scans.capacity()) +
               walking.size() * sizeof(std::pair<const Key, Bounds>) + labels.capacity() * sizeof(Label) +
               sizeof(int) * (origins.capacity() + origin_board.capacity() + targets.capacity()) +
               sizeof(Time) * egress.capacity() + sizeof(Footpath) * access.capacity() +
               sizeof(OriginWalk) * origin_walks.capacity();
    }
};
} // namespace routing
