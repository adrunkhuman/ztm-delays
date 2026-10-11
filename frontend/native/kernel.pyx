# cython: language_level=3, boundscheck=True, wraparound=True, initializedcheck=True
# distutils: language = c++
"""Required routing backend. All calls retain the GIL, including the C++ search.

PreparedNet owns immutable copies; queries own permission masks; window states
own numeric bounds. No Python Network/Profile references or callbacks in C++.
Every potentially throwing C++ call translates exceptions at this boundary.
"""
from libc.stddef cimport size_t
from libcpp.vector cimport vector

cdef extern from "engine.hpp":
    cdef cppclass CNetwork "routing::Network":
        CNetwork(size_t) except +
        void column_int(int, const int*, size_t) except +
        void column_double(const double*, size_t) except +
        void pattern(const vector[int]&, const vector[int]&, const vector[double]&) except +
        void incidence(int, int, const int*, const int*, size_t) except +
        void end_stop() except +
        void footpath(int, long long) except +
        void end_foot_stop() except +
        void envelope(const vector[double]&, const vector[double]&, const vector[double]&) except +
        void finish() except +
        long long late(int, int) except +
        size_t payload_bytes() except +
    cdef cppclass CQuery "routing::Query":
        CQuery(const CNetwork*, const vector[double]&, size_t) except +
        size_t peak_words
    cdef cppclass CLabel "routing::Label":
        long long time, seconds
        size_t parent
        int kind, trip, board, alight, source, dest
    cdef cppclass CState "routing::State":
        CState(const CNetwork*, CQuery*, long long) except +
        void origin(int) except +
        void access_walk(int, long long) except +
        void initial_walk(int, int, long long) except +
        void target(int, long long) except +
        vector[size_t] run(long long, const vector[int]&, bint) except +
        CLabel get_label(size_t) except +
        void clear_labels() except +
        size_t live_labels() except +
        bint has_egress
        size_t payload_bytes() except +
        size_t peak_labels, peak_label_capacity, peak_walk_states

cdef class PreparedNet:
    cdef object __weakref__
    cdef CNetwork* ptr
    def __cinit__(self, net):
        cdef const int[::1] ints, times, trips
        cdef const double[::1] doubles
        cdef int which, pattern, pos
        self.ptr = new CNetwork(len(net.stop_ids))
        for which, column in enumerate((net.late_base, net.expected, net.depart, net.range_ids, net.trip_rows, net.trip_pattern)):
            ints = column
            self.ptr.column_int(which, &ints[0] if ints.shape[0] else NULL, ints.shape[0])
        doubles = net.cumulative
        self.ptr.column_double(&doubles[0] if doubles.shape[0] else NULL, doubles.shape[0])
        for pattern in range(len(net.patterns)):
            self.ptr.pattern(net.patterns[pattern], net.pattern_alights[pattern], net.pattern_prefix[pattern])
        for entries in net.incidence:
            for pattern, pos, raw_times, raw_trips in entries:
                times, trips = raw_times, raw_trips
                if times.shape[0] != trips.shape[0]:
                    raise ValueError('incidence time/trip lengths differ')
                self.ptr.incidence(pattern, pos, &times[0] if times.shape[0] else NULL,
                                   &trips[0] if trips.shape[0] else NULL, times.shape[0])
            self.ptr.end_stop()
        for walks in net.footpaths:
            for dest, seconds in walks:
                self.ptr.footpath(dest, seconds)
            self.ptr.end_foot_stop()
        for envelope in net.envelopes:
            self.ptr.envelope(envelope.upper, envelope.ratio, envelope.floor)
        self.ptr.finish()
    def __dealloc__(self):
        del self.ptr
    def payload_bytes(self):
        return self.ptr.payload_bytes()
    def late(self, int board, int alight):
        return self.ptr.late(board, alight)

cdef class PreparedQuery:
    cdef CQuery* ptr
    cdef object __weakref__
    cdef readonly PreparedNet network
    def __cinit__(self, PreparedNet network not None, to_target, size_t permission_words):
        self.network = network
        self.ptr = new CQuery(network.ptr, to_target, permission_words)
    def __dealloc__(self):
        del self.ptr
    def peak_permission_words(self):
        return self.ptr.peak_words

cdef class NativeState:
    cdef CState* ptr
    cdef object __weakref__
    cdef readonly PreparedQuery query
    cdef bint failed
    def __cinit__(self, PreparedQuery query not None, profile):
        from ztm_frontend import journey as j
        if j.MAX_VEHICLES != 5 or j.INF != 2147483647:
            raise ValueError('native routing requires six vehicle-count columns and int32 INF')
        self.query = query
        self.ptr = new CState(query.network.ptr, query.ptr, j.MAX_JOURNEY_S)
        for stop in profile.origins:
            self.ptr.origin(stop)
        for stop, walk in profile.access.items():
            self.ptr.access_walk(stop, walk.walk_s)
        for source, dest, seconds in profile.origin_walks:
            self.ptr.initial_walk(source, dest, seconds)
        for dest in profile.targets:
            walk = profile.egress.get(dest)
            self.ptr.target(dest, walk.walk_s if walk else -1)
        self.ptr.has_egress = bool(profile.egress)
    def __dealloc__(self):
        del self.ptr

    cdef object materialize(self, object profile, size_t index, dict cache):
        cdef CLabel label
        from ztm_frontend import journey as j
        cached = cache.get(index)
        if cached is not None:
            return cached
        label = self.ptr.get_label(index)
        if label.kind == 0:
            result = j._Label(label.time)
        else:
            previous = self.materialize(profile, label.parent, cache)
            if label.kind == 1:
                leg = (label.trip, label.board, label.alight)
            elif label.kind == 2:
                leg = j.Walk(profile.net.stop_ids[label.source], profile.net.stop_ids[label.dest], label.seconds)
            elif label.kind == 3:
                leg = profile.access[label.dest]
            elif label.kind == 4:
                leg = profile.egress[label.dest]
            else:
                raise RuntimeError('invalid arena label kind')
            result = j._Label(label.time, previous, leg)
        cache[index] = result
        return result

    def run(self, profile, long long after, boarding=None):
        cdef vector[int] boarded
        cdef vector[size_t] results
        cdef size_t i
        if self.failed:
            raise RuntimeError('native window cannot be reused after a failed run')
        try:
            if boarding is not None:
                boarded = list(boarding)
            results = self.ptr.run(after, boarded, boarding is None)
            cache = {}
            return [self.materialize(profile, results.at(i), cache) for i in range(results.size())]
        except BaseException:
            # Bounds may already have changed; never reuse a partially completed departure.
            self.failed = True
            raise
        finally:
            # All predecessors belong to THIS call. Only numeric bounds survive a departure.
            self.ptr.clear_labels()

    def stats(self):
        return dict(peak_labels=self.ptr.peak_labels, peak_label_capacity=self.ptr.peak_label_capacity,
                    peak_label_capacity_bytes=self.ptr.peak_label_capacity * sizeof(CLabel),
                    live_labels_after_return=self.ptr.live_labels(),
                    peak_permission_words=self.query.ptr.peak_words,
                    peak_walk_states=self.ptr.peak_walk_states, state_payload_bytes=self.ptr.payload_bytes())
