"""Primitive Python boundary tests; no frontend dependency or extra test tooling."""

import gc
import math
import unittest
import weakref
from array import array
from concurrent.futures import ThreadPoolExecutor

from ztm_routing import NativeState, PreparedNet, PreparedQuery


def network() -> PreparedNet:
    return PreparedNet(
        2,
        array("i", [100, 200]),
        (100, 200),
        [100, 200],
        [-1, -1],
        [0],
        [0],
        array("d", [0.0, 100.0]),
        [[0, 1]],
        [[0, 1]],
        [[0.0, 100.0]],
        [[(0, 0, [100], [0])], []],
        [[], []],
        [],
    )


def state(query: PreparedQuery) -> NativeState:
    return NativeState(query, [0], [], [], [(1, -1)], False, 10800)


class BindingTests(unittest.TestCase):
    def test_sequences_records_and_statistics(self) -> None:
        window = state(PreparedQuery(network(), [0.0, 0.0], 0))
        self.assertEqual(
            window.run(100, (0,)),
            [[(100, 0, -1, -1, -1, -1, -1, 0), (200, 1, 0, 0, 1, -1, -1, 0)]],
        )
        stats = window.stats()
        self.assertEqual(stats["live_labels_after_return"], 0)
        self.assertGreater(stats["peak_label_capacity_bytes"], 0)
        self.assertGreater(stats["state_payload_bytes"], 0)
        self.assertEqual(window.run(0), [])

    def test_arc_ownership_does_not_retain_python_owners(self) -> None:
        net = network()
        query = PreparedQuery(net, [0.0, 0.0], 0)
        window = state(query)
        refs = [weakref.ref(net), weakref.ref(query)]
        del net, query
        gc.collect()
        self.assertTrue(all(ref() is None for ref in refs))
        self.assertEqual(window.run(0)[0][-1][0], 200)

    def test_signed_unsigned_and_index_failures(self) -> None:
        with self.assertRaises(OverflowError):
            PreparedQuery(network(), [0.0, 0.0], -1)
        for board, alight in [(-1, 1), (0, -1), (0, 2)]:
            with self.subTest(board=board, alight=alight), self.assertRaises(IndexError):
                network().late(board, alight)
        with self.assertRaises(OverflowError):
            network().late(2**100, 1)
        with self.assertRaises(TypeError):
            PreparedQuery(None, [], 0)
        with self.assertRaises(TypeError):
            NativeState(None, [], [], [], [], False, 0)
        for target in ([math.nan, 0.0], [-1.0, 0.0], [0.0]):
            with self.subTest(target=target), self.assertRaises(ValueError):
                PreparedQuery(network(), target, 0)

    def test_all_run_boundary_errors_invalidate_window(self) -> None:
        for after, boarding, error in [
            (2**100, None, OverflowError),
            (2**40, None, ValueError),
            (0, [-1], IndexError),
            (0, [2**100], OverflowError),
            (0, "bad", TypeError),
        ]:
            with self.subTest(after=after, boarding=boarding):
                window = state(PreparedQuery(network(), [0.0, 0.0], 0))
                with self.assertRaises(error):
                    window.run(after, boarding)
                self.assertEqual(window.stats()["live_labels_after_return"], 0)
                with self.assertRaisesRegex(RuntimeError, "cannot be reused"):
                    window.run(0)

    def test_copied_network_and_explicit_invalidation(self) -> None:
        late_base = [100, 200]
        net = PreparedNet(
            2,
            late_base,
            [100, 200],
            [100, 200],
            [-1, -1],
            [0],
            [0],
            [0.0, 100.0],
            [[0, 1]],
            [[0, 1]],
            [[0.0, 100.0]],
            [[(0, 0, [100], [0])], []],
            [[], []],
            [],
        )
        late_base[0] = 1000
        self.assertEqual(net.late(0, 1), 200)
        window = state(PreparedQuery(net, [0.0, 0.0], 0))
        window.invalidate()
        with self.assertRaises(RuntimeError):
            window.run(0)

    def test_shared_query_concurrent_windows_and_atomic_getter(self) -> None:
        query = PreparedQuery(network(), [0.0, 0.0], 2)

        def search(_: int) -> list[list[tuple[int, ...]]]:
            window = state(query)
            result = window.run(100)
            for after in range(99, -1, -1):
                self.assertEqual(window.run(after), [])
                self.assertLessEqual(query.peak_permission_words(), 2)
            return result

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(search, range(16)))
        self.assertTrue(all(result == results[0] for result in results))


if __name__ == "__main__":
    unittest.main()
