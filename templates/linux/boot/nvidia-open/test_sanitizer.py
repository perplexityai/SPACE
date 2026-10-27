import unittest
from dataclasses import replace

from sanitizer import AllocationExhausted, ScrubError, ScrubLimits, scrub


class Memory:
    def __init__(self, *, corrupt=False, release_failure=False):
        self.allocations = {}
        self.released = []
        self.clears = []
        self.next_address = 1024
        self.corrupt = corrupt
        self.release_failure = release_failure

    def memory_info(self):
        return 40, 64

    def allocate(self, size):
        if sum(map(len, self.allocations.values())) + size > 32:
            raise AllocationExhausted()
        address = self.next_address
        self.next_address += size + 1024
        self.allocations[address] = bytearray([0xFF] * size)
        return address

    def clear(self, address, value, size):
        self.clears.append((address, value, size))
        self.allocations[address][:] = bytes([value]) * size

    def synchronize(self):
        pass

    def read(self, address, size):
        for base, value in self.allocations.items():
            if base <= address < base + len(value):
                offset = address - base
                output = bytes(value[offset : offset + size])
                return bytes([output[0] ^ 1]) + output[1:] if self.corrupt else output
        raise AssertionError("read outside a held allocation")

    def release(self, address):
        self.released.append(bytes(self.allocations.pop(address)))
        if self.release_failure:
            raise ScrubError("injected release failure")


class ScrubTests(unittest.TestCase):
    limits = ScrubLimits(
        24, 64, allocation_chunk=16, verification_chunk=3, minimum_allocation=1
    )

    def test_coverage_counts_held_memory_and_leaves_verified_zeros(self):
        driver = Memory()
        report = scrub(driver, self.limits)
        self.assertEqual(report.overwritten_and_verified_bytes, 32)
        self.assertEqual(report.uncovered_reported_bytes, 32)
        self.assertEqual(report.free_bytes_before, 40)
        self.assertEqual(report.verified_passes, 3)
        self.assertEqual(report.allocation_count, 2)
        self.assertEqual(driver.released, [bytes(16), bytes(16)])
        self.assertEqual(
            [value for _, value, _ in driver.clears], [0xA5, 0xA5, 0x5A, 0x5A, 0, 0]
        )
        self.assertFalse(driver.allocations)

    def test_bad_coverage_corruption_time_and_cleanup_never_report_success(self):
        for failure in (
            "coverage",
            "corruption",
            "deadline",
            "count",
            "cleanup",
            "shape",
        ):
            with self.subTest(failure=failure):
                driver = Memory(
                    corrupt=failure == "corruption",
                    release_failure=failure == "cleanup",
                )
                limits = self.limits
                clock = lambda: 0
                if failure == "coverage":
                    limits = replace(limits, minimum_bytes=36)
                elif failure == "deadline":
                    ticks = iter(range(100))
                    clock = ticks.__next__
                    limits = replace(limits, timeout_seconds=3)
                elif failure == "count":
                    limits = replace(limits, maximum_allocations=1)
                elif failure == "shape":
                    limits = replace(limits, maximum_bytes=48)
                with self.assertRaises((ScrubError, ExceptionGroup)):
                    scrub(driver, limits, clock)
                self.assertFalse(driver.allocations)


if __name__ == "__main__":
    unittest.main()
