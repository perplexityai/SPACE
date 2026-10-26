#!/usr/bin/env python3
"""Bounded CUDA allocation scrub; this is not firmware or full-framebuffer proof."""

from __future__ import annotations

import argparse
import ctypes
import json
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Protocol, Self
from uuid import UUID

MIB = 1024 * 1024
REPORT_PREFIX = "SPACE_GPU_SANITIZER_REPORT_V1 "


class ScrubError(RuntimeError):
    pass


class AllocationExhausted(ScrubError):
    pass


class MemoryDriver(Protocol):
    def memory_info(self) -> tuple[int, int]: ...
    def allocate(self, size: int) -> int: ...
    def clear(self, address: int, value: int, size: int) -> None: ...
    def synchronize(self) -> None: ...
    def read(self, address: int, size: int) -> bytes: ...
    def release(self, address: int) -> None: ...


@dataclass(frozen=True)
class ScrubLimits:
    minimum_bytes: int
    maximum_bytes: int
    timeout_seconds: int = 120
    allocation_chunk: int = 256 * MIB
    verification_chunk: int = 8 * MIB
    minimum_allocation: int = MIB
    maximum_allocations: int = 1024

    def __post_init__(self) -> None:
        if not (
            0 < self.minimum_bytes <= self.maximum_bytes <= 1024 * 1024 * MIB
            and 0 < self.minimum_allocation <= self.allocation_chunk
            and 0 < self.verification_chunk <= self.allocation_chunk
            and 0 < self.maximum_allocations <= 1024
            and 0 < self.timeout_seconds <= 600
        ):
            raise ScrubError("invalid scrub limits")


@dataclass(frozen=True)
class Coverage:
    reported_total_bytes: int
    free_bytes_before: int
    overwritten_and_verified_bytes: int
    uncovered_reported_bytes: int
    allocation_count: int
    verified_passes: int


def scrub(
    driver: MemoryDriver,
    limits: ScrubLimits,
    clock: Callable[[], float] = time.monotonic,
) -> Coverage:
    deadline = clock() + limits.timeout_seconds

    def check_deadline() -> None:
        if clock() >= deadline:
            raise ScrubError("scrub deadline exceeded")

    free, total = driver.memory_info()
    if not (limits.minimum_bytes <= free <= total <= limits.maximum_bytes):
        raise ScrubError("reported memory does not fit procedure limits")
    allocations: list[tuple[int, int]] = []
    allocated = 0
    chunk = limits.allocation_chunk
    try:
        # Hold every allocation until all passes finish, so repeated allocations
        # cannot count the same pages as distinct coverage.
        while free - allocated >= limits.minimum_allocation:
            check_deadline()
            if len(allocations) >= limits.maximum_allocations:
                raise ScrubError("allocation count exceeds procedure bound")
            size = min(chunk, free - allocated)
            size -= size % limits.minimum_allocation
            try:
                address = driver.allocate(size)
            except AllocationExhausted:
                if size == limits.minimum_allocation:
                    break
                chunk = max(limits.minimum_allocation, size // 2)
                continue
            allocations.append((address, size))
            allocated += size
        if allocated < limits.minimum_bytes:
            raise ScrubError("insufficient verified memory coverage")
        for pattern in (0xA5, 0x5A, 0):
            for address, size in allocations:
                check_deadline()
                driver.clear(address, pattern, size)
            driver.synchronize()
            expected = bytes([pattern]) * limits.verification_chunk
            for address, size in allocations:
                for offset in range(0, size, limits.verification_chunk):
                    check_deadline()
                    length = min(limits.verification_chunk, size - offset)
                    if driver.read(address + offset, length) != expected[:length]:
                        raise ScrubError("device memory readback mismatch")
        check_deadline()
        return Coverage(total, free, allocated, total - allocated, len(allocations), 3)
    finally:
        failures = []
        for address, _ in reversed(allocations):
            try:
                driver.release(address)
            except ScrubError as error:
                failures.append(error)
        if failures:
            raise ExceptionGroup("CUDA allocation cleanup failed", failures)


class CudaDriver:
    """The pinned libcuda supplies the versioned driver API ABI used here."""

    def __init__(self) -> None:
        self.library = ctypes.CDLL("libcuda.so.1")
        self.context = ctypes.c_void_p()
        self._init = self._api("cuInit", [ctypes.c_uint])
        self._count = self._api("cuDeviceGetCount", [ctypes.POINTER(ctypes.c_int)])
        self._device = self._api(
            "cuDeviceGet", [ctypes.POINTER(ctypes.c_int), ctypes.c_int]
        )
        self._create = self._api(
            "cuCtxCreate_v2",
            [ctypes.POINTER(ctypes.c_void_p), ctypes.c_uint, ctypes.c_int],
        )
        self._destroy = self._api("cuCtxDestroy_v2", [ctypes.c_void_p])
        self._info = self._api("cuMemGetInfo_v2", [ctypes.POINTER(ctypes.c_size_t)] * 2)
        self._allocate = self._api(
            "cuMemAlloc_v2", [ctypes.POINTER(ctypes.c_uint64), ctypes.c_size_t]
        )
        self._clear = self._api(
            "cuMemsetD8_v2", [ctypes.c_uint64, ctypes.c_ubyte, ctypes.c_size_t]
        )
        self._synchronize = self._api("cuCtxSynchronize", [])
        self._read = self._api(
            "cuMemcpyDtoH_v2", [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_size_t]
        )
        self._release = self._api("cuMemFree_v2", [ctypes.c_uint64])

    def _api(self, name: str, arguments: list):
        function = getattr(self.library, name)
        function.argtypes = arguments
        function.restype = ctypes.c_int

        def call(*values) -> None:
            result = function(*values)
            if result == 2 and name == "cuMemAlloc_v2":
                raise AllocationExhausted("CUDA memory allocation exhausted")
            if result != 0:
                raise ScrubError(f"{name} returned CUDA status {result}")

        return call

    def __enter__(self) -> Self:
        self._init(0)
        count = ctypes.c_int()
        self._count(ctypes.byref(count))
        if count.value != 1:
            raise ScrubError("procedure requires exactly one visible CUDA device")
        device = ctypes.c_int()
        self._device(ctypes.byref(device), 0)
        self._create(ctypes.byref(self.context), 0, device)
        return self

    def __exit__(self, *_exc) -> None:
        self._destroy(self.context)

    def memory_info(self) -> tuple[int, int]:
        free, total = ctypes.c_size_t(), ctypes.c_size_t()
        self._info(ctypes.byref(free), ctypes.byref(total))
        return free.value, total.value

    def allocate(self, size: int) -> int:
        address = ctypes.c_uint64()
        self._allocate(ctypes.byref(address), size)
        return address.value

    def clear(self, address: int, value: int, size: int) -> None:
        self._clear(address, value, size)

    def synchronize(self) -> None:
        self._synchronize()

    def read(self, address: int, size: int) -> bytes:
        output = ctypes.create_string_buffer(size)
        self._read(output, address, size)
        return output.raw

    def release(self, address: int) -> None:
        self._release(address)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--attempt", type=UUID, required=True)
    parser.add_argument("--minimum-mib", type=int, required=True)
    parser.add_argument("--maximum-mib", type=int, required=True)
    args = parser.parse_args()
    report = {"attempt": str(args.attempt), "coverage_kind": "cuda_allocations"}
    try:
        limits = ScrubLimits(args.minimum_mib * MIB, args.maximum_mib * MIB)
        with CudaDriver() as driver:
            coverage = scrub(driver, limits)
        report.update(status="scrub_completed", coverage=asdict(coverage))
        result = 0
    except (ScrubError, OSError, AttributeError, ExceptionGroup) as error:
        report.update(status="failed", error=str(error)[:512])
        result = 1
    print(REPORT_PREFIX + json.dumps(report, sort_keys=True), flush=True)
    return result


if __name__ == "__main__":
    sys.exit(main())
