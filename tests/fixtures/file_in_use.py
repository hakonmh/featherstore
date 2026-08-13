"""Keep table partition files open in another process.

This recreates the Windows "file in use" situation: one process holds
partition handles open while the test process drops or overwrites the table.
"""

import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

_POLL_INTERVAL_S = 0.05
_DEFAULT_TIMEOUT_S = 10


@contextmanager
def hold_partition_files(table_path, *, timeout_s=_DEFAULT_TIMEOUT_S):
    """Hold every ``*.feather`` file under ``table_path`` open in a child process."""
    table_path = Path(table_path).resolve()
    ready_path, release_path, sync_dir = _sync_paths(table_path)
    _clear_sync_files(ready_path, release_path)

    process = _start_holder(table_path, ready_path, release_path)
    holder_error = None
    try:
        _wait_until_ready(process, ready_path, timeout_s=timeout_s)
        yield
    finally:
        holder_error = _stop_holder(
            process, ready_path, release_path, sync_dir, timeout_s=timeout_s
        )
    if holder_error is not None:
        raise holder_error


def _sync_paths(table_path):
    sync_dir = table_path.parent / f".file_in_use_{table_path.name}"
    sync_dir.mkdir(exist_ok=True)
    return sync_dir / "ready", sync_dir / "release", sync_dir


def _clear_sync_files(*paths):
    for path in paths:
        if path.exists():
            path.unlink()


def _start_holder(table_path, ready_path, release_path):
    return subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            str(table_path),
            str(ready_path),
            str(release_path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _wait_until_ready(process, ready_path, *, timeout_s):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stderr = process.stderr.read() if process.stderr else ""
            raise RuntimeError(f"File-holder process exited early: {stderr}")
        if ready_path.exists():
            return
        time.sleep(_POLL_INTERVAL_S)
    process.kill()
    raise TimeoutError("Timed out waiting for file-holder process to become ready")


def _stop_holder(process, ready_path, release_path, sync_dir, *, timeout_s):
    release_path.write_text("release", encoding="utf-8")
    try:
        _, stderr = process.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        process.kill()
        _, stderr = process.communicate(timeout=timeout_s)

    _clear_sync_files(ready_path, release_path)
    if sync_dir.exists() and not any(sync_dir.iterdir()):
        sync_dir.rmdir()

    if process.returncode not in (0, None):
        return RuntimeError(
            f"File-holder process exited with {process.returncode}: {stderr}"
        )
    return None


def _hold_partitions_until_released(table_path, ready_path, release_path):
    handles = _open_partition_handles(table_path)
    try:
        ready_path.write_text("ready", encoding="utf-8")
        while not release_path.exists():
            time.sleep(_POLL_INTERVAL_S)
    finally:
        _close_handles(handles)


def _open_partition_handles(table_path):
    ctypes, kernel32 = _windows_api()
    generic_read = 0x80000000
    share_read_write_delete = 0x1 | 0x2 | 0x4
    open_existing = 3
    invalid_handle = ctypes.c_void_p(-1).value

    handles = []
    for partition in sorted(Path(table_path).glob("*.feather")):
        handle = kernel32.CreateFileW(
            str(partition),
            generic_read,
            share_read_write_delete,
            None,
            open_existing,
            0,
            None,
        )
        if handle == invalid_handle:
            _close_handles(handles)
            raise ctypes.WinError(ctypes.get_last_error())
        handles.append(handle)
    return handles


def _close_handles(handles):
    _, kernel32 = _windows_api()
    for handle in handles:
        kernel32.CloseHandle(handle)


def _windows_api():
    import ctypes

    return ctypes, ctypes.WinDLL("kernel32", use_last_error=True)


if __name__ == "__main__":
    _hold_partitions_until_released(
        Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    )
