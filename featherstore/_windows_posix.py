"""POSIX-style file unlinking for Windows.

On Windows 10 and later, a file name can be removed from the directory while
other handles that shared delete access remain open. That matches Unix unlink
behavior and avoids many WinError 32 failures during table cleanup.

When the OS or filesystem does not support the API, callers fall back to
``os.remove``. Real access and sharing errors are never treated as unsupported.
"""

import ctypes
import os
import platform
import sys

_DELETE_ACCESS = 0x00010000
_SHARE_READ_WRITE_DELETE = 0x00000001 | 0x00000002 | 0x00000004
_OPEN_EXISTING = 3
_INVALID_HANDLE = ctypes.c_void_p(-1).value

_FILE_DISPOSITION_INFO_EX = 21
_POSIX_DELETE_FLAGS = 0x00000001 | 0x00000002  # DELETE | POSIX_SEMANTICS

# Capability probe failures: older Windows or filesystems without POSIX delete.
_UNSUPPORTED_WINERRORS = frozenset(
    {
        1,  # ERROR_INVALID_FUNCTION
        50,  # ERROR_NOT_SUPPORTED
        87,  # ERROR_INVALID_PARAMETER
    }
)


class _DispositionInfo(ctypes.Structure):
    _fields_ = [("Flags", ctypes.c_ulong)]


_kernel32 = None


def _remove_path_posix(path):
    if _can_attempt_posix_delete():
        try:
            _unlink_with_posix_semantics(path)
            return
        except OSError as error:
            if not _is_unsupported_posix_delete(error):
                raise
    os.remove(path)


def _can_attempt_posix_delete():
    return platform.system() == "Windows" and sys.getwindowsversion().major >= 10


def _is_unsupported_posix_delete(error):
    return getattr(error, "winerror", None) in _UNSUPPORTED_WINERRORS


def _unlink_with_posix_semantics(path):
    handle = _open_for_delete(path)
    try:
        _request_posix_delete(handle)
    finally:
        _close_handle(handle)


def _open_for_delete(path):
    handle = _get_kernel32().CreateFileW(
        path,
        _DELETE_ACCESS,
        _SHARE_READ_WRITE_DELETE,
        None,
        _OPEN_EXISTING,
        0,
        None,
    )
    if handle == _INVALID_HANDLE:
        raise ctypes.WinError(ctypes.get_last_error())
    return handle


def _request_posix_delete(handle):
    info = _DispositionInfo(_POSIX_DELETE_FLAGS)
    succeeded = _get_kernel32().SetFileInformationByHandle(
        handle,
        _FILE_DISPOSITION_INFO_EX,
        ctypes.byref(info),
        ctypes.sizeof(info),
    )
    if not succeeded:
        raise ctypes.WinError(ctypes.get_last_error())


def _close_handle(handle):
    _get_kernel32().CloseHandle(handle)


def _get_kernel32():
    global _kernel32
    if _kernel32 is None:
        _kernel32 = _load_kernel32()
    return _kernel32


def _load_kernel32():
    dll = ctypes.WinDLL("kernel32", use_last_error=True)
    dll.CreateFileW.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    dll.CreateFileW.restype = ctypes.c_void_p
    dll.SetFileInformationByHandle.argtypes = [
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_uint32,
    ]
    dll.SetFileInformationByHandle.restype = ctypes.c_bool
    dll.CloseHandle.argtypes = [ctypes.c_void_p]
    dll.CloseHandle.restype = ctypes.c_bool
    return dll
