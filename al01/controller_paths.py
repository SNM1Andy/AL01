"""Fail closed on stable malformed private-controller layouts.

Application checks only: no OS confinement and no hostile concurrent-writer
guarantee. Inspect lexical paths before resolving or creating any component.
"""
import os
import stat
from pathlib import Path

MAX_DIRECTORY_ENTRIES = 256


def _entries(directory):
    # scandir streams names; Path.iterdir may eagerly list the whole directory
    # on some supported Python versions. Inspect at most one overflow entry.
    with os.scandir(directory) as entries:
        for index, entry in enumerate(entries):
            if index >= MAX_DIRECTORY_ENTRIES:
                raise ValueError('Private controller directory entry budget exceeded')
            yield directory / entry.name


def _info(path):
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
        raise ValueError('Private controller path must not be a link or reparse point')
    return info


def check_directory(path):
    path = Path(os.path.abspath(path))
    for item in reversed((path,) + tuple(path.parents)):
        info = _info(item)
        if info is not None and not stat.S_ISDIR(info.st_mode):
            raise ValueError('Private controller ancestor must be a plain directory')


def check_file(path):
    path = Path(os.path.abspath(path))
    check_directory(path.parent)
    info = _info(path)
    if info is not None and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1):
        raise ValueError('Private controller file must be regular and singly linked')


def check_session_layout(directory):
    directory = Path(os.path.abspath(directory))
    check_directory(directory)
    if not directory.exists():
        return
    for item in _entries(directory):
        if item.name == 'receipts':
            check_directory(item)
            for receipt in _entries(item):
                check_file(receipt)
        else:
            check_file(item)


def check_runner_layout(directory):
    # Fixed structural depth: runner/session/receipts, never recursive descent.
    directory = Path(os.path.abspath(directory))
    check_directory(directory)
    if not directory.exists():
        return
    for item in _entries(directory):
        if item.name == 'session':
            check_session_layout(item)
        else:
            check_file(item)
