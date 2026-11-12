"""Explicitly opted-in application-level readonly broker, NOT an OS sandbox.

The process retains ambient same-user rights. No privilege or security-setting
changes are made. Construction is inert; handle() is the only filesystem entry.
"""
import contextlib
import errno
import hashlib
import json
import ntpath
import os
import stat
import uuid

from . import folder_contracts as c


class _Refusal(Exception):
    def __init__(self, status, reason):
        self.status, self.reason = status, reason


def _reject(reason='UNSAFE_ENTRY', status='DENIED'):
    raise _Refusal(status, reason)


class _Posix:
    def __init__(self, root):
        self.path = root

    def _open(self, name, parent=None, directory=False):
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, 'O_CLOEXEC', 0)
        if directory:
            flags |= os.O_DIRECTORY
        return os.open(name, flags, dir_fd=parent)

    @contextlib.contextmanager
    def root(self):
        # Walk every absolute component without following symlinks, not just
        # the final root component. No user-supplied request path is used here.
        fd = self._open('/', directory=True)
        try:
            for component in self.path.split('/')[1:]:
                if not component:
                    continue
                child = self._open(component, fd, True)
                os.close(fd)
                fd = child
            yield fd
        finally:
            os.close(fd)

    def metadata(self, handle):
        s = os.fstat(handle)
        if stat.S_ISDIR(s.st_mode):
            kind = 'directory'
        elif stat.S_ISREG(s.st_mode) and s.st_nlink == 1:
            kind = 'file'
        else:
            _reject()
        return {'kind': kind, 'size': s.st_size if kind == 'file' else None,
                'identity': (s.st_dev, s.st_ino),
                'stamp': (s.st_dev, s.st_ino, s.st_mode, s.st_nlink, s.st_size, s.st_mtime_ns, s.st_ctime_ns)}

    @contextlib.contextmanager
    def entry(self, root, relative):
        c.validate_relative_path(relative)
        opened = []
        current = root
        root_dev = self.metadata(root)['identity'][0]
        try:
            parts = relative.split('/')
            for i, part in enumerate(parts):
                prior = os.stat(part, dir_fd=current, follow_symlinks=False)
                if not (stat.S_ISDIR(prior.st_mode) or stat.S_ISREG(prior.st_mode)):
                    _reject()
                if stat.S_ISREG(prior.st_mode) and prior.st_nlink != 1:
                    _reject()
                current = self._open(part, current, i < len(parts) - 1)
                opened.append(current)
                meta = self.metadata(current)
                if meta['identity'][0] != root_dev or meta['identity'] != (prior.st_dev, prior.st_ino):
                    _reject()
            yield current
        finally:
            for fd in reversed(opened):
                os.close(fd)

    def names(self, handle):
        result = []
        with os.scandir(handle) as iterator:
            for entry in iterator:
                result.append(entry.name)
                if len(result) > c.MAX_ENTRIES:
                    _reject('ENTRY_LIMIT', 'LIMIT_EXCEEDED')
        return sorted(result)

    def read(self, handle):
        chunks, size = [], 0
        while size <= c.MAX_TEXT_BYTES:
            raw = os.read(handle, min(8192, c.MAX_TEXT_BYTES + 1 - size))
            if not raw:
                return b''.join(chunks)
            chunks.append(raw)
            size += len(raw)
        _reject('BYTE_LIMIT', 'LIMIT_EXCEEDED')


class _Windows:
    """Win32 handle checks; no Sandbox, ACL edits, elevation, or network setup."""
    def __init__(self, root):
        import ctypes
        from ctypes import wintypes as w
        self.ct, self.w = ctypes, w
        self.kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        class Info(ctypes.Structure):
            _fields_ = [('attributes', w.DWORD), ('creation', w.FILETIME), ('access', w.FILETIME),
                        ('write', w.FILETIME), ('volume', w.DWORD), ('size_high', w.DWORD),
                        ('size_low', w.DWORD), ('links', w.DWORD), ('index_high', w.DWORD), ('index_low', w.DWORD)]
        self.Info = Info
        k = self.kernel
        k.CreateFileW.argtypes = [w.LPCWSTR, w.DWORD, w.DWORD, w.LPVOID, w.DWORD, w.DWORD, w.HANDLE]
        k.CreateFileW.restype = w.HANDLE
        k.CloseHandle.argtypes = [w.HANDLE]; k.CloseHandle.restype = w.BOOL
        k.GetFileInformationByHandle.argtypes = [w.HANDLE, ctypes.POINTER(Info)]; k.GetFileInformationByHandle.restype = w.BOOL
        k.GetFinalPathNameByHandleW.argtypes = [w.HANDLE, w.LPWSTR, w.DWORD, w.DWORD]; k.GetFinalPathNameByHandleW.restype = w.DWORD
        k.GetLongPathNameW.argtypes = [w.LPCWSTR, w.LPWSTR, w.DWORD]; k.GetLongPathNameW.restype = w.DWORD
        k.GetFileType.argtypes = [w.HANDLE]; k.GetFileType.restype = w.DWORD
        k.ReadFile.argtypes = [w.HANDLE, w.LPVOID, w.DWORD, ctypes.POINTER(w.DWORD), w.LPVOID]; k.ReadFile.restype = w.BOOL
        self.path = root

    def _error(self):
        raise self.ct.WinError(self.ct.get_last_error())

    def _final(self, handle):
        buffer = self.ct.create_unicode_buffer(32768)
        size = self.kernel.GetFinalPathNameByHandleW(handle, buffer, len(buffer), 0)
        if not size or size >= len(buffer):
            self._error()
        value = buffer.value
        if value.startswith('\\\\?\\'):
            value = value[4:]
        return ntpath.normcase(ntpath.normpath(value))

    def _long_name(self, path):
        # Expand Windows 8.3 spelling only. Handle metadata still rejects all
        # reparse points; final-path, device and ancestor checks remain intact.
        buffer = self.ct.create_unicode_buffer(32768)
        size = self.kernel.GetLongPathNameW(path, buffer, len(buffer))
        if not size or size >= len(buffer):
            self._error()
        return ntpath.normcase(ntpath.normpath(buffer.value))

    def _open(self, path):
        # OPEN_EXISTING + OPEN_REPARSE_POINT + BACKUP_SEMANTICS. Share READ only:
        # retain all ancestor handles to prevent replacement while inspecting.
        h = self.kernel.CreateFileW(path, 0x80000000, 1, None, 3, 0x00200000 | 0x02000000, None)
        if h == self.ct.c_void_p(-1).value:
            self._error()
        try:
            self.metadata(h)
            if self._final(h) != self._long_name(path):
                _reject()
            return h
        except BaseException:
            self.kernel.CloseHandle(h)
            raise

    def metadata(self, handle):
        info = self.Info()
        if not self.kernel.GetFileInformationByHandle(handle, self.ct.byref(info)):
            self._error()
        if self.kernel.GetFileType(handle) != 1 or info.attributes & 0x400:
            _reject()  # Not disk, or any reparse tag including cloud placeholders.
        directory = bool(info.attributes & 0x10)
        if not directory and info.links != 1:
            _reject()
        size = (info.size_high << 32) | info.size_low
        identity = (info.volume, info.index_high, info.index_low)
        stamp = identity + (info.attributes, info.links, size, info.creation.dwHighDateTime,
                           info.creation.dwLowDateTime, info.write.dwHighDateTime, info.write.dwLowDateTime)
        return {'kind': 'directory' if directory else 'file', 'size': None if directory else size,
                'identity': identity, 'stamp': stamp}

    @contextlib.contextmanager
    def root(self):
        drive, tail = ntpath.splitdrive(self.path)
        if len(drive) != 2 or drive[1] != ':' or not tail.startswith('\\'):
            _reject('OUTSIDE_SCOPE')
        handles = []
        try:
            path = drive + '\\'
            handles.append(self._open(path))
            for part in filter(None, tail.split('\\')):
                path = ntpath.join(path, part)
                handle = self._open(path)
                handles.append(handle)
                if self.metadata(handle)['kind'] != 'directory':
                    _reject()
            yield handles[-1]
        finally:
            for handle in reversed(handles):
                self.kernel.CloseHandle(handle)

    @contextlib.contextmanager
    def entry(self, root, relative):
        c.validate_relative_path(relative)
        handles = []
        root_path = self._final(root)
        current = root_path
        try:
            for index, part in enumerate(relative.split('/')):
                current = ntpath.join(current, part)
                handle = self._open(current)
                handles.append(handle)
                final = self._final(handle)
                if not final.startswith(root_path.rstrip('\\') + '\\'):
                    _reject('OUTSIDE_SCOPE')
                if index < len(relative.split('/')) - 1 and self.metadata(handle)['kind'] != 'directory':
                    _reject()
                if self.metadata(handle)['identity'][0] != self.metadata(root)['identity'][0]:
                    _reject()
            yield handles[-1]
        finally:
            for handle in reversed(handles):
                self.kernel.CloseHandle(handle)

    def names(self, handle):
        result = []
        with os.scandir(self._final(handle)) as iterator:
            for entry in iterator:
                result.append(entry.name)
                if len(result) > c.MAX_ENTRIES:
                    _reject('ENTRY_LIMIT', 'LIMIT_EXCEEDED')
        return sorted(result)

    def read(self, handle):
        raw = self.ct.create_string_buffer(c.MAX_TEXT_BYTES + 1)
        count = self.w.DWORD()
        if not self.kernel.ReadFile(handle, raw, len(raw), self.ct.byref(count), None):
            self._error()
        if count.value > c.MAX_TEXT_BYTES:
            _reject('BYTE_LIMIT', 'LIMIT_EXCEEDED')
        return raw.raw[:count.value]


class FolderBroker:
    """Fixed-root authority chosen by host, never by an incoming request.

    Opaque entries are session-local. A fresh listing expires old references.
    Application checks cannot constrain malicious code with this user's rights.
    """
    def __init__(self, root, *, application_checks_acknowledged=False, scope_ref=None):
        if type(root) is not str or not root or '\x00' in root:
            raise ValueError('explicit root text required')
        if type(application_checks_acknowledged) is not bool:
            raise ValueError('explicit Boolean acknowledgment required')
        self.root_path = os.path.abspath(root)
        self.enabled = application_checks_acknowledged
        self._root_binding = hashlib.sha256(('application-folder-root-v1:' + os.path.normcase(self.root_path)).encode('utf8')).hexdigest()
        self.scope_ref = scope_ref if scope_ref is not None else 'scope_' + self._root_binding[:32]
        c.validate_ref(self.scope_ref, 'scope')
        self._entries = {}
        self._root_identity = None
        self._backend = None

    def export_metadata(self):
        """Pure trusted-controller snapshot; no write or external transmission.

        Relative names are private data. The controller must not publish this
        inventory. This is not a capability grant or a substitute for reopening.
        """
        return json.loads(json.dumps({'schema': 'folder-broker-metadata-1', 'scope_ref': self.scope_ref,
            'root_binding': self._root_binding, 'root_identity': self._root_identity, 'entries': self._entries}))

    def restore_metadata(self, data):
        """Validate/detach bounded controller inventory without filesystem access."""
        def require(ok):
            if not ok:
                raise c.FolderContractError('invalid broker metadata')
        require(type(data) is dict and set(data) == {'schema', 'scope_ref', 'root_binding', 'root_identity', 'entries'})
        require(data['schema'] == 'folder-broker-metadata-1' and data['scope_ref'] == self.scope_ref and data['root_binding'] == self._root_binding)
        identity_length, stamp_length = (3, 10) if os.name == 'nt' else (2, 7)
        def numbers(values, length):
            require(type(values) is list and len(values) == length and all(type(x) is int and abs(x) <= 2**64 for x in values))
            return tuple(values)
        identity = None if data['root_identity'] is None else numbers(data['root_identity'], identity_length)
        entries = data['entries']
        require(type(entries) is dict and len(entries) <= c.MAX_ENTRIES and (not entries or identity is not None))
        restored, names = {}, set()
        for ref, entry in entries.items():
            c.validate_ref(ref, 'entry')
            require(type(entry) is dict and set(entry) == {'path', 'metadata'})
            path = c.validate_relative_path(entry['path'])
            require(path.casefold() not in names)
            names.add(path.casefold())
            meta = entry['metadata']
            require(type(meta) is dict and set(meta) == {'kind', 'size', 'identity', 'stamp'})
            c.validate_entry({'entry_ref': ref, 'relative_path': path, 'kind': meta['kind'], 'byte_size': meta['size']})
            restored[ref] = {'path': path, 'metadata': {'kind': meta['kind'], 'size': meta['size'],
                'identity': numbers(meta['identity'], identity_length), 'stamp': numbers(meta['stamp'], stamp_length)}}
        self._root_identity, self._entries = identity, restored

    def _driver(self):
        if self._backend is None:
            if os.name == 'nt':
                self._backend = _Windows(self.root_path)
            elif os.name == 'posix' and all(hasattr(os, name) for name in ('O_NOFOLLOW', 'O_DIRECTORY')):
                self._backend = _Posix(self.root_path)
            else:
                _reject('ADAPTER_UNAVAILABLE', 'UNAVAILABLE')
        return self._backend

    def _listing(self, driver, root):
        entries, refs = [], {}
        examined = 0
        def walk(handle, prefix=''):
            nonlocal examined
            before = driver.metadata(handle)
            for name in driver.names(handle):
                examined += 1
                if examined > c.MAX_ENTRIES:
                    _reject('ENTRY_LIMIT', 'LIMIT_EXCEEDED')
                relative = prefix + name
                if len(relative.split('/')) > c.MAX_DEPTH:
                    _reject('DEPTH_LIMIT', 'LIMIT_EXCEEDED')
                c.validate_relative_path(relative)
                with driver.entry(root, relative) as child:
                    meta = driver.metadata(child)
                    if meta['kind'] == 'file' and not name.lower().endswith('.txt'):
                        continue
                    if meta['kind'] == 'file' and meta['size'] > c.MAX_TEXT_BYTES:
                        _reject('BYTE_LIMIT', 'LIMIT_EXCEEDED')
                    ref = 'entry_' + uuid.uuid4().hex
                    record = {'entry_ref': ref, 'relative_path': relative, 'kind': meta['kind'], 'byte_size': meta['size']}
                    c.validate_entry(record)
                    entries.append(record)
                    refs[ref] = {'path': relative, 'metadata': meta}
                    if meta['kind'] == 'directory':
                        walk(child, relative + '/')
                    if driver.metadata(child) != meta:
                        _reject('ENTRY_CHANGED', 'STALE_REF')
            if driver.metadata(handle) != before:
                _reject('ENTRY_CHANGED', 'STALE_REF')
        walk(root)
        return entries, refs

    def handle(self, request):
        c.validate_request(request)
        if request['scope_ref'] != self.scope_ref:
            return c.make_result(request, 'DENIED', reason='OUTSIDE_SCOPE')
        denial = c.policy_denial(request)
        if denial is not None:
            return denial
        if not self.enabled:
            return c.make_result(request, 'UNAVAILABLE', reason='ADAPTER_UNAVAILABLE')
        operation = request['operation']
        try:
            # LIST_SCOPE always lists the fixed root; an entry cannot redirect it.
            if operation == 'LIST_SCOPE' and request['entry_ref'] is not None:
                _reject('UNSUPPORTED_OPERATION')
            if request['entry_ref'] is not None and request['entry_ref'] not in self._entries:
                _reject('REFERENCE_EXPIRED', 'STALE_REF')
            driver = self._driver()
            with driver.root() as root:
                root_meta = driver.metadata(root)
                if root_meta['kind'] != 'directory':
                    _reject()
                if self._root_identity is not None and self._root_identity != root_meta['identity']:
                    _reject('ENTRY_CHANGED', 'STALE_REF')
                self._root_identity = root_meta['identity']
                if operation == 'LIST_SCOPE':
                    self._entries = {}
                    entries, refs = self._listing(driver, root)
                    result = c.make_result(request, 'OK', payload={'entries': entries, 'complete': True, 'untrusted_data': True})
                    self._entries = refs
                    return result
                if request['entry_ref'] is None:
                    return c.make_result(request, 'OK', payload={'readable': False, 'listable': True, 'untrusted_data': True})
                entry = self._entries[request['entry_ref']]
                with driver.entry(root, entry['path']) as handle:
                    before = driver.metadata(handle)
                    if before != entry['metadata']:
                        _reject('ENTRY_CHANGED', 'STALE_REF')
                    if operation == 'CHECK_ACCESS':
                        payload = {'readable': before['kind'] == 'file', 'listable': before['kind'] == 'directory', 'untrusted_data': True}
                    else:
                        if before['kind'] != 'file' or not entry['path'].lower().endswith('.txt'):
                            _reject('UNSUPPORTED_OPERATION')
                        raw = driver.read(handle)
                        if len(raw) != before['size'] or driver.metadata(handle) != before:
                            _reject('ENTRY_CHANGED', 'STALE_REF')
                        try:
                            payload = c.text_payload(raw)
                        except c.FolderContractError:
                            return c.make_result(request, 'INVALID_TEXT', reason='BINARY_CONTENT' if b'\0' in raw else 'NOT_STRICT_UTF8')
                    if driver.metadata(root)['identity'] != root_meta['identity']:
                        _reject('ENTRY_CHANGED', 'STALE_REF')
                    return c.make_result(request, 'OK', payload=payload)
        except _Refusal as exc:
            return c.make_result(request, exc.status, reason=exc.reason)
        except c.FolderContractError:
            return c.make_result(request, 'DENIED', reason='UNSAFE_ENTRY')
        except OSError as exc:
            if exc.errno in (errno.ELOOP, errno.ENOTDIR):
                return c.make_result(request, 'DENIED', reason='UNSAFE_ENTRY')
            if isinstance(exc, PermissionError):
                return c.make_result(request, 'DENIED', reason='ACCESS_DENIED')
            if isinstance(exc, FileNotFoundError):
                if request['entry_ref'] is not None:
                    return c.make_result(request, 'STALE_REF', reason='ENTRY_CHANGED')
                return c.make_result(request, 'NOT_FOUND', reason='ENTRY_NOT_FOUND')
            reason = {'LIST_SCOPE': 'LIST_FAILED', 'READ_TEXT': 'READ_FAILED', 'CHECK_ACCESS': 'ACCESS_CHECK_FAILED'}[operation]
            return c.make_result(request, 'IO_ERROR', reason=reason)
