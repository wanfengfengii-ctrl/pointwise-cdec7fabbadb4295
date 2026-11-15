"""New stream API: immutable opening credential, conditional byte ranges."""
from .service import ReadInterrupted

class Client:
    def __init__(self, world):
        self.world = world

    def open(self, container, key, credential, *, if_match, offset, length=None):
        if not isinstance(offset, int) or offset < 0 or (length is not None and length < 0):
            raise ValueError('range')
        self.world.trace.append(dict(op='open', sdk=2, at=self.world.now,
                                     token=credential.token, container=container,
                                     key=key, etag=if_match, offset=offset,
                                     length=length))
        self.world.authorize(credential.token)
        self.world.object(container, key, if_match)
        return Stream(self.world, container, key, credential, if_match, offset, length)

class Stream:
    def __init__(self, world, container, key, credential, etag, offset, length):
        self.w, self.c, self.k = world, container, key
        self.credential, self.etag, self.pos = credential, etag, offset
        self.stop = None if length is None else offset + length
        self.closed = False

    def readinto(self, buffer):
        if self.closed:
            raise ValueError('closed stream')
        try:
            data, fault = self.w.begin_read(2, self.c, self.k, self.etag,
                                            self.credential.token, self.pos, len(buffer))
            limit = min(len(buffer), self.w.max_read)
            if self.stop is not None:
                limit = min(limit, max(0, self.stop - self.pos))
            out = data[self.pos:self.pos + limit]
            if fault is not None:
                n = min(fault, len(out))
                buffer[:n] = out[:n]
                self.pos += n
                raise ReadInterrupted('buffer changed; no successful count')
            buffer[:len(out)] = out
            self.pos += len(out)
            return len(out)
        finally:
            self.w.end_read()

    def close(self):
        self.closed = True
        self.w.trace.append(dict(op='close', sdk=2, at=self.w.now))
