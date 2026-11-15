"""Old atomic bytes-returning API. Requests are pinned to a supplied ETag."""
from .service import locator, ReadInterrupted

class Client:
    def __init__(self, world):
        self.world = world

    def open(self, signed_url, etag, offset):
        c, k, token = locator(signed_url)
        self.world.authorize(token)
        self.world.object(c, k, etag)
        return Stream(self.world, c, k, token, etag, offset)

class Stream:
    def __init__(self, world, container, key, token, etag, offset):
        self.w, self.c, self.k = world, container, key
        self.token, self.etag, self.pos = token, etag, offset

    def read(self, count):
        try:
            data, fault = self.w.begin_read(1, self.c, self.k, self.etag,
                                            self.token, self.pos, count)
            if fault is not None:
                raise ReadInterrupted('old call returned no bytes')
            out = data[self.pos:self.pos + count]
            self.pos += len(out)
            return out
        finally:
            self.w.end_read()

    def close(self):
        pass
