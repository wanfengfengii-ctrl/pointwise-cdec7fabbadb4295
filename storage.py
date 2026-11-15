"""Existing SDK1 byte delivery boundary."""
from vendor import sdk1

class Storage:
    def __init__(self, world):
        self.client = sdk1.Client(world)

    def chunks(self, signed_url, etag, offset):
        stream = self.client.open(signed_url, etag, offset)
        try:
            while True:
                chunk = stream.read(23)
                if not chunk:
                    return
                yield chunk
        finally:
            stream.close()
