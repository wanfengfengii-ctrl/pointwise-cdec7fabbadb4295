"""Byte delivery boundaries.

``Storage`` is the frozen legacy SDK1 boundary.  ``Storage2`` is the migrated
boundary: it owns the SDK2 credential lifecycle, keeps every stream pinned to
the task's original ETag and reopens by delivered position instead of trusting
a stream's internal position after a failed call.
"""
from vendor import sdk1, sdk2
from vendor.service import Expired, ReadInterrupted, RefreshFailed


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


class Storage2:
    """SDK2 byte source with bounded buffers and conditional reopens."""

    # Fixed payload buffer.  Its size never depends on the object length.
    BLOCK = 4096
    # A finite burst of interrupted reads is recovered by reopening at the
    # last successfully delivered byte.  The cap only prevents a permanently
    # faulty service from spinning forever.
    MAX_INTERRUPTS = 1000

    def __init__(self, world):
        self.world = world
        self.client = sdk2.Client(world)

    def _credential(self, credential):
        """Return a credential usable for the next underlying call.

        At or after ``refresh_at`` a new credential is requested.  If issuance
        fails but the old credential is still before ``expires_at``, the old
        one keeps working; once expired there is no legal credential and the
        failure is propagated.
        """
        now = self.world.now
        if credential is not None and now < credential.refresh_at:
            return credential
        try:
            return self.world.issue()
        except RefreshFailed:
            if credential is not None and now < credential.expires_at:
                return credential
            raise

    def chunks(self, container, key, etag, offset):
        """Yield bytes from ``offset`` under the task's original ETag.

        The generator's position is the *delivered* position: bytes handed to
        the caller via a successful ``readinto`` return, independent of what a
        failed call may have written into its buffer or moved internally.
        """
        delivered = offset
        credential = None
        stream = None
        interrupts = 0
        buffer = bytearray(self.BLOCK)  # allocated once, size fixed
        try:
            while True:
                next_credential = self._credential(credential)
                if stream is not None and next_credential.token != credential.token:
                    # A stream is bound to the credential it was opened with.
                    # A freshly issued credential must be used for all later
                    # reads, so close and reopen at the delivered offset.
                    stream.close()
                    stream = None
                credential = next_credential
                if stream is None:
                    stream = self.client.open(container, key, credential,
                                              if_match=etag, offset=delivered)
                try:
                    count = stream.readinto(buffer)
                except ReadInterrupted:
                    interrupts += 1
                    if interrupts > self.MAX_INTERRUPTS:
                        raise
                    # The failed call may have dirtied the buffer and moved
                    # the stream's internal position.  Neither is trusted:
                    # discard both and reopen at ``delivered``.
                    stream.close()
                    stream = None
                    continue
                except Expired:
                    # Defensive: a credential became unusable between the
                    # check and the call.  Never retry the read with it.
                    stream.close()
                    stream = None
                    credential = None
                    continue
                if count == 0:
                    # Only a successful zero-byte result marks end of object;
                    # a short successful read is not EOF.
                    return
                interrupts = 0
                chunk = bytes(buffer[:count])
                yield chunk
                delivered += count
        finally:
            if stream is not None:
                stream.close()
