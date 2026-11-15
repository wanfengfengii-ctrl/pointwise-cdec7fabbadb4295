"""Offline versioned object service and observable credential issuer."""
from dataclasses import dataclass
from urllib.parse import urlsplit, parse_qs, unquote

class Expired(Exception):
    pass

class VersionGone(Exception):
    pass

class ReadInterrupted(Exception):
    pass

class RefreshFailed(Exception):
    pass

@dataclass(frozen=True)
class Credential:
    token: str
    refresh_at: int
    expires_at: int

def locator(url):
    p = urlsplit(url)
    q = parse_qs(p.query, strict_parsing=True)
    if p.scheme != 'mock' or not p.netloc or not p.path.startswith('/') or set(q) != {'sig'} or len(q['sig']) != 1:
        raise ValueError('invalid signed URL')
    key = unquote(p.path[1:], errors='strict')
    if not key:
        raise ValueError('empty key')
    return p.netloc, key, q['sig'][0]

class World:
    def __init__(self, objects, current, tokens=None, events=None,
                 renew_fail_at=(), max_read=7):
        self.objects = dict(objects)
        self.current = dict(current)
        self.tokens = dict(tokens or {})
        self.events = {int(k): list(v) for k, v in (events or {}).items()}
        self.renew_fail_at = set(renew_fail_at)
        self.max_read = max_read
        self.now = 0
        self.calls = 0
        self.serial = 0
        self.trace = []

    def issue(self):
        self.trace.append({'op': 'issue', 'at': self.now})
        if self.now in self.renew_fail_at:
            raise RefreshFailed('issuer unavailable')
        self.serial += 1
        token = 'new-' + str(self.serial)
        self.tokens[token] = self.now + 4
        return Credential(token, self.now + 2, self.now + 4)

    def authorize(self, token):
        if self.now >= self.tokens.get(token, -1):
            raise Expired('credential expired')

    def object(self, container, key, etag):
        try:
            return self.objects[(container, key, etag)]
        except KeyError:
            raise VersionGone((container, key, etag)) from None

    def begin_read(self, sdk, container, key, etag, token, offset, count):
        self.calls += 1
        self.trace.append(dict(op='read', sdk=sdk, call=self.calls,
                               at=self.now, token=token, etag=etag,
                               container=container, key=key,
                               offset=offset, count=count))
        fault = None
        for event in self.events.get(self.calls, []):
            if event['op'] == 'replace':
                e = event['etag']
                self.objects[(container, key, e)] = bytes.fromhex(event['hex'])
                self.current[(container, key)] = e
            elif event['op'] == 'drop':
                self.objects.pop((container, key, event['etag']), None)
            elif event['op'] == 'fault':
                fault = event['after']
            else:
                raise ValueError('unknown service event')
        self.authorize(token)
        return self.object(container, key, etag), fault

    def end_read(self):
        self.now += 1
