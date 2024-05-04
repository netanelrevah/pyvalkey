# pyvalkey: Python implemented Valkey server

[![PyPI version](https://img.shields.io/pypi/v/r3dis.svg)](https://pypi.python.org/pypi/r3dis/)
[![PyPI downloads](https://img.shields.io/pypi/dm/r3dis.svg)](https://pypi.python.org/pypi/r3dis/)
[![GitHub](https://img.shields.io/github/license/netanelrevah/r3dis)](https://pypi.python.org/pypi/r3dis/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/r3dis)](https://pypi.python.org/pypi/r3dis/)

Tired of DevOps telling you not deploy Valkey on your system?
Stuck in python environment but still want simple data structure store to sync between you threads?

r3dis is a pure python implementation of the popular data store. it's create tcp server that support resp protocol and can be used as regular server.

to install, run:
```shell
pip install pyvalkey
```

to start server, run:
```shell
python -m pyvalkey
```

to start inside python thread:

```python
from threading import Thread

from pyvalkey.server import ValkeyServer

server = ValkeyServer(("127.0.0.1", 6379))
t = Thread(target=server.serve_forever, daemon=True)
t.start()
```

currently supported commands:
* CONFIG SET
* CONFIG GET
* ACL HELP ~
* ACL GENPASS
* ACL CAT
* ACL DELUSER
* ACL SETUSER
* ACL GETUSER
* LPUSH
* CLIENT LIST
* CLIENT ID
* CLIENT SETNAME
* CLIENT GETNAME
* CLIENT KILL
* CLIENT PAUSE
* CLIENT UNPAUSE
* CLIENT REPLY
* INFO
* AUTH
* FLUSHDB
* SELECT
* PING
* QUIT
* DBSIZE
* ECHO
* GET
* SET
* DEL
* HSET
* HGETALL
* KEYS
* APPEND

in the future:
* more commands
* support for multiple client using asyncio loop instead of thread per client

you are invited to help :)
