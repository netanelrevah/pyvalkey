# r3dis: Python implemented Redis server

[![PyPI version](https://img.shields.io/pypi/v/r3dis.svg)](https://pypi.python.org/pypi/r3dis/)
[![PyPI downloads](https://img.shields.io/pypi/dm/r3dis.svg)](https://pypi.python.org/pypi/r3dis/)
[![GitHub](https://img.shields.io/github/license/netanelrevah/r3dis)](https://pypi.python.org/pypi/r3dis/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/r3dis)](https://pypi.python.org/pypi/r3dis/)

Tired of DevOps telling you not deploy Redis on your system?
Stuck in python environment but still want simple data structure store to sync between you threads?

r3dis is a pure python implementation of the popular data store. it's create tcp server that support resp protocol and can be used as regular redis server.

to install, run:
```shell
pip install r3dis
```

to start redis server, run:
```shell
python -m r3dis
```

to start inside python thread:
```python
from threading import Thread

from r3dis.server import RedisServer

server = RedisServer(("127.0.0.1", 6379))
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

i want to add more in the future, you are invited to help :)