### ! This is not the valkey python [client](https://github.com/valkey-io/valkey-py) !

# pyvalkey: Python implemented Valkey server

[![PyPI version](https://img.shields.io/pypi/v/pyvalkey.svg)](https://pypi.python.org/pypi/pyvalkey/)
[![PyPI downloads](https://img.shields.io/pypi/dm/pyvalkey.svg)](https://pypi.python.org/pypi/pyvalkey/)
[![GitHub](https://img.shields.io/github/license/netanelrevah/pyvalkey)](https://pypi.python.org/pypi/pyvalkey/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pyvalkey)](https://pypi.python.org/pypi/pyvalkey/)

pyvalkey is a pure python implementation of the popular data store. it's create tcp server that support RESP protocol and can be used as regular server.

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
