import time

from pyvalkey.commands.generic_commands import Keys, ObjectEncoding, TimeToLive
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import Database, DatabaseContent, KeyValue


class TestTimeToLive:
    def test_execute(self):
        database = Database(
            0, None, None, DatabaseContent({b"mykey{t}": KeyValue(b"mykey{t}", b"foo", int(time.time() + 100) * 1000)})
        )

        command = TimeToLive(database, b"mykey{t}")

        assert 95 < command.execute() < 100


class TestKeys:
    def test_validate_pattern(self):
        assert Keys.nesting(b"*?" * 1000) == 1000
        assert Keys.nesting(b"*?" * 1001) == 1001
        assert Keys.nesting(b"?*" * 1001) == 1000
        assert Keys.nesting(b"?*" * 1002) == 1001


class TestObjectEncoding:
    def test_execute(self):
        database = Database(0, None, None, DatabaseContent({b"hash1": KeyValue(b"hash1", {b"k1": b"v1"})}))
        configurations = Configurations()

        assert ObjectEncoding(database, configurations, b"hash1").execute() == b"listpack"

        database = Database(
            0,
            None,
            None,
            DatabaseContent(
                {
                    b"hash1": KeyValue(
                        b"hash1",
                        {
                            b"k1": b"v1",
                            b"k2": b"v2",
                            b"k3": b"v3",
                            b"k4": b"v4",
                            b"k5": b"v5",
                            b"k6": b"v6",
                            b"k7": b"v7",
                            b"k8": b"v8",
                        },
                    )
                }
            ),
        )
        configurations = Configurations()

        assert ObjectEncoding(database, configurations, b"hash1").execute() == b"listpack"
