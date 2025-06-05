from pyvalkey.commands.generic_commands import ObjectEncoding
from pyvalkey.database_objects.configurations import Configurations
from pyvalkey.database_objects.databases import Database, DatabaseContent, KeyValue


class TestObjectEncoding:
    def test_execute(self):
        database = Database(0, DatabaseContent({b"hash1": KeyValue(b"hash1", {b"k1": b"v1"})}))
        configurations = Configurations()

        assert ObjectEncoding(database, configurations, b"hash1").execute() == b"listpack"

        database = Database(
            0,
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
