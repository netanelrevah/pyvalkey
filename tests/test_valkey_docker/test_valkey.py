import docker
from valkey import Valkey


def run_tests(s: Valkey, tags="", additional_args: str = ""):
    container = None
    try:
        client = docker.from_env()

        image, logs = client.images.build(path=".", rm=True)

        log_file_name = f"{tags}.docker.log"

        tags = (tags + " -needs:debug -external:skip -cluster -needs:repl -needs:config-maxmemory -slow").strip()
        command = f'--host host.docker.internal --port {s.get_connection_kwargs()["port"]} --verbose --dump-logs --tags "{tags}" '

        print(command)

        container = client.containers.run(
            image,
            command=command + additional_args,
            detach=True,
        )

        status = container.wait()

        open(log_file_name, "wb").write(container.logs())

        assert status["StatusCode"] == 0
    finally:
        if container is not None:
            container.remove()


def test_all(s: Valkey):
    run_tests(s)


def test_all_known_tags(s: Valkey):
    run_tests(s, tags="sort keyspace hash incr list")


def test_sort(s: Valkey):
    run_tests(s, tags="sort")


def test_keyspace(s: Valkey):
    run_tests(s, tags="keyspace")


def test_type_hash(s: Valkey):
    run_tests(s, tags="hash")


def test_type_incr(s: Valkey):
    run_tests(s, tags="incr")


def test_type_list(s: Valkey):
    run_tests(s, tags="list")


def test_dump(s: Valkey):
    run_tests(s, tags="dump")


def test_acl(s: Valkey):
    run_tests(s, tags="acl")


def test_tracking(s: Valkey):
    run_tests(s, tags="tracking")


def test_multi(s: Valkey):
    run_tests(s, tags="multi")
