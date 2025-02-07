import docker
from valkey import Valkey


def run_tests(s: Valkey, tags="", additional_args: str = ""):
    client = docker.from_env()

    image, logs = client.images.build(path=".", rm=True)

    tags = (tags + " -needs:debug").strip()
    command = (
        f'--host host.docker.internal --port {s.get_connection_kwargs()["port"]} --verbose --dump-logs --tags "{tags}" '
    )

    print(command)

    container = client.containers.run(
        image,
        command=command + additional_args,
        detach=True,
    )

    status = container.wait()

    open("docker.logs", "wb").write(container.logs())

    container.remove()

    assert status["StatusCode"] == 0


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
