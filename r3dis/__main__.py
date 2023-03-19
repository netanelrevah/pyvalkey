import typer

from r3dis.server import RedisServer

app = typer.Typer()


@app.command()
def main(port: int = 6379):
    with RedisServer(("127.0.0.1", port)) as s:
        s.serve_forever()


if __name__ == "__main__":
    app()
