import typer

from pyvalkey.server import ValkeyServer

app = typer.Typer()


@app.command()
def main(port: int = 6379) -> None:
    ValkeyServer("127.0.0.1", port).run()


if __name__ == "__main__":
    app()
