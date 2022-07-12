import click


@click.group()
def cli():
    pass


@cli.group()
def allocation():
    pass


@cli.command()
def status():
    #   TODO
    click.echo("STATUS")


@cli.command()
def find_node():
    #   TODO
    #   yapapi find-node --runtime vm --timeout 60s
    click.echo("FIND NODES")


@allocation.command()
def list():
    #   TODO
    click.echo("ALLOCATION LIST")


@allocation.command()
def new():
    #   TODO
    #   yapapi allocation new 50 --network polygon
    click.echo("ALLOCATION NEW")


@allocation.command()
def clean():
    #   TODO
    click.echo("ALLOCATION CLEAN")
