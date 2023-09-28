from blockchainetl.logging_utils import logging_basic_config

logging_basic_config()

import click

from suietl.cli.stream import stream


@click.group()
@click.version_option(version="2.3.1")
@click.pass_context
def cli(ctx):
    pass


# streaming
cli.add_command(stream, "stream")
