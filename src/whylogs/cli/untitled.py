import logging

import click

from whylogs import __version__ as whylogs_version
from whylogs.cli.init import init
from whylogs.cli.view import view
try:
    import colorama
    colorama.init()
except ImportError:
    pass


@click.command()
@click.option(
    "--project-dir",
    "-d",
    default="./",
    help="The root of the new whylogs profiling project.",
)
def view(output_dir):
    pass