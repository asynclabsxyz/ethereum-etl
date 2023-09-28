import click
import logging
import random

from blockchainetl.streaming.streaming_utils import configure_signals, configure_logging
from ethereumetl.providers.auto import get_provider_from_uri

from suietl.enumeration.entity_type import EntityType
from suietl.streaming.item_exporter_creator import create_item_exporters


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-l",
    "--last-synced-block-file",
    default="last_synced_block.txt",
    show_default=True,
    type=str,
    help="",
)
@click.option(
    "--lag",
    default=0,
    show_default=True,
    type=int,
    help="The number of blocks to lag behind the network.",
)
@click.option(
    "-p",
    "--provider-uri",
    default="https://mainnet.infura.io",
    show_default=True,
    type=str,
    help="The URI of the web3 provider e.g. "
    "file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io",
)
@click.option(
    "-o",
    "--output",
    type=str,
    help="Either Google PubSub topic path e.g. projects/your-project/topics/crypto_ethereum; "
    "or Postgres connection url e.g. postgresql+pg8000://postgres:admin@127.0.0.1:5432/ethereum; "
    "or GCS bucket e.g. gs://your-bucket-name; "
    "or kafka, output name and connection host:port e.g. kafka/127.0.0.1:9092 "
    "or Kinesis, e.g. kinesis://your-data-stream-name"
    "If not specified will print to console",
)
@click.option(
    "-s", "--start-block", default=None, show_default=True, type=int, help="Start block"
)
@click.option(
    "-e",
    "--entity-types",
    default=",".join(EntityType.ALL_FOR_STREAMING),
    show_default=True,
    type=str,
    help="The list of entity types to export.",
)
@click.option(
    "--period-seconds",
    default=10,
    show_default=True,
    type=int,
    help="How many seconds to sleep between syncs",
)
@click.option(
    "-b",
    "--batch-size",
    default=1,
    show_default=True,
    type=int,
    help="How many blocks to batch in single request",
)
@click.option(
    "-B",
    "--block-batch-size",
    default=1,
    show_default=True,
    type=int,
    help="How many blocks to batch in single sync round",
)
@click.option(
    "-w",
    "--max-workers",
    default=1,
    show_default=True,
    type=int,
    help="The number of workers",
)
@click.option("--log-file", default=None, show_default=True, type=str, help="Log file")
@click.option("--pid-file", default=None, show_default=True, type=str, help="pid file")
def stream(
    last_synced_block_file,
    lag,
    provider_uri,
    output,
    start_block,
    entity_types,
    period_seconds=10,
    batch_size=2,
    block_batch_size=1,
    max_workers=1,
    log_file=None,
    pid_file=None,
):
    """Streams all data types to console or Google Pub/Sub."""
    configure_logging(log_file)
    configure_signals()
    entity_types = parse_entity_types(entity_types)

    from suietl.streaming.sui_streamer_adapter import SuiStreamerAdapter
    from blockchainetl.streaming.streamer import Streamer

    # TODO: Implement fallback mechanism for provider uris instead of picking randomly
    provider_uri = pick_random_provider_uri(provider_uri)
    logging.info("Using " + provider_uri)

    streamer_adapter = SuiStreamerAdapter(
        # naman: We aren't using web3 client and I'm not sure how to make it thread safe without. I'll set it to one worker.
        # I'll keep it simple for now and directly pass the http provider as it is the same json rpc request/response format.
        batch_http_provider=get_provider_from_uri(provider_uri, batch=True),
        item_exporter=create_item_exporters(output),
        # naman: Setting batch size to 1 because sui calls to ANKR fails for batch calls with error
        # {'code': -32005, 'message': 'Batched requests are not supported by this server'}, 'id': None}
        # batch_size=batch_size,
        # max_workers=max_workers,
        batch_size=1,
        max_workers=1,
        entity_types=entity_types,
    )
    streamer = Streamer(
        blockchain_streamer_adapter=streamer_adapter,
        last_synced_block_file=last_synced_block_file,
        lag=lag,
        start_block=start_block,
        period_seconds=period_seconds,
        # block_batch_size=block_batch_size,
        block_batch_size=1,
        pid_file=pid_file,
    )
    streamer.stream()


def parse_entity_types(entity_types):
    entity_types = [c.strip() for c in entity_types.split(",")]

    # validate passed types
    for entity_type in entity_types:
        if entity_type not in EntityType.ALL_FOR_STREAMING:
            raise click.BadOptionUsage(
                "--entity-type",
                "{} is not an available entity type. Supply a comma separated list of types from {}".format(
                    entity_type, ",".join(EntityType.ALL_FOR_STREAMING)
                ),
            )

    return entity_types


def pick_random_provider_uri(provider_uri):
    provider_uris = [uri.strip() for uri in provider_uri.split(",")]
    return random.choice(provider_uris)
