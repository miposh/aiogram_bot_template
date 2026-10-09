import logging

import asyncio

from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.js.api import RetentionPolicy, StorageType, StreamConfig

from app.config.loader import get_config

logger = logging.getLogger(__name__)


async def main() -> None:
    config = get_config()

    logging.basicConfig(
        level=config.logs.level_name.upper(),
        format=config.logs.format,
    )

    nc = NATS()
    await nc.connect(servers=config.nats.servers)

    js: JetStreamContext = nc.jetstream()

    stream_name = config.nats.delayed_consumer_stream

    # Stream configuration. WORK_QUEUE deletes a message once it is acked or
    # terminated; with LIMITS and no limits every processed message stays on
    # disk forever. NATS cannot switch an existing stream to/from WORK_QUEUE:
    # drain and delete the old stream before re-running this script.
    stream_config = StreamConfig(
        name=stream_name,
        subjects=[config.nats.delayed_consumer_subject],
        retention=RetentionPolicy.WORK_QUEUE,
        storage=StorageType.FILE,
        max_age=30 * 24 * 60 * 60,  # safety net for messages nobody consumes
    )

    # Stream creation
    await js.add_stream(stream_config)

    logger.info("Stream `%s` created", stream_name)

    await nc.close()


asyncio.run(main())
