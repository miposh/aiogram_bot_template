from nats.aio.client import Client as NATS
from nats.js import JetStreamContext


async def connect_to_nats(servers: str | list[str]) -> tuple[NATS, JetStreamContext]:
    nc = NATS()
    # nats-py gives up after 60 attempts (~2 min) and closes the client for
    # good, leaving the bot polling Telegram with a dead FSM storage.
    await nc.connect(servers, max_reconnect_attempts=-1)
    js: JetStreamContext = nc.jetstream()

    return nc, js
