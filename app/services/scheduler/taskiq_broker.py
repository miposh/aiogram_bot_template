import logging
from urllib.parse import quote

from taskiq import TaskiqEvents, TaskiqScheduler, TaskiqState
from taskiq.schedule_sources import LabelScheduleSource
from taskiq_nats import NatsBroker
from taskiq_redis import RedisScheduleSource

from app.config.loader import get_config
from app.config.models import RedisConfig

config = get_config()


def build_redis_url(redis: RedisConfig) -> str:
    """Redis URL including credentials and DB index from the app config."""
    auth = ""
    if redis.password:
        user = quote(redis.username or "", safe="")
        auth = f"{user}:{quote(redis.password, safe='')}@"
    return f"redis://{auth}{redis.host}:{redis.port}/{redis.database}"


broker = NatsBroker(servers=config.nats.servers, queue="taskiq_tasks")

redis_source = RedisScheduleSource(url=build_redis_url(config.redis))

scheduler = TaskiqScheduler(broker, [redis_source, LabelScheduleSource(broker)])


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def startup(state: TaskiqState) -> None:
    logging.basicConfig(level=config.logs.level_name, format=config.logs.format)
    logger = logging.getLogger(__name__)
    logger.info("Starting scheduler...")

    state.logger = logger


@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def shutdown(state: TaskiqState) -> None:
    state.logger.info("Scheduler stopped")
