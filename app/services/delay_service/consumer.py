import logging

from aiogram import Bot
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNotFound,
    TelegramRetryAfter,
)

from app.bot.enums.actions import Action
from app.services.delay_service.models.delayed_messages import DelayedMessageDeletion
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.js import JetStreamContext

logger = logging.getLogger(__name__)


class DelayedMessageConsumer:
    def __init__(
        self,
        nc: Client,
        js: JetStreamContext,
        bot: Bot,
        subject: str,
        stream: str,
        durable_name: str,
    ) -> None:
        self.nc = nc
        self.js = js
        self.bot = bot
        self.subject = subject
        self.stream = stream
        self.durable_name = durable_name

    async def start(self) -> None:
        self.stream_sub = await self.js.subscribe(
            subject=self.subject,
            stream=self.stream,
            cb=self.on_message,
            durable=self.durable_name,
            manual_ack=True,
        )

    async def on_message(self, msg: Msg) -> None:
        headers = msg.headers or {}
        if headers.get("Tg-Delayed-Type") != Action.DELETE:
            # Action.POST is not implemented and unknown types can never be
            # processed: terminate them instead of redelivering forever.
            logger.error("Dropping unsupported delayed message: %r", headers)
            await msg.term()
            return

        try:
            msg_to_delete = DelayedMessageDeletion.from_dict(headers)
        except (TypeError, ValueError):
            logger.exception("Dropping malformed delayed message: %r", headers)
            await msg.term()
            return

        if not msg_to_delete.is_ready_time():
            # Отправляем nak с временем задержки
            await msg.nak(delay=msg_to_delete.calc_delay())
            return

        try:
            await self.bot.delete_message(
                chat_id=msg_to_delete.chat_id,
                message_id=msg_to_delete.message_id,
            )
        except TelegramRetryAfter as e:
            await msg.nak(delay=e.retry_after)
            return
        except (TelegramBadRequest, TelegramForbiddenError, TelegramNotFound) as e:
            # The message is already gone or the bot lost access to the chat:
            # a retry can never succeed, so acknowledge it.
            logger.warning(
                "Delayed deletion skipped, chat_id=%s message_id=%s: %s",
                msg_to_delete.chat_id,
                msg_to_delete.message_id,
                e,
            )
        await msg.ack()

    async def unsubscribe(self) -> None:
        if self.stream_sub:
            await self.stream_sub.unsubscribe()
            logger.info("Consumer unsubscribed")
