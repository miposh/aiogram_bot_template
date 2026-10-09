import logging
from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware
from aiogram.types import Update, User

from app.bot.enums.roles import UserRole
from app.infrastructure.database.db import DB
from app.infrastructure.database.models.user import UserModel

logger = logging.getLogger(__name__)


def pick_locale(language_code: str | None, locales: list[str], default: str) -> str:
    """Map a Telegram IETF language tag (e.g. 'pt-br') to a supported bot locale."""
    if language_code:
        base = language_code.split("-")[0].lower()
        if base in locales:
            return base
    return default


class GetUserMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Update, dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: dict[str, Any],
    ) -> Any:
        user: User = data.get("event_from_user")

        if user is None:
            return await handler(event, data)

        db: DB = data.get("db")

        if db is None:
            logger.error("Database object is not provided in middleware data.")
            raise RuntimeError("Missing `db` in middleware context.")

        user_row: UserModel | None = await db.users.get_user(user_id=user.id)

        if user_row is None:
            # Register on first contact (not only on /start), so handlers and
            # dialogs can rely on `user_row`. ON CONFLICT makes this race-safe.
            await db.users.add(
                user_id=user.id,
                language=pick_locale(
                    user.language_code, data["bot_locales"], data["default_locale"]
                ),
                role=UserRole.USER,
            )
            user_row = await db.users.get_user(user_id=user.id)

        data.update(user_row=user_row)

        return await handler(event, data)
