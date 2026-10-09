from html import escape

from aiogram.types import User
from aiogram_dialog import DialogManager
from fluentogram import TranslatorRunner


async def get_hello(
    dialog_manager: DialogManager,
    i18n: TranslatorRunner,
    event_from_user: User,
    **kwargs,
) -> dict[str, str]:
    username = event_from_user.full_name or event_from_user.username or i18n.stranger()
    # Messages are sent with parse_mode=HTML: user-controlled text must be escaped.
    return {"hello": i18n.start.hello(username=escape(username, quote=False))}
