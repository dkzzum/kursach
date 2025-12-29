import asyncio

from pyrogram import Client
from pyrogram.enums import ChatType
from pyrogram.errors import RPCError, FloodWait

from core.config import app_version, phone, lang_code, api_id, api_hash

# Имя сессии (создаст файл my_parser.session)
app = Client(
    "session1",
    api_id=api_id,
    api_hash=api_hash,
    app_version=app_version,
    device_model=phone,
    lang_code=lang_code,
)
TARGET_CHANNEL_ID = -1002004440104
i = 0


async def dump_channels():
    async with app:
        print("--- 🔍 Сканирую подписки аккаунта... ---")
        print(f"{'ID':<20} | {'Тип':<10} | {'Название'}")
        print("-" * 60)
        # get_dialogs() возвращает список всех чатов, где есть аккаунт
        async for dialog in app.get_dialogs():
            chat = dialog.chat

            # Нас интересуют только КАНАЛЫ (Channel) и ГРУППЫ (Supergroup)
            # Личные чаты (Private) нам не нужны
            if chat.type in [ChatType.CHANNEL, ChatType.SUPERGROUP]:
                chat_type = "Канал" if chat.type == ChatType.CHANNEL else "Группа"

                # Выводим в красивом формате
                print(f"{chat.id:<20} | {chat_type:<10} | {chat.title}")

        print("-" * 60)
        print("Готово! Скопируйте нужные ID для парсера.")


async def parse_channel_posts(target_channel=TARGET_CHANNEL_ID):
    print(f"--- 🚀 Начинаю чтение канала {target_channel} ---")
    global i

    async with app:
        chat = await app.get_chat(target_channel)
        print(f"Канал найден: {chat.title}\n")

        # Читаем историю
        async for post in app.get_chat_history(target_channel, limit=100):
            i += 1
            # Пропускаем служебные сообщения
            if post.service:
                continue

            # --- ИСПРАВЛЕНИЕ ЗДЕСЬ ---
            # 1. Получаем объект текста
            raw_obj = post.text or post.caption or "[Без текста]"

            # 2. Превращаем его в обычную строку (str), чтобы убрать привязку к сущностям Pyrogram
            post_text = str(raw_obj)

            # 3. Теперь можно безопасно резать, Python сам разберется с суррогатными парами
            preview = post_text[:50].replace('\n', ' ')
            date_str = post.date.strftime("%Y-%m-%d")

            # 2. Получаем комментарии
            try:
                replies_count = await app.get_discussion_replies_count(target_channel, post.id)
                await asyncio.sleep(0.3)

            except FloodWait as e:
                # print(f"😴 Спим {e.value} сек (FloodWait)...")
                await asyncio.sleep(e.value)
                replies_count = await app.get_discussion_replies_count(target_channel, post.id)

            except Exception:
                replies_count = 0
                continue

            # 3. Вывод
            print(f"ID: {post.id} | 📅 {date_str} | 👀 {post.views or 0} | 💬 {replies_count}")
            print(f"📝 {preview}...")
            await get_comments_for_one_post(target_channel=target_channel, post_id=post.id)
            print("-" * 40)


async def get_comments_for_one_post(target_channel, post_id):
    global i

    while True:
        try:
            # Твой основной цикл получения данных
            async for comment in app.get_discussion_replies(target_channel, post_id):
                i += 1
                author = comment.from_user.first_name if comment.from_user else "Аноним"
                text = comment.text or "Картинка/Стикер"

                # Защита от ошибок кодировки при принте
                text_str = str(text).replace('\n', ' ')

                is_reply_to_user = (comment.reply_to_message_id != comment.reply_to_top_message_id)
                prefix = "└──🗣 ОТВЕТ ЮЗЕРУ:" if is_reply_to_user else "💬 КОММЕНТ:"

                print(f"{prefix} [{author}]: {text_str[:50]}...")

            break

        except FloodWait as e:
            # 3. Если Telegram просит подождать
            # print(f"\n😴 Спим {e.value} сек (FloodWait)...")
            await asyncio.sleep(e.value)
            # print("🔄 Повторяем попытку получения комментариев...\n")
            # Мы НЕ делаем break, поэтому while True начнется заново

        except Exception as e:
            break


async def main():
    await dump_channels()
    await parse_channel_posts()


if __name__ == "__main__":
    app.run(main())
    print(i)


# -1002145502987  – чат АБЦ
# -1002004440104 – канал АБЦ
