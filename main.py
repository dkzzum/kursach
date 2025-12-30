import asyncio
import json
import os
import time
from typing import List, Dict

from pyrogram import Client
from pyrogram.enums import ChatType
from pyrogram.errors import FloodWait

from core.config import app_version, phone, lang_code, api_id, api_hash


# --- НАСТРОЙКИ ---
BATCH_SIZE_POSTS = 500       # Сбрасываем посты каждые 500 шт
BATCH_SIZE_COMMENTS = 1000   # Сбрасываем комменты каждые 1000 шт
DATA_PATH = "./raw_data"     # Куда сохранять

# Имя сессии
app = Client(
    "session1",
    api_id=api_id,
    api_hash=api_hash,
    app_version=app_version,
    device_model=phone,
    lang_code=lang_code,
)
TARGET_CHANNEL_ID = -1002004440104


def save_batch(data: List[Dict], prefix: str, date_folder: str):
    """Сохраняет список словарей в JSON файл и очищает список"""
    if not data:
        return

    # Создаем папку под дату: ./raw_data/posts/date=2024-01-01
    folder = f"{DATA_PATH}/{prefix}/date={date_folder}"
    os.makedirs(folder, exist_ok=True)

    # Уникальное имя файла: posts_17000000_uuid.json
    filename = f"{prefix}_{int(time.time())}_{os.urandom(4).hex()}.json"
    filepath = os.path.join(folder, filename)

    data = json.loads(json.dumps(data, indent=4))
    with open(filepath, "w", encoding="utf-8") as f:
        # ensure_ascii=False чтобы русские буквы не превращались в \u0430
        json.dump(data, f, ensure_ascii=False, indent=4)

    print(f"💾 [SAVED] {len(data)} объектов в {filename}")
    data = ''  # ОЧИЩАЕМ СПИСОК (Память освобождается)


def extract_post_data(post, channel_id, replies_count):
    """
    Эта функция 'чистит' данные: берет сырой объект поста и
    возвращает красивый словарь только с тем, что нам нужно.
    """
    return {
        'channel_id': channel_id,         # ID канала
        'post_id': post.id,               # ID поста
        'date': post.date.strftime("%Y-%m-%d"), # Дата строкой
        'ts': int(post.date.timestamp()), # Дата числом (для сортировки)
        'text': str(post.text or post.caption or ""), # Текст
        'views': post.views or 0,         # Просмотры
        'replies_count': replies_count    # Количество комментариев
    }


def extract_comment_data(comment, post_id, channel_id):
    """
    Превращает сырой объект комментария Pyrogram в чистый словарь.
    """
    # 1. Достаем текст (он может быть в text или caption, если прислали картинку)
    text_content = str(comment.text or comment.caption or "")

    # 2. Определяем автора (если пользователь удален или скрыт — ставим 0)
    author_id = comment.from_user.id if comment.from_user else 0
    author_name = comment.from_user.first_name if comment.from_user else "Hidden/Deleted"

    # 3. Логика дискуссий: Это ответ на пост или спор с другим человеком?
    # Если reply_to_message_id не равен ID "верхушки" ветки, значит это ответ юзеру.
    is_reply_to_user = False
    if comment.reply_to_top_message_id:
        is_reply_to_user = (comment.reply_to_message_id != comment.reply_to_top_message_id)

    return {
        'comment_id': comment.id,
        'post_id': post_id,  # ID Родительского поста (СВЯЗЬ!)
        'channel_id': channel_id,  # ID Канала
        'text': text_content,  # Текст комментария
        'author_id': author_id,  # ID Автора (для поиска ботов/агитаторов)
        'author_name': author_name,  # Имя (можно убрать, если не нужно для отладки)
        'date': comment.date.strftime("%Y-%m-%d"),
        'ts': int(comment.date.timestamp()),

        # --- Поля для графового анализа (кто с кем спорит) ---
        'reply_to_msg_id': comment.reply_to_message_id,
        'is_reply_to_user': is_reply_to_user  # True = Спор, False = Мнение о новости
    }


async def dump_channels() -> List[int]:
    group_id = []

    print("--- 🔍 Сканирую подписки аккаунта... ---")
    print(f"{'ID':<20} | {'Тип':<10} | {'Название'}")
    print("-" * 60)
    # get_dialogs() возвращает список всех чатов, где есть аккаунт
    async for dialog in app.get_dialogs():
        chat = dialog.chat

        # Нас интересуют только КАНАЛЫ (Channel) и ГРУППЫ (Supergroup)
        # Личные чаты (Private) нам не нужны
        if chat.type in [ChatType.CHANNEL, ChatType.SUPERGROUP]:
            group_id.append(chat.id)

            chat_type = "Канал" if chat.type == ChatType.CHANNEL else "Группа"
            print(f"{chat.id:<20} | {chat_type:<10} | {chat.title}")

    print("-" * 60)
    return group_id


async def parse_channel(target_channel_id: int, limit: int):
    print(f"\n--- 🚀 Канал {target_channel_id} ---")

    posts_buffer = []
    comments_buffer = []

    try:
        chat = await app.get_chat(target_channel_id)
        print(f"Название: {chat.title}")

        async for post in app.get_chat_history(target_channel_id, limit=limit):
            if post.service: continue

            # --- A. Получаем количество комментариев (БРОНЕБОЙНЫЙ МЕТОД) ---
            replies_count = 0

            # Цикл будет крутиться, пока мы не получим ответ или не поймем, что комментов нет
            while True:
                try:
                    replies_count = await app.get_discussion_replies_count(target_channel_id, post.id)
                    break  # Успех! Выходим из цикла while

                except FloodWait as e:
                    print(f"😴 FloodWait (счетчик): Ждем {e.value} сек...")
                    await asyncio.sleep(e.value + 1)  # Спим +1 сек для надежности

                except Exception:
                    # Если комментов нет, или ошибка доступа — просто ставим 0 и выходим
                    replies_count = 0
                    break

            # --- B. Сохраняем пост ---
            post_obj = extract_post_data(post, target_channel_id, replies_count)
            posts_buffer.append(post_obj)

            if len(posts_buffer) >= BATCH_SIZE_POSTS:
                save_batch(posts_buffer, "posts", post_obj['date'])

            # --- C. Собираем комментарии (ТОЖЕ БЕЗОПАСНО) ---
            if replies_count > 0:
                print(f"   ↳ Комментариев: {replies_count}")

                # Тут сложнее: FloodWait может вылететь ПРЯМО ВО ВРЕМЯ цикла for
                # Мы обернем весь цикл сбора комментов в try/except,
                # но если он упадет на середине, мы просто перейдем к следующему посту,
                # чтобы не усложнять логику до бесконечности.
                try:
                    async for com in app.get_discussion_replies(target_channel_id, post.id):
                        com_obj = extract_comment_data(com, post.id, target_channel_id)
                        comments_buffer.append(com_obj)

                        if len(comments_buffer) >= BATCH_SIZE_COMMENTS:
                            save_batch(comments_buffer, "comments", com_obj['date'])

                except FloodWait as e:
                    # Если FloodWait случился во время загрузки комментов,
                    # к сожалению, придется пропустить остаток этого поста и идти дальше,
                    # иначе мы застрянем навечно.
                    print(f"😴 FloodWait (список комментов): Ждем {e.value} сек...")
                    await asyncio.sleep(e.value + 2)

                except Exception as e:
                    print(f"   ⚠️ Ошибка сбора комментариев: {e}")

            # 🔥 ВАЖНО: Добавляем маленькую паузу между ПОСТАМИ,
            # чтобы не злить Telegram и снизить шанс FloodWait
            await asyncio.sleep(1)

    except Exception as e:
        print(f"❌ Критическая ошибка канала {target_channel_id}: {e}")

    finally:
        if posts_buffer: save_batch(posts_buffer, "posts", "leftovers")
        if comments_buffer: save_batch(comments_buffer, "comments", "leftovers")


async def main():
    async with app:
        group_id = await dump_channels()

        for gid in group_id:
            await parse_channel(gid, limit=10)


if __name__ == "__main__":
    app.run(main())
