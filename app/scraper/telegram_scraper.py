import asyncio
import json
import logging
import os
import sys
import time
from typing import List, Dict, Any, Optional

from pyrogram import Client
from pyrogram.enums import ChatType
from pyrogram.errors import FloodWait
from pyrogram.types import Message
from itertools import groupby

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from app.core.config import app_version, phone, lang_code, api_id, api_hash

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


class TelegramSparkParser:
    """
    Класс для парсинга каналов Telegram и сохранения данных
    в формате, совместимом со Spark (JSON с партиционированием по дате).
    """

    def __init__(
            self,
            client: Client,
            data_path: str = "./data/raw",
            batch_size_posts: int = 500,
            batch_size_comments: int = 1000
    ):
        self.app = client
        self.data_path = data_path
        self.batch_size_posts = batch_size_posts
        self.batch_size_comments = batch_size_comments

    def _save_batch(self, data: List[Dict], prefix: str, date_folder: str) -> None:
        """Сохраняет пачку данных на диск и очищает буфер."""
        if not data:
            return

        # Структура: ./raw_data/posts/date=2024-01-01
        folder = os.path.join(self.data_path, prefix, f"date={date_folder}")
        os.makedirs(folder, exist_ok=True)

        filename = f"{prefix}_{int(time.time())}_{os.urandom(4).hex()}.json"
        filepath = os.path.join(folder, filename)

        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            logger.info(f"💾 [SAVED] {len(data)} объектов -> {filename}")

            # Очищаем список inplace для экономии памяти
            data.clear()

        except Exception as e:
            logger.error(f"⚠️ Ошибка сохранения файла {filepath}: {e}")

    def _extract_post_payload(self, post: Message, channel_id: int, replies_count: int) -> Dict[str, Any]:
        """Преобразует объект поста Pyrogram в словарь."""
        return {
            'channel_id': channel_id,
            'post_id': post.id,
            'date': post.date.strftime("%Y-%m-%d"),
            'ts': int(post.date.timestamp()),
            'text': str(post.text or post.caption or ""),
            'views': post.views or 0,
            'replies_count': replies_count
        }

    def _extract_comment_payload(self, comment: Message, post_id: int, channel_id: int) -> Dict[str, Any]:
        """Преобразует объект комментария Pyrogram в словарь."""
        text_content = str(comment.text or comment.caption or "")

        author_id = comment.from_user.id if comment.from_user else 0
        author_name = comment.from_user.first_name if comment.from_user else "Hidden/Deleted"

        is_reply_to_user = False
        if comment.reply_to_top_message_id:
            is_reply_to_user = (comment.reply_to_message_id != comment.reply_to_top_message_id)

        return {
            'comment_id': comment.id,
            'post_id': post_id,
            'channel_id': channel_id,
            'text': text_content,
            'author_id': author_id,
            'author_name': author_name,
            'date': comment.date.strftime("%Y-%m-%d"),
            'ts': int(comment.date.timestamp()),
            'reply_to_msg_id': comment.reply_to_message_id,
            'is_reply_to_user': is_reply_to_user
        }

    async def _safe_get_replies_count(self, channel_id: int, post_id: int) -> int:
        """Получает количество комментариев с обработкой FloodWait."""
        while True:
            try:
                return await self.app.get_discussion_replies_count(channel_id, post_id)
            except FloodWait as e:
                logger.warning(f"😴 FloodWait (Get Count): Ждем {e.value} сек...")
                await asyncio.sleep(e.value + 1)
            except Exception:
                # Если комментарии отключены или пост удален
                return 0

    async def _process_comments(self, channel_id: int, post_id: int, comments_buffer: List[Dict]):
        """Сбор комментариев для конкретного поста."""
        try:
            async for com in self.app.get_discussion_replies(channel_id, post_id):
                com_obj = self._extract_comment_payload(com, post_id, channel_id)
                comments_buffer.append(com_obj)

                if len(comments_buffer) >= self.batch_size_comments:
                    self._save_batch(comments_buffer, "comments", com_obj['date'])

        except FloodWait as e:
            logger.warning(f"😴 FloodWait (Fetching Comments): Ждем {e.value} сек...")
            await asyncio.sleep(e.value + 2)
        except Exception as e:
            logger.error(f"   ⚠️ Ошибка сбора комментариев для поста {post_id}: {e}")

    async def process_channel(self, channel_id: int, limit: int):
        """Основной цикл обработки одного канала."""
        logger.info(f"--- 🚀 Запуск обработки канала {channel_id} ---")

        posts_buffer = []
        comments_buffer = []

        try:
            chat = await self.app.get_chat(channel_id)
            logger.info(f"Название канала: {chat.title}")

            async for post in self.app.get_chat_history(channel_id, limit=limit):
                if post.service:
                    continue

                # 1. Получаем кол-во ответов
                replies_count = await self._safe_get_replies_count(channel_id, post.id)

                # 2. Обработка поста
                post_obj = self._extract_post_payload(post, channel_id, replies_count)
                posts_buffer.append(post_obj)

                if len(posts_buffer) >= self.batch_size_posts:
                    self._save_batch(posts_buffer, "posts", post_obj['date'])

                # 3. Обработка комментариев (если есть)
                if replies_count > 0:
                    logger.info(f"   ↳ Комментариев: {replies_count} (ID поста: {post.id})")
                    await self._process_comments(channel_id, post.id, comments_buffer)

                # Небольшая пауза между постами
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"❌ Критическая ошибка канала {channel_id}: {e}")



        finally:

            # Умный сброс остатков (группируем по дате, чтобы не создавать папку leftovers)

            if posts_buffer:
                # Сортируем, так как groupby требует отсортированных данных
                posts_buffer.sort(key=lambda x: x['date'])
                for date_key, group in groupby(posts_buffer, key=lambda x: x['date']):
                    # БЫЛО: save_batch(...) -> ОШИБКА
                    # СТАЛО: self._save_batch(...)
                    self._save_batch(list(group), "posts", date_key)
            if comments_buffer:
                comments_buffer.sort(key=lambda x: x['date'])
                for date_key, group in groupby(comments_buffer, key=lambda x: x['date']):
                    # БЫЛО: save_batch(...) -> ОШИБКА
                    # СТАЛО: self._save_batch(...)
                    self._save_batch(list(group), "comments", date_key)

    async def get_channel_list(self) -> List[int]:
        """Сканирует подписки и возвращает ID каналов/групп."""
        group_ids = []
        logger.info("--- 🔍 Сканирую подписки... ---")

        async for dialog in self.app.get_dialogs():
            if dialog.chat.type in [ChatType.CHANNEL, ChatType.SUPERGROUP]:
                group_ids.append(dialog.chat.id)
                logger.info(f"ID: {dialog.chat.id:<20} | Title: {dialog.chat.title}")

        logger.info("-" * 60)
        return group_ids

    async def run(self, specific_channels: Optional[List[int]] = None, limit_per_channel: int = 100):
        """Точка входа."""
        async with self.app:
            # Если передали конкретные ID, используем их, иначе сканируем все подписки
            target_ids = specific_channels or await self.get_channel_list()

            for gid in target_ids:
                await self.process_channel(gid, limit=limit_per_channel)


# --- ЗАПУСК ---
if __name__ == "__main__":
    # 1. Создаем клиент Pyrogram
    pyro_client = Client(
        "session1",
        api_id=api_id,
        api_hash=api_hash,
        app_version=app_version,
        device_model=phone,
        lang_code=lang_code,
    )

    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Формируем правильный путь: /Users/.../kursch/data/raw
    correct_data_path = os.path.join(base_dir, "data", "raw")
    print(f"📂 Данные будут сохранены в: {correct_data_path}")

    # 2. Создаем наш парсер
    parser = TelegramSparkParser(
        client=pyro_client,
        data_path=correct_data_path,
        batch_size_posts=500,
        batch_size_comments=1000
    )

    # 3. Запускаем
    # Можно передать список ID, чтобы не сканировать всё: await parser.run([-100123456...])
    pyro_client.run(parser.run(limit_per_channel=1000))