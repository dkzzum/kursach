import asyncio
import json
import logging
import os
import sys
import time
import datetime
from typing import List, Dict, Any, Optional

from pyrogram import Client
from pyrogram.enums import ChatType
from pyrogram.errors import FloodWait

# Добавляем путь к конфигу
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
try:
    from app.core.config import app_version, phone, lang_code, api_id, api_hash
except ImportError:
    print("⚠️ Config not found. Using placeholders.")
    api_id = 123456
    api_hash = "your_hash_here"
    app_version = "1.0"
    phone = "Unknown"
    lang_code = "en"

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


class TelegramSparkParser:
    def __init__(
            self,
            session_name: str,
            api_id: int,
            api_hash: str,
            data_path: str = "./data/raw",
            batch_size_posts: int = 500
    ):
        self.session_name = session_name
        self.api_id = api_id
        self.api_hash = api_hash
        self.data_path = data_path
        self.batch_size_posts = batch_size_posts

        # Генерируем ID запуска (один на весь скрипт)
        self.run_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")

        # --- ИСПРАВЛЕНИЕ 1: Глобальный буфер для всех каналов ---
        self.global_buffer = []

        os.makedirs(self.data_path, exist_ok=True)

    def _save_batch(self, data: List[Dict], prefix: str) -> None:
        """Сохраняет накопленные данные в файл."""
        if not data:
            return

        unique_id = int(time.time() * 1000)
        filename = f"{prefix}_{self.run_timestamp}_{unique_id}.json"
        full_path = os.path.join(self.data_path, filename)

        try:
            with open(full_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, default=str)
            logger.info(f"💾 Saved BATCH: {filename} ({len(data)} records)")
        except Exception as e:
            logger.error(f"❌ Error saving batch {filename}: {e}")

    async def get_channel_list(self, app: Client) -> List[int]:
        """Получает список каналов."""
        logger.info("Fetching channel list...")
        group_ids = []
        async for dialog in app.get_dialogs():
            if dialog.chat.type in (ChatType.CHANNEL, ChatType.SUPERGROUP, ChatType.GROUP):
                group_ids.append(dialog.chat.id)
        logger.info(f"Found {len(group_ids)} channels.")
        return group_ids

    async def process_channel(self, app: Client, chat_id: int, limit: int):
        """Парсит канал и складывает посты в ОБЩИЙ буфер."""
        logger.info(f"Processing channel {chat_id}...")

        try:
            async for message in app.get_chat_history(chat_id, limit=limit):
                if not message.text and not message.caption:
                    continue

                post_data = {
                    "post_id": message.id,
                    "channel_id": chat_id,
                    "date": message.date,
                    "text": message.text or message.caption or "",
                    "views": message.views if message.views else 0,
                    "type": "post"
                }

                # Добавляем в общий котел
                self.global_buffer.append(post_data)

                # --- ИСПРАВЛЕНИЕ 2: Проверяем размер ОБЩЕГО буфера ---
                if len(self.global_buffer) >= self.batch_size_posts:
                    self._save_batch(self.global_buffer, "posts")
                    self.global_buffer = []  # Очищаем после сохранения

        except FloodWait as e:
            logger.warning(f"⏳ FloodWait: sleeping {e.value} seconds...")
            await asyncio.sleep(e.value)
        except Exception as e:
            logger.error(f"Error accessing channel {chat_id}: {e}")

    async def run(self, limit_per_channel: int = 100):
        """Основной цикл."""

        # --- ИСПРАВЛЕНИЕ 3: Создаем Client ВНУТРИ цикла asyncio ---
        # Это решает ошибку 'attached to a different loop'
        app = Client(
            self.session_name,
            api_id=self.api_id,
            api_hash=self.api_hash,
            app_version=app_version,
            device_model=phone,
            lang_code=lang_code,
        )

        async with app:
            target_ids = await self.get_channel_list(app)
            logger.info(f"Starting job. Run ID: {self.run_timestamp}")

            for gid in target_ids:
                await self.process_channel(app, gid, limit=limit_per_channel)

            # --- ИСПРАВЛЕНИЕ 4: Сохраняем остатки в конце ---
            # Если в буфере осталось 18 постов, их тоже надо сохранить перед выходом
            if self.global_buffer:
                logger.info(f"🧹 Flushing remaining {len(self.global_buffer)} posts...")
                self._save_batch(self.global_buffer, "posts")


# --- ЗАПУСК ---
if __name__ == "__main__":
    # Определяем пути
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_output_path = os.path.join(current_dir, "data", "raw")
    if not os.path.exists(data_output_path):
        data_output_path = "data/raw"

    print(f"📂 Storage path: {os.path.abspath(data_output_path)}")

    # Инициализируем парсер (без создания клиента, только конфиг)
    parser = TelegramSparkParser(
        session_name="session1",
        api_id=api_id,
        api_hash=api_hash,
        data_path=data_output_path,
        batch_size_posts=500  # Теперь он честно будет ждать 500 постов
    )

    try:
        # Запускаем асинхронный цикл
        asyncio.run(parser.run(limit_per_channel=50))
        print("✅ Парсинг завершен успешно.")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
