import asyncio
import json
import logging
import os
import sys
import time
import uuid
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

    def _save_batch(self, data: List[Dict], entity_name: str):
        """
        Сохраняет батч данных в Data Lake с партиционированием по дате.
        Путь: data/raw/{entity_name}/date={YYYY-MM-DD}/{filename}.json
        """
        if not data:
            return

        # 1. Группируем данные по дате (чтобы не валить 31 дек и 1 янв в одну папку)
        grouped_data = {}
        for item in data:
            # Извлекаем дату. Предполагаем, что в telegram data поле date - это datetime или ISO строка
            date_val = item.get("date")

            if isinstance(date_val, str):
                # Если строка "2024-03-01 12:00:00", берем первые 10 символов
                date_str = date_val[:10]
            elif isinstance(date_val, (int, float)):
                # Если timestamp
                date_str = datetime.datetime.fromtimestamp(date_val).strftime("%Y-%m-%d")
            elif isinstance(date_val, datetime.datetime):
                # Если объект datetime
                date_str = date_val.strftime("%Y-%m-%d")
            else:
                # Fallback на сегодня
                date_str = datetime.datetime.now().strftime("%Y-%m-%d")

            if date_str not in grouped_data:
                grouped_data[date_str] = []
            grouped_data[date_str].append(item)

        # 2. Сохраняем каждую группу в свою папку
        for date_key, items_list in grouped_data.items():
            # Формируем путь: data/raw/posts/date=2024-03-01/
            partition_dir = os.path.join(self.data_path, entity_name, f"date={date_key}")
            os.makedirs(partition_dir, exist_ok=True)

            # Генерируем уникальное имя файла
            # posts_1709283000_a1b2c3d4.json
            timestamp = int(time.time())
            unique_id = str(uuid.uuid4())[:8]
            filename = f"{entity_name}_{timestamp}_{unique_id}.json"
            full_path = os.path.join(partition_dir, filename)

            try:
                with open(full_path, "w", encoding="utf-8") as f:
                    # Делаем default=str для сериализации datetime объектов, если они остались
                    json.dump(items_list, f, ensure_ascii=False, indent=4, default=str)
                logger.info(f"💾 Saved {len(items_list)} {entity_name} -> {full_path}")
            except Exception as e:
                logger.error(f"❌ Error saving batch: {e}")

    async def get_channel_list(self, app: Client) -> List[int]:
        """Получает список каналов."""
        logger.info("Fetching channel list...")
        group_ids = []
        async for dialog in app.get_dialogs():
            if dialog.chat.type in (ChatType.CHANNEL, ChatType.SUPERGROUP, ChatType.GROUP):
                group_ids.append(dialog.chat.id)
        logger.info(f"Found {len(group_ids)} channels.")
        return group_ids

    async def process_channel(self, client: Client, channel_id: int, limit: int = 1000):
        # ... (твоя логика получения истории) ...

        posts_buffer = []
        comments_buffer = []
        batch_size = 50  # Сохраняем каждые 50 сообщений

        async for message in client.get_chat_history(channel_id, limit=limit):
            # 1. Преобразуем сообщение в словарь
            msg_dict = {
                "id": message.id,
                "chat_id": message.chat.id,
                "date": message.date,  # datetime объект
                "text": message.text or message.caption or "",
                "views": message.views,
                # ... любые другие поля ...
            }
            posts_buffer.append(msg_dict)

            # 2. Если буфер заполнился -> Сохраняем
            if len(posts_buffer) >= batch_size:
                self._save_batch(posts_buffer, "posts")
                posts_buffer = []  # Очищаем буфер

            # (Если есть логика для комментов, аналогично для comments_buffer)

        # 3. ВАЖНО: Сохраняем остатки после выхода из цикла
        if posts_buffer:
            self._save_batch(posts_buffer, "posts")

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
