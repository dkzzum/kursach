import json
from math import ceil
from typing import Dict, List, Any
from collections import deque
from core.config import headers_ym_all, cookies_ym
import requests

s = requests.Session()
s.headers.update(headers_ym_all)
s.cookies.update(cookies_ym)

target_artist = '41191'
queue = deque([target_artist])
seen_ids = {target_artist}
processed_count = 0

while processed_count < 500:
    current_id = queue.popleft()
    artist_response = s.get(f'https://api.music.yandex.ru/artists/{current_id}/brief-info', timeout=10)

    artist_data: Dict[str, Any] = json.loads(artist_response.text)

    # Запись в БД raw_artist artist_data

    processed_count += 1
    print(processed_count, ' – ', len(seen_ids), ' артистов')

    seen_ids.add(current_id)

    similar_artist: List[Dict[str, Any]] = artist_data['similarArtists']

    for s_artist in similar_artist:
        id_s_artist = s_artist['id']
        if id_s_artist not in seen_ids:
            seen_ids.add(id_s_artist)
            queue.append(id_s_artist)

    all_tracks_response = s.get(f'https://api.music.yandex.ru/artists/{current_id}/track-ids?`')
    all_tracks = json.loads(all_tracks_response.text)

    chunk_size = 100
    chunks_count = ceil(len(all_tracks) / chunk_size)

    for c in range(chunks_count):
        chunk = all_tracks[c * chunk_size:(c + 1) * chunk_size]

        tracks_response = s.post(
            "https://api.music.yandex.ru/tracks",
            data={"trackIds": chunk},
            timeout=10
        )

        tracks = json.loads(tracks_response.text)
        for track in tracks:
            # Запись в БД raw_track
            ...

remaining_ids = list(queue)

if remaining_ids:
    print(f"Сохраняем {len(remaining_ids)} необработанных артистов для следующего запуска...")

    checkpoint_data = {
        "queue": list(queue),  # Те, кого надо скачать (1266 шт)
        "seen_ids": list(seen_ids)  # Все, кого мы "знаем" (1766 шт)
    }

    with open('crawler_state.json', 'w') as f:
        json.dump(checkpoint_data, f)

    # ИЛИ (лучше) записать в MongoDB как "состояние"
    # db.crawler_state.insert_one({
    #     "timestamp": time.time(),
    #     "queue": remaining_ids,
    #     "seen_ids": list(seen_ids) # Чтобы не скачивать их повторно
    # })
