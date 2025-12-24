import json
import time
from math import ceil
from typing import Dict, List, Any
from collections import deque
from core.config import headers_ym_all, cookies_ym
import requests


class MusicCrawler:
    def __init__(self, target_artist, limit=500):
        self.s = requests.Session()
        self.s.headers.update(headers_ym_all)
        self.s.cookies.update(cookies_ym)

        self.queue = deque([target_artist])
        self.seen_ids = {target_artist}

        self.limit = limit

    def _save_to_db(self, collection, data):
        ...

    def _download_profile(self, artist_id) -> str|int:
        artist_response = self.s.get(f'https://api.music.yandex.ru/artists/{artist_id}/brief-info', timeout=10)
        if artist_response.status_code != 200:
            raise Exception(f"Bad status code: {artist_response.status_code}")

        artist_data: Dict[str, Any] = json.loads(artist_response.text)
        self.seen_ids.add(artist_id)

        self._save_to_db('raw_artist', artist_data)

        similar_artist: List[Dict[str, Any]] = artist_data.get('similarArtists', [])
        for s_artist in similar_artist:
            id_s_artist = s_artist['id']
            if id_s_artist not in self.seen_ids:
                self.seen_ids.add(id_s_artist)
                self.queue.append(id_s_artist)

    def _download_tracks(self, artist_id):
        all_tracks_response = self.s.get(f'https://api.music.yandex.ru/artists/{artist_id}/track-ids?`')
        if all_tracks_response.status_code != 200:
            raise Exception(f"Bad status code: {all_tracks_response.status_code}")

        all_tracks = json.loads(all_tracks_response.text)
        if not all_tracks:
            return

        chunk_size = 100
        chunks_count = ceil(len(all_tracks) / chunk_size)

        for c in range(chunks_count):
            chunk = all_tracks[c * chunk_size:(c + 1) * chunk_size]

            tracks_response = self.s.post(
                "https://api.music.yandex.ru/tracks",
                json={"trackIds": chunk},
                timeout=10
            )

            tracks = json.loads(tracks_response.text)
            for track in tracks:
                self._save_to_db('raw_track', track)

    def _save_checkpoint(self):
        checkpoint_data = {
            "queue": list(self.queue),
            "seen_ids": list(self.seen_ids),
            "timestamp": time.time()
        }

        # self.db['crawler_state'].replace_one({}, checkpoint_data, upsert=True)
        # ИЛИ (лучше) записать в MongoDB как "состояние"
        # db.crawler_state.insert_one({
        #     "timestamp": time.time(),
        #     "queue": remaining_ids,
        #     "seen_ids": list(seen_ids) # Чтобы не скачивать их повторно
        # })

        print("Checkpoint saved.")

    def run(self):
        processed_count = 0
        while processed_count < self.limit or self.queue:
            current_id = self.queue.popleft()
            try:
                self._download_profile(current_id)
                self._download_tracks(current_id)

                processed_count += 1
                print(processed_count, ' – ', len(self.seen_ids), ' артистов')

                if processed_count % 10 == 0:
                    self._save_checkpoint()

            except Exception as e:
                print(f"Error processing {current_id}: {e}")
                time.sleep(2)


if __name__ == "__main__":
    bot = MusicCrawler(target_artist='41191')
    bot.run()
