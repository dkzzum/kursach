import json
import requests
from src.config import cookies, headers


s = requests.Session()
s.headers.update(headers)
s.cookies.update(cookies)

with open('../src/music.json', 'r', encoding='utf-8') as musics_data:
    musics = json.load(musics_data)

# print(musics)
musics_list = []
for music in musics.values():
    musics_list.append(music['track_id'])

params = {
    "trackIds": musics_list[:200]
}

r1 = s.get("https://api.music.yandex.ru/tracks", timeout=10, params=params)
print(json.dumps(json.loads(r1.text), indent=4))
