from src.secret.config import headers_ym_base, cookies_ym
import requests
import json


s = requests.Session()
s.headers.update(headers_ym_base)
s.cookies.update(cookies_ym)

user_id = '1537003491'
r = s.get(f"https://api.music.yandex.ru/users/{user_id}/playlists/list/kinds?addPlaylistWithLikes=true", timeout=10)

data = json.loads(r.text)

with open('json_requests/playlists_list.json', '+w', encoding="utf-8") as f:
    json.dump(data, f, indent=4)