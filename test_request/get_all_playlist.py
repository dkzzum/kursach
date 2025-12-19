from src.secret.config import headers_ym_base, cookies_ym
import requests
import json


s = requests.Session()
s.headers.update(headers_ym_base)
s.cookies.update(cookies_ym)

user_id = '777425285'
user_id = '95673834'
r1 = s.get(f"https://api.music.yandex.ru/users/{user_id}/playlists/list/kinds?addPlaylistWithLikes=true", timeout=10)
print("get all playlist:", r1.status_code)

print(json.dumps(json.loads(r1.text), indent=4))
