import json
import requests
from src.secret.config import headers_ym_base, cookies_ym

s = requests.Session()
s.headers.update(headers_ym_base)
s.cookies.update(cookies_ym)

user_id = '777425285'
r = s.get(f"https://api.music.yandex.ru/users/{user_id}/playlists/list/kinds?addPlaylistWithLikes=true", timeout=10)

playlist_id = json.loads(r.text)['result'][0]

response = s.get(
    f"https://api.music.yandex.ru/users/{user_id}/playlists?kinds={playlist_id}",
    timeout=10
)
playlist_data = json.loads(response.text)
print(json.dumps(playlist_data, indent=4))
