import json
import requests
from src.secret.config import headers_ym_base, cookies_ym

s = requests.Session()
s.headers.update(headers_ym_base)
s.cookies.update(cookies_ym)

artist_id = '11307507'
response = s.get(
    f"https://api.music.yandex.ru/users/{1537003491}/playlists?kinds={3}",
    timeout=10
)
data = json.loads(response.text)

with open('json_requests/playlist_data.json', '+w', encoding="utf-8") as f:
    json.dump(data, f, indent=4)