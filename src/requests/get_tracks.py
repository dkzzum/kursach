import json
import requests
from src.core.config import headers_ym_base, cookies_ym

s = requests.Session()
s.headers.update(headers_ym_base)
s.cookies.update(cookies_ym)

params = {
    "trackIds": "104102778"
}

r = s.get("https://api.music.yandex.ru/tracks", timeout=10, params=params)
data = json.loads(r.text)

with open('json_requests/track.json', '+w', encoding="utf-8") as f:
    json.dump(data, f, indent=4)
