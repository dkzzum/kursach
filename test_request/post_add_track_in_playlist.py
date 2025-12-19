import requests
import json
from src.secret.config import headers_ym_base, cookies_ym

s = requests.Session()
headers_ym_base['Accept'] = '*/*'
headers_ym_base['accept-language'] = 'ru'


s.headers.update(headers_ym_base)
s.cookies.update(cookies_ym)

user_id = '777425285'
playlist_id = 1013
revision = 47
url = f'https://api.music.yandex.ru/users/{user_id}/playlists/{playlist_id}/change-relative'
data = {
    'diff': json.dumps([{
        "op": "insert",
        "at": 0,
        "tracks": [
            {
                "id": "53114",
                "albumId": 2870259
            }
        ]
    }]),
    'revision': revision
}

r = s.post(url, params=data)
print(r.url)
print(r.status_code)
print(r.text)