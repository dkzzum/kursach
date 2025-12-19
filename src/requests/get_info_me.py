from src.secret.config import headers_kp, cookies_kp
import requests
import json

s = requests.Session()
s.headers.update(headers_kp)
s.cookies.update(cookies_kp)

# user_id = '1537003491'
user_id = '98494675'
r = s.get(f"https://www.kinopoisk.ru/api/user/", timeout=10)
data = json.loads(r.text)

with open('json_requests/info_me.json', '+w', encoding="utf-8") as f:
    json.dump(data, f, indent=4)