from src.secret.config import headers_kp, cookies_kp
import requests
import json

s = requests.Session()
s.headers.update(headers_kp)
s.cookies.update(cookies_kp)

# user_id = '1537003491'
user_id = '98494675'
r1 = s.post(f"https://www.kinopoisk.ru/api/profile/", timeout=10)

# print(r1.text)
print(json.dumps(json.loads(r1.text), indent=4))
