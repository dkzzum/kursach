from src.secret.config import headers_kp, cookies_kp
import requests
import json

# headers_kp['Referer'] = 'https://www.kinopoisk.ru/community/cf_user/130901766/'
# headers_kp['Content-Type'] = 'application/json'
# headers_kp['Accept'] = 'application/json'
# headers_kp['X-Requested-With'] = 'XMLHttpRequest'
# headers_kp['Origin'] = 'XMLHttpRequest'
# headers_kp['X-Requested-With'] = 'XMLHttpRequest'


s = requests.Session()
s.headers.update(headers_kp)
s.cookies.update(cookies_kp)

# user_id = '1537003491'
user_id = '98494675'
r1 = s.get(f"https://www.kinopoisk.ru/api/user/", timeout=10)

print(r1.text)
print(json.dumps(json.loads(r1.text), indent=4))