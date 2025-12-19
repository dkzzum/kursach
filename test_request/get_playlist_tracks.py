import requests
import json
from src.core.config import headers_ym_base, cookies_ym

s = requests.Session()
s.headers.update(headers_ym_base)
s.cookies.update(cookies_ym)

playlist_uuid = 'lk.c494fcf8-f870-44fb-8819-be239d46d4fd'
playlist_uuid = 'd2fee75c-a340-4027-b77e-4bdf05d44514'
# playlist_id = '3'

# 1) GET главной страницы (получаем возможные дополнительные cookie/редиректы)
# r1 = s.get(f"https://api.music.yandex.ru/users/1537003491/playlists/", timeout=10)
# r1 = s.get(f"https://api.music.yandex.ru/users/1537003491/playlists/{playlist_id}", timeout=10)
r1 = s.get(f"https://api.music.yandex.ru/playlist/{playlist_uuid}?resumeStream=false&richTracks=false", timeout=10)
# r1 = s.get(f"https://music.yandex.ru/users/dokuchaewaver/playlists/3?utm_medium=copy_link&ref_id=d2fee75c-a340-4027-b77e-4bdf05d44514", timeout=10)


print("main:", r1.status_code)


all_data = json.loads(r1.text)
# tracks = all_data['result']
print(json.dumps(all_data, indent=4))
# music_id_list = []
#
# for track in tracks:
#     music_id_list.append(track['id'])


