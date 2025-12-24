import json


headers_b = '''
accept
*/*
accept-encoding
gzip, deflate, br, zstd
accept-language
ru
origin
https://music.yandex.ru
priority
u=1, i
referer
https://music.yandex.ru/
sec-ch-ua
"Chromium";v="140", "Not=A?Brand";v="24", "YaBrowser";v="25.10", "Yowser";v="2.5"
sec-ch-ua-arch
"arm"
sec-ch-ua-bitness
"64"
sec-ch-ua-full-version-list
"Chromium";v="140.0.7339.1186", "Not=A?Brand";v="24.0.0.0", "YaBrowser";v="25.10.2.1186", "Yowser";v="2.5"
sec-ch-ua-mobile
?0
sec-ch-ua-platform
"macOS"
sec-ch-ua-platform-version
"26.1.0"
sec-ch-ua-wow64
?0
sec-fetch-dest
empty
sec-fetch-mode
cors
sec-fetch-site
same-site
user-agent
Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 YaBrowser/25.10.0.0 Safari/537.36
x-request-id
4a52634b-bc44-49e7-9e05-497de94c3fb4
x-requested-with
XMLHttpRequest
x-retpath-y
https://music.yandex.ru/artist/41191
x-yandex-music-client
YandexMusicWebNext/1.0.0
x-yandex-music-multi-auth-user-id
1537003491
x-yandex-music-without-invocation-info
1
'''.split('\n')

headers = {}
for s in range(-1, len(headers_b) - 1, 2):
    headers[headers_b[s]] = headers_b[s + 1]

print(json.dumps(headers, indent=4))
