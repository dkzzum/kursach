import json
from typing import Dict

import requests
from src.core.config import headers_ym_all, cookies_ym


def write_rec_info(rec_track: Dict[str, Dict[str, int]], target_user: str):
    s = requests.Session()
    s.headers.update(headers_ym_all)
    s.cookies.update(cookies_ym)

    all_track_score = {}
    rec_list = []
    for _, track_dict in rec_track.items():
        for track_id, score in track_dict.items():
            rec_list.append(track_id)
            all_track_score[track_id] = score

    params = {
        "trackIds": rec_list
    }

    r = s.post("https://api.music.yandex.ru/tracks", timeout=10, params=params)
    data = json.loads(r.text)

    finale_rec_data = {}
    for idx, track in enumerate(data):
        finale_rec_data[idx] = {
            'realId': track.get('realId'),
            'title': track.get('title'),
            'score': all_track_score[int(track.get('id'))],
            'available': track.get('available'),
            'artists': {
                            i: {
                                'id': artist.get('id'),
                                "name": artist.get('name'),
                                'available': artist.get('available'),
                            }
                            for i, artist in enumerate(track.get('artists', []), start=1)
                        },
            'albums': {
                            i: {
                                'id': album.get('id'),
                                "title": album.get('title'),
                                'type': album.get('type'),
                                'genre': album.get('genre'),
                                'releaseDate': album.get('releaseDate'),  # Исправлена опечатка
                                'trackCount': album.get('trackCount'),
                            }
                            for i, album in enumerate(track.get('albums', []), start=1)
                        },
            'type': track['type']
        }

    with open(f'../data/recommendations/rec_for_{target_user}.json', '+w', encoding="utf-8") as f:
        json.dump(finale_rec_data, f, indent=4, ensure_ascii=False)

    return finale_rec_data
