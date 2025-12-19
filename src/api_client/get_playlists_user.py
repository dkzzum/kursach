from math import ceil
from typing import Dict, List, Any
from src.core.config import cookies_ym, headers_ym_base
import requests
import json


def get_playlists(s: requests.Session, user_id: str) -> Dict[int, Any]:
    """
    Получить список всех плейлистов, созданных пользователем.

    :param s: requests.Session – авторизованная сессия запросов
    :param user_id: str – UID пользователя Яндекс.Музыки
    :return: Dict[int, Any] – словарь вида:
        {
            playlist_kind_id (int): {...},
            ...
        }
    """
    try:
        response = s.get(
            f"https://api.music.yandex.ru/users/{user_id}/playlists/list/kinds?addPlaylistWithLikes=true",
            timeout=10
        )

        if response.status_code != 200:
            raise RuntimeError("Ошибка при запросе списка плейлистов")

        return json.loads(response.text)['result']

    except Exception as e:
        print(f"Не удалось получить плейлисты пользователя {user_id}. По причине: {e}")
        return None


def get_tracks_from_playlists(
    s: requests.Session,
    playlists_list: Dict[int, Any],
    user_id: str
) -> Dict[str, Any]:
    """
    Получить данные треков и информацию о плейлистах.

    :param s: requests.Session
    :param playlists_list: Dict[int, Any]
    :param user_id: str
    :return: Dict[str, Any] – структура:
        {
            "playlists": {
                "Название плейлиста": {
                    "name": str,
                    "playlistUuid": str,
                    "link": str,
                    "tracks": [ {объект трека}, ... ],
                    "available": bool,
                    "trackCount": int,
                    "visibility": str,
                    "collective": bool,
                    "created": str,
                    "modified": str,
                    "isBanner": bool
                },
                ...
            },
            "owner": {...}
        }
    """
    try:
        tracks_dict: Dict[str, Any] = {'playlists': {}}

        for playlist_id in playlists_list:
            response = s.get(
                f"https://api.music.yandex.ru/users/{user_id}/playlists?kinds={playlist_id}",
                timeout=10
            )

            if response.status_code != 200:
                raise RuntimeError(f"Ошибка при запросе треков плейлиста {playlist_id}")

            result = json.loads(response.text)['result'][0]

            if result.get('owner', {}).get('uid'):
                if 'owner' not in tracks_dict:
                    tracks_dict['owner'] = result['owner']

                playlist_name: str = result['title']
                track_ids: List[Dict] = [
                    track for track in result.get('tracks', [])
                ]

                tracks_dict['playlists'][playlist_name] = {
                    'name': playlist_name,
                    'playlistUuid': result.get('playlistUuid'),
                    'link': f'https://music.yandex.ru/playlists/{result.get("playlistUuid")}',
                    'kind': result['kind'],
                    'available': result['available'],
                    'trackCount': result['trackCount'],
                    'visibility': result['visibility'],
                    'collective': result['collective'],
                    'revision': result['revision'],
                    'created': result['created'],
                    'modified': result['modified'],
                    'isBanner': result['isBanner'],
                    'tracks': track_ids,
                }

        return tracks_dict
    except Exception as e:
        raise RuntimeError(f"Ошибка при загрузке треков: {e}")


def request_info_about_music(
    s: requests.Session,
    playlists_info: Dict[str, Any]
) -> Dict[str, Dict[int, Dict[str, Any]]]:
    """
    Обновляет информацию о треках внутри плейлистов с использованием API Яндекс.Музыки.

    :param s: requests.Session – сессия для выполнения HTTP-запросов к API.
    :param playlists_info: Dict[str, Any] – словарь с плейлистами и треками.
        Пример структуры:
        {
            "playlists": {
                "Название плейлиста": {
                    "name": str,
                    "playlistUuid": str,
                    "link": str,
                    "available": bool,
                    "trackCount": int,
                    "visibility": str,
                    "collective": bool,
                    "created": str,
                    "modified": str,
                    "isBanner": bool,
                    "tracks": [
                        {
                            "id": int,
                            "timestamp": str,
                            ...
                        },
                        ...
                    ]
                },
                ...
            },
            "owner": {...}
        }
    :return: Dict[str, Dict[int, Dict[str, Any]]] – словарь с обновлённой информацией о треках,
        где ключи — это playlist_id, а значения — словари с информацией о треках.
    """
    try:
        # Собираем все уникальные ID треков из всех плейлистов
        all_track_ids = set()
        for playlist_info in playlists_info['playlists'].values():
            for track in playlist_info['tracks']:
                all_track_ids.add(track['id'])

        print(f'Получаем информацию о {len(all_track_ids)} уникальных треках...')

        # Запрашиваем информацию о всех треках сразу
        all_tracks_info: Dict[int, Dict[str, Any]] = {}
        track_ids_list = list(all_track_ids)

        chunk_size = 100
        chunks_count = ceil(len(track_ids_list) / chunk_size)

        for c in range(chunks_count):
            chunk = track_ids_list[c * chunk_size:(c + 1) * chunk_size]

            response = s.post(
                "https://api.music.yandex.ru/tracks",
                params={"trackIds": chunk},
                timeout=10
            )

            if response.status_code != 200:
                print(response.text)
                # print(f"Ошибка при запросе информации о треках - чанк {c + 1}")
                raise RuntimeError(f"Ошибка при запросе информации о треках - чанк {c + 1}")

            result: List[Dict] = json.loads(response.text).get('result', [])

            for track in result:
                if 'error' in track:
                    print(f'Не найдена информация о треке: {track["id"]}')
                    continue

                all_tracks_info[track['id']] = {
                    "title": track['title'],
                    'available': track['available'],
                    'durationMs': track['durationMs'],
                    'type': track.get('type'),
                    "artists": {
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
                    }
                }

            # print(f'Обработано чанков: {c + 1}/{chunks_count}')

        for playlist_name, playlist_info in playlists_info['playlists'].items():
            print(f'Обновляем информацию для плейлиста "{playlist_name}"...')
            for track in playlist_info['tracks']:
                track_id = str(track['id'])
                if track_id in all_tracks_info:
                    del track['albumId']
                    track.update(all_tracks_info[track_id])

        return playlists_info
    except Exception as e:
        print(f"Ошибка при анализе информации о треках: {e}")
        # raise f"Ошибка при анализе информации о треках: {e}"
        # return playlists_info


def save_json(user_id: str, data: Dict[str, Any]) -> None:
    """
    Сохранить данные в JSON-файл playlist_track.json
    """
    print(f"Сохраняем данные в {user_id}.json...")
    with open(f"../data/users_playlists/{user_id}.json", "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)
    # print("Файл успешно сохранён!")


def get_playlists_user(user_id: str, rating: str) -> None:
    s = requests.Session()
    s.headers.update(headers_ym_base)
    s.cookies.update(cookies_ym)

    user_id = "1537003491"
    playlists_list = get_playlists(s=s, user_id=user_id)
    if playlists_list is None:
        print(f'При нахождении плейлистов для пользователя – {user_id} произошла ошибка!')
        return

    playlists_info = get_tracks_from_playlists(s=s, playlists_list=playlists_list, user_id=user_id)
    full_info = request_info_about_music(s=s, playlists_info=playlists_info)
    if full_info is not None:
        full_info['rating'] = rating[:-1] if rating else None

    save_json(user_id=user_id, data=full_info)


# if __name__ == "__main__":
#     get_playlists_user(-1, '100.00%')
