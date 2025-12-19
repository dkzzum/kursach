from typing import Dict, Any, Optional, Set, Union
from src.secret.config import headers_ym, cookies_ym

import requests
import json


TrackID = Union[int, str]
ArtistInfo = Dict[int, Dict[str, str]]
TrackInfo = Dict[int, Dict[str, Any]]


def request_info_about_music(s: requests.Session, track_ids: Set[TrackID]) -> TrackInfo:
    """
    Получить детальную информацию о треках по их ID.

    :param s: requests.Session – авторизованная сессия
    :param track_ids: Set[TrackID] – множество ID треков
    :return: TrackInfo – структура вида:
        {
            1: {
                "title": str,
                "track_id": str | int,
                "artists": {
                    1: {"name": str},
                    2: {"name": str},
                }
            },
            ...
        }
    """
    try:
        response = s.post(
            "https://api.music.yandex.ru/tracks",
            timeout=10,
            params={"trackIds": list(track_ids)}
        )

        if response.status_code != 200:
            raise RuntimeError("Ошибка при запросе информации о треках!")

        result = response.json().get("result", [])
        tracks: TrackInfo = {}

        print("Получаем информацию о совпадающих треках...")

        for idx, track in enumerate(result, start=1):
            artists: ArtistInfo = {
                i: {"name": art["name"]}
                for i, art in enumerate(track.get("artists", []), start=1)
            }

            tracks[idx] = {
                "title": track.get("title"),
                "track_id": track.get("id"),
                "artists": artists
            }

        return tracks

    except Exception as e:
        raise RuntimeError(f"Ошибка при запросе данных о треках: {e}")


def request_playlist(s: requests.Session, playlist_uuid: str) -> Optional[Dict[str, Any]]:
    """
    Запросить информацию о плейлисте по UUID.

    :param s: requests.Session
    :param playlist_uuid: str – уникальный идентификатор плейлиста (начинается с 'lk.')
    :return: Dict с данными плейлиста или None, если ошибка
    """
    try:
        print("Получаем данные плейлиста...")

        response = s.get(
            f"https://api.music.yandex.ru/playlist/{playlist_uuid}?resumeStream=false&richTracks=false",
            timeout=10
        )

        if response.status_code == 200:
            return response.json()

        raise RuntimeError("Ошибка ответа сервера")

    except Exception as e:
        raise RuntimeError(f"Ошибка при загрузке плейлиста ({playlist_uuid}): {e}")


def get_track_ids_from_playlist(s: requests.Session, playlist_uuid: str) -> Set[TrackID]:
    """
    Извлечь уникальные ID треков из плейлиста.

    :param s: requests.Session
    :param playlist_uuid: str
    :return: Set[TrackID]
    """
    playlist_data = request_playlist(s, playlist_uuid)

    if not playlist_data:
        raise RuntimeError("Плейлист не найден или пуст")

    print("Извлекаем треки из плейлиста...")

    tracks = playlist_data.get("result", {}).get("tracks", [])
    return {track.get("id") for track in tracks if "id" in track}


def save_json(musics: TrackInfo) -> None:
    """
    Сохранить информацию о треках в JSON-файл.
    """
    print("Сохраняем результат в music.json...")
    with open("music.json", "w", encoding="utf-8") as file:
        json.dump(musics, file, ensure_ascii=False, indent=4)
    print("Готово! ✅")


def main():
    s = requests.Session()
    s.headers.update(headers_ym)
    s.cookies.update(cookies_ym)

    # UUID плейлистов
    tracks_1 = get_track_ids_from_playlist(s=s, playlist_uuid='lk.e6007380-20fa-4ba1-b379-9ae2a87ba462')
    # tracks_2 = get_track_ids_from_playlist(s, 'lk.7d3ab245-9460-4731-ae48-df38d9cb2d39')
    tracks_2 = get_track_ids_from_playlist(s=s, playlist_uuid='lk.38d17df6-b6be-460e-af08-88f11d8e1f5a')

    print("Сравниваем плейлисты...")
    intersections = tracks_1.intersection(tracks_2)

    tracks_info = request_info_about_music(s=s, track_ids=intersections)
    save_json(musics=tracks_info)


if __name__ == '__main__':
    main()
