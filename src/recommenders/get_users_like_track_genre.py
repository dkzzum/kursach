import json
import os
from typing import Dict


def handler_users_file(user_count: int, target_users: str, dir: str = '../data/users_playlists/'):
    file_dir = os.listdir(dir)
    users = file_dir[:user_count]
    users.append(f'{target_users}.json')
    genres_track = {}
    set_genre = set()
    track_count = 0
    total_fail = 0

    for user in users:
        with open(dir + user, 'r') as file:
            user_palylists: dict = json.load(file)

        # print(json.dumps(user_palylists, indent=4)[:5000])

        if not user_palylists:
            continue

        like_track = user_palylists.get('playlists').get('Мне нравится')
        if like_track is None:
            total_fail += 1
            continue

        if user_palylists.get('owner').get('uid') in genres_track:
            continue

        genre_track = {}
        album_false = 0
        for track in like_track.get('tracks'):
            album = track.get('albums')
            if not album:
                album_false += 1
                continue

            genre = album.get('1').get('genre')
            if not genre:
                album_false += 1
                continue

            track_count += 1
            if genre in genre_track:
                genre_track[genre].append(track.get('id'))
            else:
                set_genre.add(genre)
                genre_track[genre] = [track.get('id')]

        # print(f'Пропусков в жанрах у пользователя {user[:-5]}: {album_false}')
        total_fail += album_false
        genres_track[user[:-5]] = genre_track

    print(f'Треков обработано: {track_count}')
    print(f'Всего пропущено треков: {total_fail}')
    print(f'Всего жанров: {len(set_genre)}')
    # print(f'Жанры:')
    # for genre in set_genre:
    #     print(genre)

    return genres_track


def save_json(users_track: Dict, name_file: str = 'user_genre'):
    with open(f"../data/user_like_tracks/{name_file}.json", "w", encoding="utf-8") as file:
        json.dump(users_track, file, ensure_ascii=False, indent=4)


def get_genres_and_likes_users(user_count: int, target_users: str, dir: str = '../data/users_playlists/'):
    users_track = handler_users_file(dir=dir, user_count=user_count, target_users=target_users)
    save_json(users_track)


if __name__ == '__main__':
    user_count = 100
    target_users = '1537003491'
    dir = '../data/users_playlists/'

    get_genres_and_likes_users(user_count, target_users, dir)