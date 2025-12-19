import os
import json
from typing import Dict


def get_users_file(dir: str, user_count: int, target_users: str):
    file_dir = os.listdir(dir)
    users = file_dir[:user_count]
    users.append(f'{target_users}.json')
    users_track = {}

    for user in users:
        with open(dir + user, 'r') as file:
            user_palylists: dict = json.load(file)

        if not user_palylists:
            continue

        like_track = user_palylists.get('playlists').get('Мне нравится')
        if like_track is None:
            continue

        if user_palylists.get('owner').get('uid') in users_track:
            continue

        tracks = set()
        for track in like_track.get('tracks'):
            tracks.add(track['id'])

        users_track[user[:-5]] = sorted(list(tracks))

    return users_track


def save_json(users_track: Dict):
    with open(f"../data/user_like_tracks/users_like.json", "w", encoding="utf-8") as file:
        json.dump(users_track, file, ensure_ascii=False, indent=4)


def get_likes_users(dir: str, user_count: int, target_users: str):
    users_track = get_users_file(dir=dir, user_count=user_count, target_users=target_users)
    save_json(users_track)


if __name__ == '__main__':
    dir = '../data/users_playlists/'
    user_count = 200
    target_users = '1537003491'
    get_likes_users(dir=dir, user_count=user_count, target_users=target_users)
