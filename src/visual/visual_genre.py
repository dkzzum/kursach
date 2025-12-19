from typing import Dict
from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import json


sns.set_theme(style="darkgrid")


def get_users_file(dir: str, user_count: int, target_user_id: str) -> Dict[str, int]:
    """
    Анализирует плейлисты пользователей и подсчитывает жанры треков.

    Args:
        dir: путь к директории с JSON файлами пользователей
        user_count: количество файлов для обработки
        target_user_id: ID целевого пользователя для исключения

    Returns:
        Словарь с подсчетом жанров {жанр: количество}
    """
    tracks_genre = defaultdict(int)

    try:
        file_list = os.listdir(dir)
    except FileNotFoundError:
        print(f"Ошибка: Директория '{dir}' не найдена!")
        return dict(tracks_genre)

    files_to_process = file_list[:user_count]
    target_file = f'{target_user_id}.json'

    if target_file not in files_to_process and target_file in file_list:
        files_to_process.append(target_file)

    for filename in files_to_process:
        filepath = os.path.join(dir, filename)

        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                user_data = json.load(file)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Ошибка при чтении файла {filename}: {e}")
            continue

        if not user_data:
            continue

        playlists = user_data.get('playlists')
        if not playlists:
            continue

        owner = user_data.get('owner')
        if owner and str(owner.get('uid')) == str(target_user_id):
            print(f"Пропускаем файл целевого пользователя: {filename}")
            continue

        for playlist_name, playlist_data in playlists.items():
            if not playlist_data or not isinstance(playlist_data, dict):
                continue

            tracks = playlist_data.get('tracks', [])
            if not tracks:
                continue

            for track in tracks:
                if not track:
                    continue

                albums = track.get('albums')
                if not albums or not isinstance(albums, dict):
                    continue

                for album_key, album_data in albums.items():
                    if not album_data or not isinstance(album_data, dict):
                        continue

                    genre = album_data.get('genre')
                    if genre:
                        tracks_genre[genre] += 1

    return dict(tracks_genre)


if __name__ == '__main__':
    # Параметры
    dir_path = '../data/users_playlists/'
    user_count = 200
    target_user_id = '1537003491'

    # Получаем статистику по жанрам
    tracks_genre = get_users_file(dir=dir_path, user_count=user_count, target_user_id=target_user_id)

    # Выводим результаты
    if tracks_genre:
        print("\nРезультаты анализа жанров:")
        print(json.dumps(tracks_genre, indent=4, ensure_ascii=False, sort_keys=True))

        df = pd.DataFrame(list(tracks_genre.items()), columns=['Genre', 'Count'])
        df = df.sort_values('Count', ascending=False).reset_index(drop=True)

        print(f"\nВсего уникальных жанров: {len(df)}")
        print(f"Всего треков с жанрами: {df['Count'].sum()}")

        print("\nТоп-10 самых популярных жанров:")
        print(df.head(10).to_string(index=False))

        # Опционально: визуализация
        plt.figure(figsize=(12, 6))
        sns.barplot(data=df[:30], x='Count', y='Genre')
        plt.title('Топ-15 жанров')
        plt.tight_layout()
        plt.savefig('genres_distribution.png', dpi=150)
        print("\nГрафик сохранен в 'genres_distribution.png'")
    else:
        print("Жанры не найдены!")
