import json
from typing import Dict, List, Tuple

import numpy as np
from numpy import ndarray


def read_data_json(path: str = '../data/user_like_tracks/',
                   file_name: str = 'user_genre.json') -> Dict[str, Dict[str, List[str]]]:
    """
    Загружает данные о жанрах пользователей из JSON файла.

    Returns:
        Dict[str, Dict[str, List[str]]]: Словарь вида:
            {
                'user_id': {
                    'genre_name': ['track_id1', 'track_id2', ...],
                    ...
                },
                ...
            }
    """
    with open(path + file_name, 'r') as file:
        user_genre: Dict[str, Dict[str, List[str]]] = json.load(file)

    return user_genre


def handle_data(target_user: str) -> Tuple[Dict[str, Dict[str, List[int]]], List[str], int, Dict[str, int], Dict[str, int]]:
    """
    Обрабатывает данные пользователей и жанров.

    Returns:
        Tuple:
            - handle_genre: Словарь {user_id: {genre_name: count_tracks}}
            - target_user_idx: Индекс целевого пользователя в матрице
            - user_to_idx: Маппинг {user_id: index в матрице}
            - genre_to_idx: Маппинг {genre_name: index в матрице}
            - users_list: Список со всеми пользователями, в порядке матрицы
    """
    user_genre_data = read_data_json()
    all_genre = set()
    users_list = []
    handle_genre = {}
    failed_count = 0

    for user_id, genres in user_genre_data.items():
        new_genre = {}
        current_user_genres = sorted(genres.keys())

        for genre_name in current_user_genres:
            all_genre.add(genre_name)
            new_genre[genre_name] = genres[genre_name]

        if not new_genre:
            failed_count += 1
            continue

        users_list.append(user_id)
        handle_genre[user_id] = new_genre

    users_list.sort()
    genre_list = sorted(all_genre)

    # Маппинги
    user_to_idx = {
        user_id: idx for idx, user_id in enumerate(users_list)
    }
    genre_to_idx = {
        genre: idx for idx, genre in enumerate(genre_list)
    }

    if target_user not in user_to_idx:
        raise ValueError(f"Целевой пользователь {target_user} не найден в данных!")

    target_user_idx = user_to_idx[target_user]

    # Статистика
    print(f'Пропущено пользователей: {failed_count}')
    print(f'Валидных пользователей: {len(users_list)}')
    print(f'Целевой пользователь: {target_user} (индекс: {target_user_idx})')
    print(f'Всего уникальных жанров: {len(genre_list)}')

    return handle_genre, users_list, target_user_idx, user_to_idx, genre_to_idx


def create_matrix(
    handle_genre: Dict[str, Dict[str, int]],
    user_to_idx: Dict[str, int],
    genre_to_idx: Dict[str, int]
) -> ndarray:
    """
    Создает взвешенную матрицу пользователи × жанры.

    Каждый элемент matrix[i, j] содержит количество треков жанра j
    у пользователя i.

    Returns:
        ndarray: Матрица формы (num_users, num_genres) с типом int32
    """

    matrix: ndarray = np.zeros(
        shape=(len(user_to_idx), len(genre_to_idx)),
        dtype=np.int32
    )

    for user_id, user_genres in handle_genre.items():
        uid: int = user_to_idx[user_id]
        for genre, count in user_genres.items():
            gid: int = genre_to_idx[genre]
            matrix[uid, gid] = len(count)

    print(f'Форма матрицы: {matrix.shape}')
    print(f'Наполненность: {np.count_nonzero(matrix) / matrix.size * 100:.2f}%')

    return matrix


def get_data_matrix():
    target_users = '1537003491'
    handle_genre, users_list, target_user_idx, user_to_idx, genre_to_idx = handle_data(target_users)
    matrix = create_matrix(handle_genre, user_to_idx, genre_to_idx)
    print(matrix)

    return matrix, target_user_idx, user_to_idx, genre_to_idx


if __name__ == '__main__':
    get_data_matrix()
