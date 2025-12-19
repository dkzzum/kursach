from typing import Dict, List, Tuple, Optional


def _init_known_genres(
        target_user: str,
        handle_genre: Dict[str, Dict[str, List[str]]],
) -> Dict[str, Dict[str, float]]:
    """
    Создает словарь жанров пользователя с пустыми словарями для треков.

    Args:
        target_user: имя пользователя, для которого делаем рекомендации
        handle_genre: структура {user: {genre: [track_ids]}}

    Returns:
        Словарь {genre_name: {}} для всех жанров target_user
    """
    target_user_genre = handle_genre[target_user]
    return {genre_name: {} for genre_name in target_user_genre}


def _aggregate_neighbor_tracks(
        know_genre: Dict[str, Dict[str, float]],
        handle_genre: Dict[str, Dict[str, List[str]]],
        top_k_neighbors: List[Tuple[int, float]],
        idx_to_user: Dict[int, str],
) -> None:
    """
    Агрегирует рейтинги треков по жанрам от соседей пользователя.
    Для каждого соседа и его жанров суммирует баллы трекам в know_genre.

    Args:
        know_genre: аккумулятор рекомендаций по жанрам
        handle_genre: структура {user: {genre: [track_ids]}}
        top_k_neighbors: список соседей (индекс, score)
        idx_to_user: отображение индекса на имя пользователя

    Modifies:
        know_genre — добавляет/складывает scores трекам
    """
    for idx_user, score in top_k_neighbors:
        user_id = idx_to_user[idx_user]
        for genre_name, track_list in handle_genre[user_id].items():
            if genre_name in know_genre:
                for track_id in track_list:
                    know_genre[genre_name][track_id] = (
                            know_genre[genre_name].get(track_id, 0) + score
                    )


def _get_target_user_track_set(
        target_user: str,
        handle_genre: Dict[str, Dict[str, List[str]]],
) -> set:
    """
    Получает множество всех треков пользователя target_user.
    Используется для фильтрации уже знакомых треков.

    Args:
        target_user: имя пользователя
        handle_genre: структура {user: {genre: [track_ids]}}

    Returns:
        Множество всех track_id пользователя
    """
    target_user_tracks = [
        track_id
        for _, track_list in handle_genre[target_user].items()
        for track_id in track_list
    ]
    return set(target_user_tracks)


def _filter_known_genre_tracks(
        know_genre: Dict[str, Dict[str, float]],
        target_user_tracks_set: set,
        default_score: Optional[float] = None,
) -> None:
    """
    Фильтрует треки из know_genre, исключая треки, уже известные пользователю.
    При переданном default_score допускает только треки с оценкой >= default_score.

    Args:
        know_genre: рекомендации по жанрам (модифицируется)
        target_user_tracks_set: множество треков пользователя
        default_score: необязательный порог для допуска трека

    Modifies:
        know_genre: удаляет или оставляет треки по условию
    """
    for genre_name, tracks in know_genre.items():
        clear_tracks_list: Dict[str, float] = {}
        for track_id, score in tracks.items():
            if track_id in target_user_tracks_set:
                continue

            if default_score is not None:
                if score >= default_score:
                    clear_tracks_list[track_id] = score
            else:
                clear_tracks_list[track_id] = score

        know_genre[genre_name] = clear_tracks_list


def _sort_and_limit_known_genre(
        know_genre: Dict[str, Dict[str, float]],
        top_n: int = 5,
) -> Dict[str, Dict[str, float]]:
    """
    Сортирует треки по оценке для каждого жанра и возвращает топ N.

    Args:
        know_genre: словарь жанров с треками и их оценками
        top_n: максимальное количество треков на жанр

    Returns:
        Отсортированный словарь с топ N треками на жанр
    """
    know_genre_sort: Dict[str, Dict[str, float]] = {}
    for genre, tracks in know_genre.items():
        know_genre_sort[genre] = dict(
            sorted(tracks.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
        )
    return know_genre_sort


def get_top_track_know_genre_recommendations(
        target_user: str,
        handle_genre: Dict[str, Dict[str, List[str]]],
        top_k_neighbors: List[Tuple[int, float]],
        idx_to_user: Dict[int, str],
        default_score: Optional[int] = None,
        top_n: int = 5,
) -> Dict[str, Dict[str, float]]:
    """
    Основная функция для рекомендаций знакомых жанров и треков для target_user.

    Аргументы:
    - target_user — имя пользователя
    - handle_genre — структура жанр/треков у пользователей
    - top_k_neighbors — список ближайших соседей с их весами
    - idx_to_user — отображение индексов на имена пользователей
    - default_score — (опционально) порог по score для фильтрации треков
    - top_n — сколько треков на жанр оставить

    Возвращает:
    Словарь в формате {жанр: {track_id: score}} с топ-N треками на жанр.

    Логика:
    1. Инициализирует словарь known_genre жанров пользователя target_user.
    2. Агрегирует треки соседей с суммированием баллов.
    3. Убирает треки, уже прослушанные target_user, и применяет порог score.
    4. Сортирует и возвращает топ-N треков на жанр.
    """
    know_genre = _init_known_genres(target_user, handle_genre)

    _aggregate_neighbor_tracks(
        know_genre=know_genre,
        handle_genre=handle_genre,
        top_k_neighbors=top_k_neighbors,
        idx_to_user=idx_to_user,
    )

    target_user_tracks_set = _get_target_user_track_set(target_user, handle_genre)

    _filter_known_genre_tracks(
        know_genre=know_genre,
        target_user_tracks_set=target_user_tracks_set,
        default_score=default_score,
    )

    know_genre_sort = _sort_and_limit_known_genre(
        know_genre=know_genre,
        top_n=top_n,
    )

    return know_genre_sort
