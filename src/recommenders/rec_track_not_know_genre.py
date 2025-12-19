import json
from typing import Dict


def _build_reverse_mapping(user_to_idx: Dict[str, int]) -> Dict[int, str]:
    """
    Инвертирует маппинг пользователь -> индекс в индекс -> пользователь.

    Args:
        user_to_idx: словарь вида {"user1": 0, "user2": 1, ...}

    Returns:
        Инвертированный словарь вида {0: "user1", 1: "user2", ...}
    """
    return {v: k for k, v in user_to_idx.items()}


def _aggregate_track_scores(
        recommendations: list[tuple[str, float]],
        top_k_neighbors: list[tuple[int, float]],
        handle_genre: Dict[str, Dict[str, list[str]]],
        idx_to_user: Dict[int, str]
) -> Dict[str, Dict[str, float]]:
    """
    Агрегирует scores треков от всех соседей по жанрам.

    Для каждого соседа собирает его треки из рекомендуемых жанров
    и суммирует их scores на основе similarity_score соседа.

    Args:
        recommendations: список кортежей (жанр, начальный_score)
        top_k_neighbors: список кортежей (индекс_соседа, similarity_score)
        handle_genre: словарь вида {username: {жанр: [track_ids]}}
        idx_to_user: маппинг индекса на username

    Returns:
        Словарь вида {жанр: {track_id: агрегированный_score}}
    """
    track_recommendations = {k[0]: {} for k in recommendations}

    for neighbor in top_k_neighbors:
        idx = neighbor[0]
        score = neighbor[1]

        for genre in track_recommendations:
            user_genres = handle_genre[idx_to_user[idx]]
            if genre in user_genres:
                for track_id in user_genres[genre]:
                    track_recommendations[genre][track_id] = track_recommendations[genre].get(track_id, 0) + score

    return track_recommendations


def _sort_and_limit_tracks(
        track_recommendations: Dict[str, Dict[str, float]],
        N: int = 5
) -> Dict[str, Dict[str, float]]:
    """
    Сортирует треки по score (убывающий порядок) и оставляет топ N.

    Args:
        track_recommendations: словарь вида {жанр: {track_id: score}}
        N: количество лучших треков для каждого жанра (по умолчанию 5)

    Returns:
        Отсортированный словарь с ограничением N треков на жанр
    """
    track_recommendations_sort = {}
    for genre, tracks in track_recommendations.items():
        track_recommendations_sort[genre] = dict(
            sorted(tracks.items(), key=lambda kv: kv[1], reverse=True)[:N]
        )

    return track_recommendations_sort


def get_top_track_not_know_genre_recommendations(
        handle_genre: Dict[str, Dict[str, list[str]]],
        user_to_idx: Dict[str, int],
        top_k_neighbors: list[tuple[int, float]],
        recommendations: list[tuple[str, float]],
) -> (Dict[str, Dict[str, float]], Dict[int, str]):
    """
    Основная функция для получения рекомендаций треков на основе соседей.

    Процесс:
    1. Инвертирует маппинг user_to_idx для быстрого поиска username по индексу
    2. Агрегирует scores треков от k-ближайших соседей
    3. Сортирует треки и оставляет топ 5 для каждого жанра

    Args:
        handle_genre: словарь пользователей с их жанрами и треками
                     вида {username: {жанр: [track_ids]}}
        user_to_idx: маппинг username -> индекс вида {"user1": 0, ...}
        top_k_neighbors: k-ближайшие соседи с их scores
                        вида [(индекс, similarity_score), ...]
        recommendations: список рекомендуемых жанров с весами
                        вида [(жанр, вес), ...]

    Returns:
        Рекомендации вида {жанр: {track_id: score}} с топ 5 треками на жанр
    """
    idx_to_user = _build_reverse_mapping(user_to_idx)
    track_recommendations = _aggregate_track_scores(
        recommendations, top_k_neighbors, handle_genre, idx_to_user
    )
    limit_track = _sort_and_limit_tracks(track_recommendations)
    return limit_track, idx_to_user
