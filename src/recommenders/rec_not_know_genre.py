from src.recommenders.create_matrix_genre import handle_data, create_matrix
import numpy as np


def compute_cosine_similarity(matrix: np.ndarray, target_idx: int) -> np.ndarray:
    """
    Вычисляет косинусное сходство всех пользователей с целевым пользователем.

    Args:
        matrix: Матрица пользователь-жанр (строки - пользователи, столбцы - жанры).
        target_idx: Индекс целевого пользователя в матрице.

    Returns:
        Массив косинусных сходств для всех пользователей (исключая целевого).

    Raises:
        ValueError: Если целевой пользователь не слушает ни один жанр.
    """
    target_vector = matrix[target_idx, :]
    target_norm = np.linalg.norm(target_vector)

    if target_norm == 0:
        raise ValueError('Целевой пользователь не слушает ни один жанр!')

    cos_similarity = []
    for user_vector in matrix:
        norm_user = np.linalg.norm(user_vector)
        if norm_user == 0:
            cos_similarity.append(0)
        else:
            sim = np.dot(user_vector, target_vector) / (norm_user * target_norm)
            cos_similarity.append(sim)

    cos_similarity_array = np.array(cos_similarity)
    cos_similarity_array[target_idx] = np.float64(0)
    return cos_similarity_array


def filter_similar_neighbors(
        similarity_array: np.ndarray,
        matrix: np.ndarray,
        target_vector: np.ndarray,
        threshold: float = 0.3,
        min_common: int = 20,
        k: int = 10
) -> list[tuple[int, float]]:
    """
    Фильтрует и сортирует топ-K наиболее похожих пользователей.

    Args:
        similarity_array: Массив косинусных сходств всех пользователей.
        matrix: Матрица пользователь-жанр.
        target_vector: Вектор жанров целевого пользователя.
        threshold: Минимальное косинусное сходство (по умолчанию 0.3).
        min_common: Минимальное количество общих жанров (по умолчанию 20).
        k: Количество топ-соседей для возврата (по умолчанию 10).

    Returns:
        Список кортежей (индекс_пользователя, сходство) для топ-K соседей.
    """
    mask_similarity = similarity_array >= threshold
    common_genres = np.sum((matrix > 0) & (target_vector > 0), axis=1)
    mask_common = common_genres > min_common

    valid_mask = mask_similarity & mask_common
    valid_indices = np.where(valid_mask)[0]
    valid_similarities = similarity_array[valid_indices]

    valid_sort_idx = np.argsort(-valid_similarities)[:k]
    return [(valid_indices[idx], valid_similarities[idx]) for idx in valid_sort_idx]


def aggregate_candidates(
        top_k_neighbors: list[tuple[int, float]],
        matrix: np.ndarray,
        target_genres: np.ndarray
) -> dict[int, float]:
    """
    Агрегирует жанры соседей, исключая уже известные целевому пользователю.

    Args:
        top_k_neighbors: Список топ-K соседей (индекс, сходство).
        matrix: Матрица пользователь-жанр.
        target_genres: Индексы жанров целевого пользователя.

    Returns:
        Словарь {индекс_жанра: суммарный_score_по_сходствам_соседей}.
    """
    top_k_indices = [idx for idx, _ in top_k_neighbors]
    top_k_neighbors_matrix = matrix[top_k_indices]

    neighbor_genres = [np.where(genres > 0)[0] for genres in top_k_neighbors_matrix]
    all_neighbor_genres = np.hstack(neighbor_genres)
    candidates = np.unique(np.setdiff1d(all_neighbor_genres, target_genres))

    candidates_score = {}
    for idx, user_genres in enumerate(neighbor_genres):
        similarity = top_k_neighbors[idx][1]
        for genre_idx in user_genres:
            if genre_idx in candidates:
                candidates_score[genre_idx] = candidates_score.get(genre_idx, 0) + similarity

    return candidates_score


def get_top_genre_recommendations(
        candidates_score: dict[int, float],
        top_k_similarities: list[float],
        genre_to_idx: dict,
        N: int = 10
) -> list[tuple[str, float]]:
    """
    Нормализует scores и возвращает топ-N жанров с названиями.

    Args:
        candidates_score: Словарь сырых scores кандидатов-жанров.
        top_k_similarities: Список сходств топ-K соседей.
        genre_to_idx: Словарь {название_жанра: индекс}.
        N: Количество топ-рекомендаций (по умолчанию 10).

    Returns:
        Список кортежей (название_жанра, нормализованный_score).
    """
    sum_score = sum(sim for _, sim in top_k_similarities)
    normalized_scores = {}
    for genre_idx, score in candidates_score.items():
        normalized_scores[genre_idx] = score / sum_score

    idx_to_genre = {v: k for k, v in genre_to_idx.items()}

    top_genres = []
    for genre_idx, score in sorted(normalized_scores.items(), key=lambda x: x[1], reverse=True)[:N]:
        genre_name = idx_to_genre[genre_idx]
        top_genres.append((genre_name, score))

    return top_genres

