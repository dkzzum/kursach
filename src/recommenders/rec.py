import json

import numpy as np

from src.recommenders.create_matrix_genre import handle_data, create_matrix
from src.recommenders.rec_not_know_genre import compute_cosine_similarity, filter_similar_neighbors, aggregate_candidates, \
    get_top_genre_recommendations

from src.recommenders.normalize_score import normalize_score
from src.recommenders.rec_track_not_know_genre import get_top_track_not_know_genre_recommendations
from src.recommenders.rec_track_know_genre import get_top_track_know_genre_recommendations
from src.recommenders.get_tracks import write_rec_info


def get_rec(target_users: str) -> list[tuple[str, float]]:
    """
    Главная функция рекомендаций жанров для целевого пользователя.

    Args:
        target_users: ID целевого пользователя (строка).

    Returns:
        Список топ-N рекомендованных жанров с нормализованными scores.
    """

    handle_genre, users_list, target_user_idx, user_to_idx, genre_to_idx = handle_data(target_users)
    matrix = create_matrix(handle_genre, user_to_idx, genre_to_idx)

    target_vector = matrix[target_user_idx, :]
    target_genres = np.where(target_vector > 0)[0]

    similarities = compute_cosine_similarity(matrix, target_user_idx)
    top_k_neighbors = filter_similar_neighbors(similarities, matrix, target_vector)

    candidates_score = aggregate_candidates(top_k_neighbors, matrix, target_genres)
    genre_recommendations = get_top_genre_recommendations(
        candidates_score, top_k_neighbors, genre_to_idx
    )
    not_know_tracks_recommendations, idx_to_user = get_top_track_not_know_genre_recommendations(handle_genre, user_to_idx,
                                                                          top_k_neighbors, genre_recommendations)

    know_track_recommendations = get_top_track_know_genre_recommendations(target_users,
                                             handle_genre, top_k_neighbors, idx_to_user, 2.5)

    # for genre_name, tracks in know_track_recommendations.items():
    #     # print(len(tracks))
    #     if tracks:
    #         tracks_recommendations[genre_name] = tracks

    norm_recommendations = normalize_score(not_know_tracks_recommendations, know_track_recommendations)

    finale_rec_data = write_rec_info(norm_recommendations, target_user)
    return finale_rec_data


if __name__ == '__main__':
    target_user = '1537003491'
    recs = get_rec(target_user)

    # with open(f"../data/recommendations/rec_for_{target_user}.json", "w", encoding="utf-8") as file:
    #     json.dump(recs, file, ensure_ascii=False, indent=4)

    # print(json.dumps(recs, indent=4))
