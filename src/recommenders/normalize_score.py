import json
from copy import deepcopy
from typing import Dict, List


def _get_list_score(json: Dict[str, Dict[str, float]]) -> List[float]:
    tracks_list = []

    for _, tracks_dict in json.items():
        for _, score in tracks_dict.items():
            tracks_list.append(score)

    return tracks_list


def _get_weight_factor(
        not_know_tracks_recommendations: Dict[str, Dict[str, float]],
        know_track_recommendations: Dict[str, Dict[str, float]]
):
    not_t = _get_list_score(not_know_tracks_recommendations)
    know_t = _get_list_score(know_track_recommendations)

    len_nt = len(not_t)
    len_kt = len(know_t)
    if len_nt == 0 or len_kt == 0:
        raise ValueError("Деление на ноль")

    not_t_avg: float = sum(not_t) / len_nt
    know_t_avg: float = sum(know_t) / len_kt

    weight_factor: float = know_t_avg / not_t_avg
    return weight_factor


def _union_dict(tracks_rec: Dict, json: Dict[str, Dict[str, float]]) -> None:
    for genre_name, tracks in json.items():
        if tracks:
            tracks_rec[genre_name] = tracks.copy()


def normalize_score(
        not_know_tracks_recommendations: Dict[str, Dict[str, float]],
        know_track_recommendations: Dict[str, Dict[str, float]]
) -> Dict[str, Dict[str, float]]:
    weight_factor = _get_weight_factor(not_know_tracks_recommendations, know_track_recommendations)

    track_rec_norm = deepcopy(know_track_recommendations)
    for genre_name, tracks_dict in track_rec_norm.items():
        for track_id, score in tracks_dict.items():
            track_rec_norm[genre_name][track_id] *= weight_factor

    tracks_rec = {}
    _union_dict(tracks_rec, not_know_tracks_recommendations)
    _union_dict(tracks_rec, track_rec_norm)

    max_score = max([score for _, tracks_dict in tracks_rec.items() for _, score in tracks_dict.items()])
    if max_score == 0:
        raise ValueError("Деление на ноль!")

    for genre_name, tracks_dict in tracks_rec.items():
        for track_id, score in tracks_dict.items():
            tracks_rec[genre_name][track_id] /= max_score

    # print(json.dumps(tracks_rec, indent=4))
    return tracks_rec
