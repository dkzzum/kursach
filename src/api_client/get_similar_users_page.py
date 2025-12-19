from src.core.config import headers_kp_graphql, cookies_kp, headers_kp
from get_playlists_user import get_playlists_user
from typing import List, Dict, Any
from bs4 import BeautifulSoup
import requests


def ger_friend_page(s: requests.Session, user_id: str, perpage: int | str) -> str:
    """
    Запрашивает HTML-страницу друзей пользователя Kinopoisk по user_id.

    :param s: Активная requests Session с cookies и заголовками
    :param user_id: ID пользователя Кинопоиска
    :param perpage: количество отображаемых друзей на странице
    :return: HTML страницы в виде строки
    """
    try:
        response = s.get(
            f"https://www.kinopoisk.ru/community/cf_user/{user_id}/perpage/{perpage}",
            timeout=10
        )
        if response.status_code != 200:
            raise RuntimeError("Ошибка при загрузке html страницы друзей")
        return response.text
    except Exception as e:
        raise RuntimeError(f"Ошибка при получении страницы друзей: {e}")


def handlers_friends_response(s: requests.Session, user_id: str, perpage: int | str) -> Dict[str, str]:
    """
    Находит на странице всех друзей и извлекает их socialAlias (kpid).

    :return: Список строк — списки ID друзей (kpid)
    """
    response = ger_friend_page(s, user_id, perpage)

    soup = BeautifulSoup(response, "html.parser")
    users_div = soup.find_all('p', 'profile_name')
    users_continue = soup.find_all('a', 'continue')
    users_a = [user.find('a') for user in users_div]
    users_p = [user.text for user in users_continue]

    users_id_list: List[str] = [
        user['href'].split('/')[-2]
        for user in users_a if user and user.get('href')
    ]

    friends_with_ratings: Dict[str, str] = {}
    for kpid, rating in zip(users_id_list, users_p):
        friends_with_ratings[kpid] = rating

    return friends_with_ratings


def get_uid(s: requests.Session, friends_kpid: str) -> Dict[str, Any]:
    """
    Получает данные GraphQL по socialAlias (kpid) одного пользователя:
    содержит данные профиля + список фильмов, где есть оценка или факт просмотра.

    :param s: Активная requests.Session с cookies и заголовками
    :param friends_kpid: социальный ID пользователя Кинопоиска (kpid)
    :return: JSON-объект (словарь) с данными профиля
    """
    try:
        response = s.post(
            "https://graphql.kinopoisk.ru/graphql/?operationName=UserReactionMovies",
            json={
                "operationName": "UserReactionMovies",
                "query": "query UserReactionMovies($isAuthorized: Boolean!, $socialAlias: String!, $includeTypes: "
                         "[ReactionType!]!, $limit: Int!, $offset: Int!) { userProfileBySocialAlias(socialAlias: "
                         "{socialAlias: $socialAlias}) { ...SocialUserProfileId userData { movieReactions(limit: "
                         "$limit, offset: $offset, includeTypes: $includeTypes) { items { ...UserReaction __typename } "
                         "total limit offset __typename } __typename } __typename } } fragment RatingValue on "
                         "RatingValue { value isActive count __typename } fragment MovieForPoster on Movie { id title "
                         "{ russian original __typename } poster { avatarsUrl __typename } genres { id name __typename "
                         "} rating { kinopoisk { ...RatingValue __typename } __typename } userData @include(if: "
                         "$isAuthorized) { watchStatuses { watched { value __typename } __typename } __typename } "
                         "viewOption { buttonText isAvailableOnline: isWatchable(filter: {anyDevice: false, anyRegion: "
                         "false}) purchasabilityStatus contentPackageToBuy { billingFeatureName __typename } "
                         "subscriptionBadge { image { avatarsUrl __typename } __typename } type "
                         "posterWithRightholderLogo __typename } ... on Film { productionYear __typename } ... on Video"
                         " { productionYear __typename } ... on TvSeries { releaseYears { start end __typename }"
                         " __typename } ... on TvShow { releaseYears { start end __typename } __typename } ... on "
                         "MiniSeries { releaseYears { start end __typename } __typename } __typename } fragment "
                         "SocialUserProfileId on UserProfileInterface { id { kpId ottId puid __typename } __typename }"
                         " fragment UserReaction on UserMovieReactions { movie { ...MovieForPoster contentId __typename"
                         " } reactions(includeTypes: $includeTypes) { __typename ... on Watched { watched __typename }"
                         " ... on Vote { value __typename } ... on PlannedToWatch { plannedToWatch __typename } } "
                         "__typename } ",
                "variables": {
                    "socialAlias": friends_kpid,
                    "offset": 0,
                    "limit": 10,
                    "isAuthorized": True,
                    "includeTypes": ["VOTE", "WATCHED"]
                }
            },
            timeout=10
        )
        if response.status_code != 200:
            print(response.text)
            return None

        return response.json()
    except Exception as e:
        print(f"Ошибка при загрузке информации об оценках пользователя: {e}")
        # raise RuntimeError(f"Ошибка при загрузке информации об оценках пользователя: {e}")


def main() -> None:
    s = requests.Session()
    s.headers.update(headers_kp)
    s.cookies.update(cookies_kp)

    friends_with_ratings = handlers_friends_response(s, "98494675", "200")
    print(f"Получены все kpid пользователей! Количество: {len(friends_with_ratings)}")

    friends_uid: List[Dict[str, str]] = []

    print("Обновляем headers для GraphQL")
    s.headers.update(headers_kp_graphql)
    total = len(friends_with_ratings)
    for idx, (kpid, rating) in enumerate(friends_with_ratings.items(), 1):
        remaining = total - idx

        friend_reactions = get_uid(s=s, friends_kpid=kpid)
        if friend_reactions is None:
            continue

        print(f"Осталось: {remaining}")

        friend_puid = friend_reactions["data"]["userProfileBySocialAlias"]["id"]["puid"]

        friend_data = {
            'puid': friend_puid,
            'rating': rating
        }
        friends_uid.append(friend_data)

        # Получаем плейлисты пользователя
        get_playlists_user(friend_puid, rating)


# print(friends_uid)


if __name__ == "__main__":
    main()
