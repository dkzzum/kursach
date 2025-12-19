import json

from src.secret.config import headers_kp_graphql, cookies_kp
import requests

s = requests.Session()
s.headers.update(headers_kp_graphql)
s.cookies.update(cookies_kp)

payload = {
    "operationName": "UserReactionMovies",
    "query": "query UserReactionMovies($isAuthorized: Boolean!, $socialAlias: String!, $includeTypes: [ReactionType!]!, $limit: Int!, $offset: Int!) { userProfileBySocialAlias(socialAlias: {socialAlias: $socialAlias}) { ...SocialUserProfileId userData { movieReactions(limit: $limit, offset: $offset, includeTypes: $includeTypes) { items { ...UserReaction __typename } total limit offset __typename } __typename } __typename } } fragment RatingValue on RatingValue { value isActive count __typename } fragment MovieForPoster on Movie { id title { russian original __typename } poster { avatarsUrl __typename } genres { id name __typename } rating { kinopoisk { ...RatingValue __typename } __typename } userData @include(if: $isAuthorized) { watchStatuses { watched { value __typename } __typename } __typename } viewOption { buttonText isAvailableOnline: isWatchable(filter: {anyDevice: false, anyRegion: false}) purchasabilityStatus contentPackageToBuy { billingFeatureName __typename } subscriptionBadge { image { avatarsUrl __typename } __typename } type posterWithRightholderLogo __typename } ... on Film { productionYear __typename } ... on Video { productionYear __typename } ... on TvSeries { releaseYears { start end __typename } __typename } ... on TvShow { releaseYears { start end __typename } __typename } ... on MiniSeries { releaseYears { start end __typename } __typename } __typename } fragment SocialUserProfileId on UserProfileInterface { id { kpId ottId puid __typename } __typename } fragment UserReaction on UserMovieReactions { movie { ...MovieForPoster contentId __typename } reactions(includeTypes: $includeTypes) { __typename ... on Watched { watched __typename } ... on Vote { value __typename } ... on PlannedToWatch { plannedToWatch __typename } } __typename } ",  # твой query
    "variables": {
        "includeTypes": ["VOTE", "WATCHED"],
        "isAuthorized": True,
        "limit": 10,
        "offset": 0,
        "socialAlias": '130901766'
    }
}

r = s.post("https://graphql.kinopoisk.ru/graphql/?operationName=UserReactionMovies", json=payload)
data = json.loads(r.text)

with open('json_requests/user_reaction_movies.json', '+w', encoding="utf-8") as f:
    json.dump(data, f, indent=4)
