from src.secret.config import headers_kp, cookies_kp
import requests

s = requests.Session()
headers_kp['Accept'] = '/'

cookies = {
    "L": "ZglGU11fAU5AVlpNCFBxbGNDdQ1oYEZ+Ij88Nz8g.1755465239.16251.382117.f00be40ab94bf1da0e8522cf41fc49b8",
    "PHPSESSID": "5fd3307ae85e789137c5120929713a33",
    "_yasc": "1qZ+Ovyhev33I0inIZx7JNONlzoeJHM7SRu1Uf88jlcZido1og1WorjUfWfBFqQ=",
    "_ym_d": "1762275528",
    "_ym_isad": "1",
    "_ym_uid": "1745768212403884557",
    "_ym_visorc": "b",
    "bh": "ElAiQ2hyb21pdW0iO3Y9IjEzNCIsICJOb3Q6QS1CcmFuZCI7dj0iMjQiLCAiWWFCcm93c2VyIjt2PSIyNS40IiwgIllvd3NlciI7dj0iMi41IhoFImFybSIqAj8wOgcibWFjT1MiQggiMTUuNS4wIkoEIjY0IlJpIkNocm9taXVtIjt2PSIxMzQuMC42OTk4LjExNTciLCAiTm90OkEtQnJhbmQiO3Y9IjI0LjAuMC4wIiwgIllhQnJvd3NlciI7dj0iMjUuNC4xLjExNTciLCAiWW93c2VyIjt2PSIyLjUiWgI/MGDZjK/CBmoj3MrRtgG78Z+rBPrWhswI0tHt6wP8ua//B9/9n44I4MXNhwg=",
    "cmtchd": "MTc2MTg5NjU1MDAyOQ==",
    "coockoos": "1",
    "crookie": "0Ho3yoqAosgPNbykFf93hn1C4xQcieOKDH/uss6Wm2xlKA/NhlrNedO1gOe8sPiv2U9rAcdt6OKPvSNdScggVELArJQ=",
    "gdpr": "0",
    "hideBlocks": "135168",
    "i": "0frf2dzqqYsFJN9vS67FG5oRV5Rf7mgXEUj4kK1PC7kjry6mwDDqOEtHwDjqsdpbHQfkdHUNy9uFAH7nBkiOBg01FaM=",
    "location": "1",
    "mda2_beacon": "1762275515199",
    "mda_exp_enabled": "1",
    "mobile": "no",
    "mustsee_sort_v5": "01.10.200.21.31.41.121.131.51.61.71.81.91.101.111",
    "my_perpages": "%7B%2277%22%3A200%7D",
    "no-re-reg-required": "1",
    "sessar": "1.1299404.CiD-NsQK6mY5I01xwVMxoWm04GOurlEKK7z5mt3omy4gbw.Q7_57m3MvOYHw150bPj02IME5k3JE8Ekab5dSY5hcI0",
    "sso_status": "sso.passport.yandex.ru:synchronized",
    "uid": "98494675",
    "vote_data_cookie": "0213fd1658580dcd97dd1c24a635b374",
    "ya_sess_id": "3:1762275515.5.1.1745767302917:TvDruQ:4f82.1.2:1|1442696724.-1.2.3:1745767302|1537003491.65.2.2:65.3:1745767367|2167429108.3265767.2.2:3265767.3:1749033069|30:11356149.5863.6_q59LAjH_RAihWNmLzAaSH1V-0",
    "yandex_login": "dkzzum",
    "yandexuid": "6487430611745834695",
    "yashr": "9615164371745768210",
    "ymex": "1752388441.oyu.6487430611745834695",
    "yp": "1749882841.yu.6487430611745834695",
    "ys": "udn.cDrQlNCw0L3QuNC7#c_chck.684135079",
    "yuidss": "6487430611745834695"
}

headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "ru,en;q=0.9",
    "content-length": "1937",
    "content-type": "application/json",
    "origin": "https://www.kinopoisk.ru",
    "priority": "u=1, i",
    "referer": "https://www.kinopoisk.ru/",
    "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"YaBrowser\";v=\"25.8\", \"Yowser\";v=\"2.5\"",
    "sec-ch-ua-arch": "\"arm\"",
    "sec-ch-ua-bitness": "\"64\"",
    "sec-ch-ua-full-version-list": "\"Not)A;Brand\";v=\"8.0.0.0\", \"Chromium\";v=\"138.0.7204.977\", \"YaBrowser\";v=\"25.8.5.977\", \"Yowser\";v=\"2.5\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"macOS\"",
    "sec-ch-ua-platform-version": "\"26.0.1\"",
    "sec-ch-ua-wow64": "?0",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "service-id": "25",
    "traceparent": "00-f81ebeadb4fb7fe6e1a212f20ec14c30-e535a14467e1ba8d-01",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 YaBrowser/25.8.0.0 Safari/537.36",
    "x-preferred-language": "ru",
    "x-request-id": "1762275528784620-4790592419421422185:4"
}

s.headers.update(headers)
s.cookies.update(cookies)

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
print(r.status_code)
print(r.text)