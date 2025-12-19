import requests
import json

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://music.yandex.ru/",
    "Connection": "keep-alive",
}

cookies = {
    "L": "ZglGU11fAU5AVlpNCFBxbGNDdQ1oYEZ+Ij88Nz8g.1755465239.16251.382117.f00be40ab94bf1da0e8522cf41fc49b8",
    "Session_id": "3:1761680596.5.1.1745767302917:TvDruQ:4f82.1.2:1|1442696724.-1.2.3:1745767302|1537003491.65.2.2:65.3:1745767367|2167429108.3265767.2.2:3265767.3:1749033069|3:11331724.585418.wdFAu-knHg5zwMmAzFnpTFS_QnI",
    "_yasc": "WoTWXaJDkbuhe8nzvWvdJEgIX5mWuCVpBRhxloS/IiqtFiV/CQUP42kPCfayhgli5pn0IqoWi/dsH7h6QSNAx/9xWrI=",
    "_ym_d": "1761549984",
    "_ym_isad": "2",
    "_ym_uid": "1745767251811010481",
    "_ym_visorc": "b",
    "alice_uuid": "6d897161-a274-40f9-b754-33448CA5734E",
    "amcuid": "4703738411746650970",
    "bh": "Ek8iTm90KUE7QnJhbmQiO3Y9IjgiLCAiQ2hyb21pdW0iO3Y9IjEzOCIsICJZYUJyb3dzZXIiO3Y9IjI1LjgiLCAiWW93c2VyIjt2PSIyLjUiGgUiYXJtIiIKMjUuOC41Ljg5MioCPzAyAiIiOgcibWFjT1MiQggiMjYuMC4xIkoEIjY0IlJmIk5vdClBO0JyYW5kIjt2PSI4LjAuMC4wIiwgIkNocm9taXVtIjt2PSIxMzguMC43MjA0Ljg5MiIsICJZYUJyb3dzZXIiO3Y9IjI1LjguNS44OTIiLCAiWW93c2VyIjt2PSIyLjUiWgI/MGCD7YnIBmoj3MrRtgG78Z+rBPrWhswI0tHt6wP8ua//B9/9q9AGwo/Nhwg=",
    "csrftoken": "bLnWPmJ0OPe0t7woekZOfVK6zx5MyULkHPnhzP2tarZuT7kaAsavAVb8dCF9II5Z",
    "cycada": "sSh4XJ/3V2cJCj9J7+QgO0CG8WCn7N+hWELZg/YqTvg=",
    "font_loaded": "YSv1",
    "gdpr": "0",
    "i": "0frf2dzqqYsFJN9vS67FG5oRV5Rf7mgXEUj4kK1PC7kjry6mwDDqOEtHwDjqsdpbHQfkdHUNy9uFAH7nBkiOBg01FaM=",
    "is_gdpr": "1",
    "is_gdpr_b": "CLb2ShDv3wIYASgC",
    "isa": "kxFfjnnQxCR2JPi1eMZ086Za6/w1Brk5wiz1jxSx3GHcoeN2UUuuAxFflf0Wyhb6LMt2meP7qGU/mL4Qo3hZxgTuKrY=",
    "maps_session_id": "1761571357904886-14039773390517978868-balancer-l7leveler-kubr-yp-sas-48-BAL",
    "my": "YwA=",
    "sae": "0:6d897161-a274-40f9-b754-33448CA5734E:p:25.8.5.892:m:d:RU:20230811",
    "sessar": "1.1299404.CiCDEj-u-Zen8iOuNHNkLC58MDTYCXNMjlLZafvkHv5-MQ.zs_L82AOaSNXGfsomtz_fPWpPPSE-AWfYmv45GLUffg",
    "sessionid2": "3:1761680596.5.1.1745767302917:TvDruQ:4f82.1.2:1|1442696724.-1.2.3:1745767302|1537003491.65.2.2:65.3:1745767367|2167429108.3265767.2.2:3265767.3:1749033069|3:11331724.585418.fakesign0000000000000000000",
    "skid": "669221501746111799",
    "yabs-vdrf": "CSVze4G3Jfem1Q_zeSG0grHC10",
    "yandex_expboxes": "681842%2C0%2C17%3B1390464%2C0%2C45%3B663872%2C0%2C83%3B1257223%2C0%2C80%3B1389048%2C0%2C34",
    "yandex_login": "dkzzum",
    "yandexuid": "6487430611745834695",
    "yashr": "2352989301745834695",
    "ymex": "1777370703.yrts.1745834703",
    "yp": "1782214704.cld.1955450#1782976305.dc_neuro.10#1763882446.hdrc.1#2077112699.pcs.1#1793288699.swntab.2087866109#1776377218.szm.2%3A1440x900%3A1440x824#2070825239.udn.cDrQlNCw0L3QuNC7#2070825210.multib.1#1764022058.gph.225_127#1763287626.csc.1#1762544596.dlp.2#1761780842.uc.ru#1761780842.duc.ru",
    "ys": "def_bro.1#ead.2FECB7CF#udn.cDrQlNCw0L3QuNC7#wprid.1761752697894751-9630305836422276186-balancer-l7leveler-kubr-yp-sas-132-BAL#c_chck.2220113693",
    "yuidss": "6487430611745834695"
}

s = requests.Session()
s.headers.update(headers)
s.cookies.update(cookies)

params = {
    "trackIds": "30179988"
}


# 1) GET главной страницы (получаем возможные дополнительные cookie/редиректы)
r1 = s.get("https://api.music.yandex.ru/tracks", timeout=10, params=params)
print("main:", r1.status_code)

# print(r1.text)
print(json.dumps(json.loads(r1.text), indent=4))
