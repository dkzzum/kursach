from src.config import headers_kp, cookies_kp
import requests
import json

s = requests.Session()
s.headers.update(headers_kp)
s.cookies.update(cookies_kp)

# user_id = '1537003491'
user_id = '98494675'
payload = {
  "0": {
    "operationName": "UserInfo",
    "query": "query UserInfo {\n  viewer {\n    id\n    firstName\n    lastName\n    displayName\n    login\n    avatarId\n    havePlus\n    isChild\n    birthdate\n    __typename\n  }\n  account {\n    id\n    phones {\n      id\n      isPrimary\n      number\n      __typename\n    }\n    __typename\n  }\n}",
    "variables": {}
  },
  "1": {
    "operationName": "GetPlusBalanceWidgetData",
    "query": "query GetPlusBalanceWidgetData {\n  subscription(input: PLUS) {\n    type: __typename\n    isActive\n    ... on SubscriptionPlus {\n      balance\n      __typename\n    }\n  }\n}",
    "variables": {}
  },
  "2": {
    "operationName": "GetSplitWidgetData",
    "query": "query GetSplitWidgetData {\n  splitActive {\n    isActive\n    __typename\n  }\n  splitScoring {\n    isAgreementAccepted\n    __typename\n  }\n  splitPayment {\n    id\n    overdue {\n      amount\n      days\n      __typename\n    }\n    nextPaymentAt\n    amount\n    __typename\n  }\n}",
    "variables": {}
  },
  "3": {
    "operationName": "GetFamilyCardWidgetData",
    "query": "query GetFamilyCardWidgetData {\n  family {\n    id\n    currentMember {\n      id\n      isAdmin\n      pay {\n        id\n        status\n        balance\n        limit {\n          value\n          limitMode\n          __typename\n        }\n        limitCurrency\n        unlim\n        __typename\n      }\n      __typename\n    }\n    pay {\n      id\n      enabled\n      cardInfo {\n        id\n        maskedNumber\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  account {\n    id\n    phones {\n      id\n      isPrimary\n      __typename\n    }\n    __typename\n  }\n}",
    "variables": {}
  },
  "4": {
    "operationName": "GetBankCardWidgetData",
    "query": "query GetBankCardWidgetData {\n  accountBankCards {\n    id\n    isFamilyCard\n    __typename\n  }\n  sbpTokens {\n    id\n    __typename\n  }\n}",
    "variables": {}
  },
  "5": {
    "operationName": "GetDocumentSectionList",
    "query": "query GetDocumentSectionList {\n  documents {\n    id\n    type: __typename\n  }\n}",
    "variables": {}
  },
  "6": {
    "operationName": "GetAddressSectionList",
    "query": "query GetAddressSectionList {\n  addressList {\n    __typename\n    id\n    label\n    tags\n    type: __typename\n    previewDarkImageUrl: previewImageUrl(\n      input: {colorScheme: DARK, width: 736, height: 160}\n    )\n    previewLightImageUrl: previewImageUrl(\n      input: {colorScheme: LIGHT, width: 736, height: 160}\n    )\n  }\n}",
    "variables": {}
  },
  "7": {
    "operationName": "GetInviteWidgetData",
    "query": "query GetInviteWidgetData {\n  account {\n    id\n    type\n    __typename\n  }\n  family {\n    id\n    currentMember {\n      id\n      isAdmin\n      roles\n      __typename\n    }\n    invites {\n      id\n      __typename\n    }\n    settings {\n      maxCapacity\n      __typename\n    }\n    members {\n      id\n      __typename\n    }\n    __typename\n  }\n}",
    "variables": {}
  },
  "8": {
    "operationName": "GetChildrenWidgetData",
    "query": "query GetChildrenWidgetData {\n  family {\n    id\n    members {\n      id\n      isChild\n      __typename\n    }\n    __typename\n  }\n}",
    "variables": {}
  },
  "9": {
    "operationName": "GetPetsWidgetData",
    "query": "query GetPetsWidgetData {\n  pets {\n    id\n    __typename\n  }\n}",
    "variables": {}
  },
  "10": {
    "operationName": "GetFamilyPendingInviteList",
    "query": "query GetFamilyPendingInviteList {\n  family {\n    id\n    invites {\n      id\n      contact\n      __typename\n    }\n    __typename\n  }\n}",
    "variables": {}
  },
  "11": {
    "operationName": "GetPetList",
    "query": "query GetPetList {\n  pets {\n    ...PetFields\n    __typename\n  }\n}\n\nfragment PetFields on Pet {\n  __typename\n  id\n  activityType\n  ageType\n  avatarUrl\n  birthDate {\n    __typename\n    date\n    month\n    year\n  }\n  bodyType\n  breed {\n    id\n    name\n    __typename\n  }\n  features\n  feedTypes\n  gender\n  isCastrated\n  isBreedUnknown\n  name\n  type\n  customType\n}",
    "variables": {}
  },
  "12": {
    "operationName": "GetFamilySectionData",
    "query": "query GetFamilySectionData {\n  family {\n    id\n    members {\n      ...FamilyMemberForFamilySectionFields\n      __typename\n    }\n    invites {\n      ...FamilyInviteForFamilySectionFields\n      __typename\n    }\n    __typename\n  }\n  viewer {\n    id\n    avatarId\n    havePlus\n    displayName\n    isChild\n    __typename\n  }\n  pets {\n    ...PetForFamilySectionFields\n    __typename\n  }\n}\n\nfragment FamilyMemberForFamilySectionFields on FamilyMember {\n  __typename\n  id\n  name\n  avatar\n  hasPlus\n  isAdmin\n  isChild\n  pay {\n    id\n    status\n    balance\n    unlim\n    limit {\n      value\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment FamilyInviteForFamilySectionFields on FamilyInvite {\n  __typename\n  id\n  contact\n}\n\nfragment PetForFamilySectionFields on Pet {\n  __typename\n  id\n  type\n  name\n  avatarUrl\n}",
    "variables": {}
  }
}

payload = list(payload.values())
r1 = s.post(f"https://id.yandex.ru/front-api/graphql", json=payload, timeout=10)

print(json.dumps(json.loads(r1.text), indent=4))
