import CrawlModulesPG.const as const

VK_ACCOUNT = [
    #--0--
    { 'login': '89253701963',
      'password': 'Ubn_Ubn_2020_5$%$#',
      'app_id': '7700818'
     }
    ]


TG_ACCOUNT = [
    #--0--
    { 'api_id': '3756968',
      'api_hash': '6f685fcaf0056c27f8b5d77ef1265f04',
      'username': 'tg_api_key',
      'phone': '+79253701963'
     }
    ]


if const.PY_ENVIRONMENT:
    import CrawlModulesPyOnly.self_psw as self_psw

    VK_ACCOUNT[0] = { 'login': '89273824101',
                      'password': self_psw.get_psw_vk_mtyurin(),
                      'app_id': '7467601'
                     }
    TG_ACCOUNT[0] = { 'api_id': '2389423',
                      'api_hash': self_psw.get_api_hash_for_telegram(),
                      'username': 'Cassandra',
                      'phone': '+79273824101'
     }


#первый запрос токена https://oauth.vk.com/authorize?client_id=7700818&display=page&redirect_uri=https://oauth.vk.com/blank.html&scope=wall&response_type=token

#cоздание своего клиента Telegram https://tlgrm.ru/docs/api/obtaining_api_id