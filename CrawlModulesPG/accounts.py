import CrawlModulesPG.const as const

VK_ACCOUNT = [
    #--0--
    { 'login': '89253701963',
      'password': 'Ubn_Ubn_2020_5$%$#',
      'app_id': '7700818'
     }
    ]

if False and const.PY_ENVIRONMENT:
    import CrawlModulesPyOnly.self_psw as self_psw

    VK_ACCOUNT[0] = { 'login': '89273824101',
                      'password': self_psw.get_psw_vk_mtyurin(),
                      'app_id': '7467601'
                     }


#первый запрос токена https://oauth.vk.com/authorize?client_id=7700818&display=page&redirect_uri=https://oauth.vk.com/blank.html&scope=wall&response_type=token