import modules.common_mod.const as const
import modules.common_mod.common as common

from deeppavlov import configs, build_model

class SentimentAnalizer(common.CommonFunc):
    def __init__(self, *args,
                 db = None,
                 id_project = None,
                 id_www_source = None,
                 need_stop_cheker = None,
                 dict_download = False,
                 **kwargs):
        
        super().__init__(*args, **kwargs)

        self.db = db
        self.need_stop_checker = need_stop_cheker
        self.id_project = id_project
        self.id_www_source = id_www_source

        self.text_list = {}

        self.sent_model = build_model(configs.classifiers.rusentiment_elmo_twitter_cnn, download=dict_download)

    def get_text_portion(self):
        pass

    def analize(self, list_text_only):
        return self.sent_model(list_text_only)


    def process(self):
        pass

    def save_result(self):
        pass

    def check_user_interrupt(self):
        if self.need_stop_checker is None:
            return False
        self.need_stop_checker.need_stop()


