
def download_ner():
    import Modules.Parsing.ner as ner
    _ = ner.NerRecognizer(dict_download=True)

def download_lemma():
    import Modules.Parsing.lemma as lemma
    _ = lemma.Lemmatizer(dict_download=True)

def download_sentiment():
    import Modules.Parsing.sentiment as sentiment
    _ = sentiment.SentimentAnalizer(dict_download=True)

def download_all():
    download_sentiment()
    download_ner()
    download_lemma()

if __name__ == "__main__":
    download_all()


