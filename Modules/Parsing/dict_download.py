
def download_ner():
    import modules.parsing.ner as ner
    _ = ner.NerRecognizer(dict_download=True)

def download_lemma():
    import modules.parsing.lemma as lemma
    _ = lemma.Lemmatizer(dict_download=True)

def download_sentiment():
    import modules.parsing.sentiment as sentiment
    _ = sentiment.SentimentAnalizer(dict_download=True)

def download_all():
    download_sentiment()
    download_ner()
    download_lemma()

if __name__ == "__main__":
    download_all()


