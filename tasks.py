from sklearn.cluster.k_means_ import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from celery import Celery
import pymongo
from itertools import chain

app = Celery('tasks', backend='amqp', broker='amqp://')


@app.task()
def perform_cluster(data, params):
    km = KMeans()
    km.set_params(**params)
    vectorizer = TfidfVectorizer()
    print(data[1][0])
    tfidf = vectorizer.fit_transform(data[1])
    labels = km.fit_predict(tfidf)
    result = {i: [] for i in set(labels)}
    for i, l in zip(range(len(labels)), labels):
        result[l].append(data[0][i])
    return result


@app.task()
def load_data(query, db_connect):
    data = list(pymongo.MongoClient(db_connect['addr'], db_connect['port']).root.Posts.find(query))
    return data


@app.task()
def process_data(raw):
    ids, texts = [], []
    for r in raw:
        id, lemmas = r['_id'], list(
            chain(
                *list(
                    chain(
                        *[
                            [
                                [l["_lemma"] for l in t["_tokens"]] for t in s["_sentences"]
                            ]
                            for s in r["Annotation"]["_paragraphs"]
                        ]
                    )
                )
            )
        )
        ids.append(str(id))
        texts.append(' '.join(lemmas))
    return ids, texts