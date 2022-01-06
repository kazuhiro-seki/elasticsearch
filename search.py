# -*- coding: utf-8 -*-

'''
elasticsearch での簡単な検索の例
'''

from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')

# 検索の関数

def search(keyword, index):

    # query definition
    query = {
        "query": {
            "match": {
                "title": keyword,
                }
        }
    }

    results = []
    scores = []
    ids = []
    for r in es.search(index=index, body=query)['hits']['hits']:
        ids.append(r['_id'])
        results.append(r['_source'])
        scores.append(r['_score'])

    return results, scores, ids


# 関数呼び出し

index = 'acm' # インデックスの名前を書く
query = 'algorithm improved' # 検索クエリを書く
docs, scores, ids = search(query, index)

for d, s, i in zip(docs, scores, ids):
    print(s, i, d)

# 検索された文書の統計的詳細を見る方法（普通は不要）

print
print('Details:')

for doc in es.mtermvectors(
        index=index,
        body=dict(
            ids=ids,
            parameters=dict(
                term_statistics=True,
                field_statistics=True,
                fields=['title', 'abstract']
                )
        )
)['docs']:
    # すべての情報
    #print(doc)

    # たとえばTFとDFだけ出力
    print('[ID ' + doc['_id'] + ']')
    for w in query.split():
        if w in doc['term_vectors']['title']['terms']:
            print(w,
                  doc['term_vectors']['title']['terms'][w]['term_freq'],
                  doc['term_vectors']['title']['terms'][w]['doc_freq'])
            
