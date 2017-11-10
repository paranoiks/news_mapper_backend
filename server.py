from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import json

from flask import Flask
from flask import request

app = Flask(__name__)

def ElasticConnect():
	host = 'search-news-mapper-g74yzjnkcvobfi5uakasm6brtq.us-east-2.es.amazonaws.com'
	awsauth = AWS4Auth('AKIAJC33CWG63SB5TXHA', 'RxnertdVv3+vIHiKqgs/XsoikouraFSpUsJbXha5', 'us-east-2', 'es')

	es = Elasticsearch(
	    hosts=[{'host': host, 'port': 443}],
	    http_auth=awsauth,
	    use_ssl=True,
	    verify_certs=True,
	    connection_class=RequestsHttpConnection
	)

	return es;


@app.route("/")
def hello():
	es=ElasticConnect()
	return json.dumps(es.info())

@app.route("/add", methods=['POST'])
def addNews():
	newsTitle = request.args.get("title")
	newsBody = request.args.get("body")

	es=ElasticConnect()
	json_file_body = "{{\"title\": \"{0}\", \"body\": \"{1}\"}}".format(newsTitle, newsBody)
	#json_file_body = '{"title": "sad", "body": "das"}'
	es.index(index="news", doc_type="news", body=json_file_body)
	return json_file_body

@app.route("/get", methods=['GET'])
def getNews():
	es=ElasticConnect()
	doc = {
        'size' : 10000,
        'query': {
            'match_all' : {}
       }
   	}
   	#this will only return the first 10000 results:
   	#understand and use:
   	#scrollId = res['_scroll_id']
	#es.scroll(scroll_id = scrollId, scroll = '1m')

	res = es.search(index='news', doc_type='news', body=doc, scroll='1m')
	resultsList = res["hits"]["hits"]

	return json.dumps(resultsList)

#json_body='{"title": "boy","body": "girl"}'

#es.index(index='news', doc_type='news', id=1, body=json_body)
#print(es.get(index='news', doc_type='news', id=1))


if __name__ == "__main__":
	app.run()