import os
import json
from datetime import datetime
from elasticsearch7 import Elasticsearch, RequestsHttpConnection
from elasticsearch7 import Transport
from elasticsearch7.exceptions import NotFoundError


class ES(object):
    _index = ""
    _type = ""

    def __init__(self, hosts):
        # 基于requests实例化es连接池
        self.conn_pool = Transport(hosts=hosts, connection_class=RequestsHttpConnection).connection_pool

    def get_conn(self):
        """
        从连接池获取一个连接
        """
        conn = self.conn_pool.get_connection()
        return conn

    def request(self, method, url, headers=None, params=None, body=None):
        """
        想es服务器发送一个求情
        @method     请求方式
        @url        请求的绝对url  不包括域名
        @headers    请求头信息
        @params     请求的参数：dict
        @body       请求体：json对象(headers默认Content-Type为application/json)
        # return    返回体：python内置数据结构
        """
        conn = self.get_conn()
        try:
            status, headers, body = conn.perform_request(method, url, headers=headers, params=params, body=body)
        except NotFoundError as e:
            return None
        if method == "HEAD":
            return status
        return json.loads(body)

    def search(self, query=None, method="GET"):
        url = "/%s/%s/_search" % (self._index, self._type)
        if method == "GET":
            data = self.get(url, params=query)
        elif method == "POST":
            data = self.post(url, body=query)
        else:
            return None
        return data

    def get(self, url, params=None, method="GET"):
        """
        使用get请求访问es服务器
        """
        data = self.request(method, url, params=params)
        return data

    def put(self, url, body=None, method="PUT"):
        """
        使用put请求访问es
        """
        data = self.request(method, url, body=body)
        return data

    def post(self, url, body=None, method="POST"):
        """使用post请求访问服务器"""
        data = self.request(method, url, body=body)
        return data

    def head(self, url, *args, **kwargs):
        status = self.request("HEAD", url, *args, **kwargs)
        return status

    def delete(self, url, *args, **kwargs):
        ret = self.request("DELETE", url, *args, **kwargs)
        return ret


if __name__ == '__main__':
    # 0.1 初始化
    es = Elasticsearch([{"host": "127.0.0.1", "port": 9200}])

    # 0.2 索引名 secret_es_1,secret_es_1=400,表示忽视400这个错误,如果存在secret_es_1时，会返回400"""
    es.indices.create(index='secret_es_1', ignore=400)

    # 1.1 插入单条数据
    body = {'name': '刘婵', "age": 6,
            "sex": "male", 'birthday': '1984-01-01',
            "salary": -12000}
    es.index(index='secret_es_1', doc_type='_doc', body=body)

    # 1.2 插入多条数据
    doc = [
        {'index': {'_index': 'secret_es_1', '_type': '_doc', '_id': 1}},
        {'name': '赵云', 'age': 25, 'sex': 'male', 'birthday': '1995-01-01', 'salary': 8000},
        {'index': {'_index': 'secret_es_1', '_type': '_doc', '_id': 2}},
        {'name': '张飞', 'age': 24, 'sex': 'male', 'birthday': '1996-01-01', 'salary': 8000},
        {'create': {'_index': 'secret_es_1', '_type': '_doc', '_id': 3}},
        {'name': '关羽', 'age': 23, 'sex': 'male', 'birthday': '1996-01-01', 'salary': 8000},
    ]
    es.bulk(index='secret_es_1', doc_type='_doc', body=doc)

    # 2.1 查询数据
    body = {
        'from': 0,  # 从0开始
        'size': 2  # 取2个数据。类似mysql中的limit 0, 20。 注：size可以在es.search中指定，也可以在此指定，默认是10
    }

    # 定义过滤字段，最终只显示此此段信息
    filter_path = ['hits.hits._source.ziduan1',  # 字段1
                   'hits.hits._source.ziduan2']  # 字段2

    es.search(index='secret_es_1', filter_path=filter_path, body=body)

    # 2.2 模糊查询数据
    body = {
        'query': {  # 查询命令
            'match': {  # 查询方法：模糊查询（会被分词）。比如此代码，会查到只包含：“我爱你”， “中国”的内容
                'name': '刘'
            }
        },
        'size': 20  # 不指定默认是10，最大值不超过10000（可以修改，但是同时会增加数据库压力）
    }

    # size的另一种指定方法
    es.search(index='secret_es_1', filter_path=filter_path, body=body, size=200)

    # 2.3 精准单值查询
    # 注：此方法只能查询一个字段，且只能指定一个值。类似于mysql中的where ziduan='a'
    body = {
        'query': {
            'term': {
                'ziduan1.keyword': '刘婵'  # 查询内容等于“我爱你中国的”的数据。查询中文，在字段后面需要加上.keyword
                    }
                }
            }

    # 2.4 精准多值查询
    # 此方法只能查询一个字段，但可以同时指定多个值。类似于mysql中的where ziduan in (a, b,c...)
    body = {
        "query": {
            "terms": {
                "ziduan1.keyword": ["刘婵", "赵云"]  # 查询ziduan1="刘婵"或=赵云...的数据
            }
        }
    }

    # 2.5多字段查询
    # 查询多个字段中都包含指定内容的数据
    body = {
        "query": {
            "multi_match": {
                "query": "我爱你中国",  # 指定查询内容，注意：会被分词
                "fields": ["ziduan1", "ziduan2"]  # 指定字段
            }
        }
    }

    # 2.6 前缀查询
    body = {
        'query': {
            'prefix': {
                'ziduan.keyword': '我爱你'  # 查询前缀是指定字符串的数据
            }
        }
    }

    # 2.7 通配符查询
    body = {
        'query': {
            'wildcard': {
                'ziduan1.keyword': '?刘婵*'  # ?代表一个字符，*代表0个或多个字符
            }
        }
    }

    # 2.8 正则查询
    body = {
        'query': {
            'regexp': {
                'ziduan1': 'W[0-9].+'  # 使用正则表达式查询
            }
        }
    }

    # 2.9 多条件查询
    # must：[] 各条件之间是and的关系
    body = {
        "query": {
            "bool": {
                'must': [{"term": {'ziduan1.keyword': '我爱你中国'}},
                         {'terms': {'ziduan2': ['I love', 'China']}}]
            }
        }
    }

    # should: [] 各条件之间是or的关系
    body = {
        "query": {
            "bool": {
                'should': [{"term": {'ziduan1.keyword': '我爱你中国'}},
                           {'terms': {'ziduan2': ['I love', 'China']}}]
            }
        }
    }

    # must_not：[]各条件都不满足
    body = {
        "query": {
            "bool": {
                'must_not': [{"term": {'ziduan1.keyword': '我爱你中国'}},
                             {'terms': {'ziduan2': ['I love', 'China']}}]
            }
        }
    }

    # bool嵌套bool
    # ziduan1、ziduan2条件必须满足的前提下，ziduan3、ziduan4满足一个即可
    body = {
        "query": {
            "bool": {
                "must": [{"term": {"ziduan1": "China"}},  # 多个条件并列  ，注意：must后面是[{}, {}],[]里面的每个条件外面有个{}
                         {"term": {"ziduan2.keyword": '我爱你中国'}},
                         {'bool': {
                             'should': [
                                 {'term': {'ziduan3': 'Love'}},
                                 {'term': {'ziduan4': 'Like'}}
                             ]
                         }}
                         ]
            }
        }
    }





