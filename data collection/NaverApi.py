from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import json
import requests
from requests.exceptions import SSLError
from datetime import datetime, timedelta
import pytz


class News:
    def __init__(self, client_id, client_secret):
        self.url = 'https://openapi.naver.com/v1/search/news.json'
        self.headers = {'X-Naver-Client-Id': client_id,
                        'X-Naver-Client-Secret': client_secret}

    def get(self, keywords, sort='sim', display_num=50):
        # sim: similarity 유사도, date: 날짜
        news_items = []

        for keyword in keywords:
            start_num = 1

            params = {'display': display_num, 'start': start_num,
                      'query': keyword.encode('utf-8'), 'sort': sort}

            # B-2. API Request
            r = requests.get(self.url, headers=self.headers,  params=params)

            # C. Response 결과
            # C-1. 응답결과값(JSON) 가져오기
            # Request(요청)이 성공하면
            if r.status_code == requests.codes.ok:
                result_response = json.loads(r.content.decode('utf-8'))

                result = result_response['items']
            else:
                # print('request 실패!')
                failed_msg = json.loads(r.content.decode('utf-8'))
                print(failed_msg)

            news_items.extend(result)

        return news_items

    def date_filter(self, datas):
        # 전날 데이터만 return
        result = []
        target_date = cal_datetime_kst(1)
        for data in datas:
            dt = datetime.strptime(data['pubDate'], '%a, %d %b %Y %X %z')
            if target_date['date_st'] <= dt <= target_date['date_end']:
                result.append(data)
        return result

    def description_filter(self, datas):
        # 내용이 없는 기사 필터링
        result = []
        for data in datas:
            if data['description'] != '':
                result.append(data)
        return result

    def filtering(self, datas):
        datas = self.date_filter(datas)
        datas = self.description_filter(datas)
        return datas

    def save(self, collection, docs):
        collection.remove()
        result = 'success'
        collection.create_index([('link', 1)], unique=True)
        try:
            collection.insert_many(docs, ordered=False)

        except BulkWriteError as bwe:
            result = 'Insert and Ignore duplicated data'

        return result


class Summary:
    def __init__(self, client_id, client_secret):
        self.url = 'https://naveropenapi.apigw.ntruss.com/text-summary/v1/summarize'
        self.headers = {
            'X-NCP-APIGW-API-KEY-ID': client_id,
            'X-NCP-APIGW-API-KEY': client_secret,
            'Content-Type': 'application/json'
        }

    def get(self, title, content, num):
        data = {
            "document": {
                "title": title,
                "content": content
            },
            "option": {
                "language": "ko",
                "model": "news",
                "summaryCount": num
            }
        }
        result = ''
        r = requests.post(self.url, headers=self.headers,
                          data=json.dumps(data))
        if r.status_code == requests.codes.ok:
            result_response = json.loads(r.content)

            result = result_response['summary']
        else:
            # print('request 실패!')
            failed_msg = json.loads(r.content)
            print(failed_msg)

        return result

    def save(self, collection, link, data):
        collection.update_one(
            {'link': link}, {'$set': {'summary': data}})

    def loop(self, sentence_num, collection):
        cnt_items = collection.count_documents({})

        # page 번호 - 몇 페이지까지 조회해와야 데이터를 다 가져올 수 있을지 계산하기
        page_num = (cnt_items // 30) + 1

        # 30개씩 가져와서 summary 한 후 데이터베이스에 업데이트하기 - 반복
        for i in range(page_num):
            # print(i)
            skip_num = i * 30
            print(f'{i}번째 진행. {skip_num}번째 이후부터')
            # 예. 61~90 번 -> skip(60).limit(30)
            limit_items = list(collection.find(
                {}, {'_id': False}).skip(skip_num).limit(30))
            for item in limit_items:
                if 200 < len(item['content'])+len(item['title']) < 2000:
                    result = self.get(
                        item['title'], item['content'], sentence_num)
                else:
                    # description -> summary field
                    result = item['description']
                # DB 에 업데이트
                self.save(collection, item['link'], result)


class Sentiment:
    def __init__(self, client_id, client_secret):
        self.url = 'https://naveropenapi.apigw.ntruss.com/sentiment-analysis/v1/analyze'
        self.headers = {
            'X-NCP-APIGW-API-KEY-ID': client_id,
            'X-NCP-APIGW-API-KEY': client_secret,
            'Content-Type': 'application/json'
        }

    def get(self, content):
        data = {
            'content': content
        }

        r = requests.post(self.url, headers=self.headers,
                          data=json.dumps(data))
        result = ''
        if r.status_code == requests.codes.ok:
            result_response = json.loads(r.content)

            result = result_response['document']
        else:
            # print('request 실패!')
            failed_msg = json.loads(r.content)
            print(failed_msg)
        return result

    def save(selt, collection, link, data):
        collection.update_one(
            {'link': link}, {'$set': {'sentiment': data['sentiment'],
                                      'positive': data['confidence']['positive'],
                                      'neutral': data['confidence']['neutral'],
                                      'negative': data['confidence']['negative']}})

    def delete_data(self, collection, link):
        collection.delete_one({'link': link})

    def loop(self, collection):
        cnt_items = collection.count_documents({})

        # page 번호 - 몇 페이지까지 조회해와야 데이터를 다 가져올 수 있을지 계산하기
        page_num = (cnt_items // 30) + 1

        # 30개씩 가져와서 summary 한 후 데이터베이스에 업데이트하기 - 반복
        for i in range(page_num):
            # print(i)
            skip_num = i * 30
            print(f'{i}번째 진행. {skip_num}번째 이후부터')
            # 예. 61~90 번 -> skip(60).limit(30)
            limit_items = list(collection.find(
                {}, {'_id': False}).skip(skip_num).limit(30))
            for item in limit_items:
                result = ''
                if len(item['summary']) < 1000:
                    result = self.get(item['summary'])

                # DB 에 업데이트
                if result != '':
                    self.save(collection, item['link'], result)
                else:
                    # sentiment 분석 실패한 데이터 삭제
                    self.delete_data(collection, item['link'])


def cal_datetime_kst(before_date, timezone='Asia/Seoul'):
    '''
    현재 일자에서 before_date 만큼 이전의 일자의 시작시간,끝시간 반환
    :param before_date: 이전일자
    :param timezone: 타임존
    :return: 해당일의 시작시간(date_st)과 끝 시간(date_end)
    :rtype: dict of datetime object
    :Example:
    2021-09-13 KST 에 get_date(1) 실행시,
    return은 {'date_st': datetype object 형태의 '2021-09-12 00:00:00+09:00'), 'date_end': datetype object 형태의 '2021-09-12 23:59:59.999999+90:00'}
    '''
    today = pytz.timezone(timezone).localize(datetime.now())
    target_date = today - timedelta(days=before_date)

    # 같은 일자 same date 의 00:00:00 로 변경
    start = target_date.replace(hour=0, minute=0, second=0,
                                microsecond=0)

    # 같은 일자 same date 의 23:59:59 로 변경
    end = target_date.replace(
        hour=23, minute=59, second=59, microsecond=999999)

    return {'date_st': start, 'date_end': end}
