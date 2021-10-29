from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import json
import requests
from requests.exceptions import SSLError
from bs4 import BeautifulSoup


class Scrapping:
    def __init__(self):
        self.header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'}

    def scrape_image_url(self, url):
        """
        웹 페이지에서 og:image 링크 scraping
        :param url: 웹 페이지 url
        :return: og:image 링크
        :rtype: str
        """
        # 기본 이미지 url  설정 / ref : https://unsplash.com/photos/tAcoHIvCtwM
        image_url = 'https://images.unsplash.com/photo-1588492069485-d05b56b2831d?ixid=MnwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8&ixlib=rb-1.2.1&auto=format&fit=crop&w=1051&q=80'

        # ==========1. GET Request==========
        # Request 설정값(HTTP Msg) - Desktop Chrome 인 것처럼

        try:
            data = requests.get(url, headers=self.header)
        except SSLError as e:
            # print(e)
            data = requests.get(url, headers=self.header, verify=False)

        # ========2. 특정 요소 접근하기===========
        # BeautifulSoup4 사용해서 html 요소에 각각 접근하기 쉽게 만듦.
        soup = BeautifulSoup(data.text, 'html.parser')

        # image url 가져오기 - og:image
        og_img_el = soup.select_one('meta[property="og:image"]')
        # 만약 해당 tag가 없으면 바로 기본 image_url 을 반환하고 함수 종료
        if not og_img_el:
            return image_url

        image_url = og_img_el['content']
        # 예외 - http 없는 경우 앞에 붙여주기
        if 'http' not in image_url:
            image_url = 'http:' + image_url

        return image_url

    def scrape_content(self, url):
        """
        네이버 뉴스에서 기사 본문 scraping 해오기
        :param url: 네이버 뉴스 기사 url
        :return content 기사본문 없으면 빈 문자열
        :rtype: str
        """
        content = ''

        # ==========1. GET Request==========
        # Request 설정값(HTTP Msg) - Desktop Chrome 인 것처럼
        try:
            data = requests.get(url, headers=self.header)
        except SSLError as e:
            # print(e)
            data = requests.get(url, headers=self.header, verify=False)

        # ========2. 특정 요소 접근하기===========
        # BeautifulSoup4 사용해서 html 요소에 각각 접근하기 쉽게 만듦.
        soup = BeautifulSoup(data.text, 'html.parser')
        content = ''

        if 'news.naver.com' in url:
            # ========== news_naver ==========
            naver_content = soup.select_one(
                '#articeBody') or soup.select_one('#articleBodyContents')

            # 해당 tag 가 존재하지 않으면 기본 content return 하고 함수 종료
            if not naver_content:
                return content

            for tag in naver_content(['div', 'span', 'p', 'br', 'script']):
                tag.decompose()
            content = naver_content.text.strip()

        return content

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
                image_url = self.scrape_image_url(item['link'])
                content = self.scrape_content(item['link'])
                if content == '':
                    content = item['description']

                collection.update_one({'link': item['link']},
                                      {'$set': {'image_url': image_url}})
                collection.update_one({'link': item['link']},
                                      {'$set': {'content': content}})
