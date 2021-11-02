# DailyMotion
|home|calendar|
|----|----|
|<img src="https://github.com/LIMM036/my_news_webpage/blob/master/images/home.png">|<img src="https://github.com/LIMM036/my_news_webpage/blob/master/images/calendar.png">|
|news|contact|
|<img src="https://github.com/LIMM036/my_news_webpage/blob/master/images/news.png">|<img src="https://github.com/LIMM036/my_news_webpage/blob/master/images/contact.png">|

## 1. DailyMotion 이란?

## 2. 구성

![](https://github.com/LIMM036/my_news_webpage/blob/master/images/configuration.png)

## 3. 서버
- Naver Cloud Flatform에서 서버를 대여하여 사용 [https://www.ncloud.com/product/compute/server]


## 4. 데이터 수집 / 가공  
 - Naver에서 제공하는 api를 사용하여 데이터를 수집하고 가공한다.  
 - Scrapping하여 데이터를 수집한다.  
 - NaverCloudFunction을 사용하여 서버의 db에 원격 접속하여 수집한 데이터를 저장한다. [https://www.ncloud.com/product/compute/cloudFunctions]


  1. 뉴스 수집  
  - Naver news searching api 사용 [https://developers.naver.com/docs/serviceapi/search/news/news.md]

  2. 뉴스 본문 내용 수집  
  - 수집한 url을 통해 뉴스의 본문을 수집한다.

  3. Summary  
  - Clova summary api를 사용하여 뉴스 본문을 summary 한다. [https://www.ncloud.com/product/aiService/clovaSummary]

  4. Sentiment 분석  
  - Clova sentiment api를 사용하여 summary한 내용으로 해당 기사의 sentiment를 분석한다. [https://www.ncloud.com/product/aiService/clovaSentiment]
