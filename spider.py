import requests
import chardet
import pymongo
from multiprocessing import Pool
from requests.exceptions import ConnectionError
from pyquery import PyQuery as pq
headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.27 Safari/537.36'}

#配置MongoDB数据库
client = pymongo.MongoClient('localhost')
db = client['Hupu']

#获得起始页的HTMl
def get_one_page(url):
    try:
        response = requests.get(url,headers=headers)
        if response.status_code == 200:
            response.encoding = chardet.detect(response.content)['encoding']
            return response.text
        return None
    except ConnectionError:
        print('请求初始页失败')
        return None

#分析起始页，获得每一页里面的每篇文章的URL的一部分，然后在main函数中进行拼接
def parse_one_page(html):
    doc = pq(html)
    items = doc('.show-list .for-list li').items()
    for item in items:
        yield item.find('.truetit').attr['href']

#获取每篇文章的HTML
def get_detail_page(url):
    try:
        response = requests.get(url,headers=headers)
        if response.status_code == 200:
            return response.text
        return None
    except ConnectionError:
        print('请求初始页失败')
        return None

#获取每篇文章的一些信息，这里使用的pyquery解析库
def parse_detail_page(html):
    doc = pq(html)
    title = doc('#j_data').text()
    author = doc('#tpc > div > div.floor_box > div.author > div.left > a').text()
    time = doc('#tpc > div > div.floor_box > div.author > div.left > span.stime').text()
    response_num = doc('.bbs_head .bbs-hd-h1 .browse').text().replace('浏览','')
    content = doc('#tpc > div > div.floor_box > table.case > tbody > tr > td > div.quote-content').text()
    data = {
        'title':title,
        'author':author,
        'time':time,
        'respone_num':response_num,
        'content':content
    }
    save_to_mongo(data)

#存储到数据路MongoDB
def save_to_mongo(data):
    if db['buxingjie'].insert(data):
        print('存储到MongoDB数据库成功',data)
    else:
        print('存储到数据库失败',data)

#主函数
def main(page):
    print('正在爬取第{}页。'.format(page))
    url = 'https://bbs.hupu.com/bxj-{}'.format(page)
    html = get_one_page(url)
    for i in parse_one_page(html):
        detail_page_url = 'https://bbs.hupu.com{}'.format(i)
        html = get_detail_page(detail_page_url)
        parse_detail_page(html)




if __name__ == '__main__':
    #添加多进程，提高爬取的速度
    pool = Pool()
    pool.map(main,[page for page in range(1,101)])
