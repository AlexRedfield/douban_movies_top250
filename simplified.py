import re
import requests
import csv
import pymysql
import lxml.html

DOWNLOAD_URL = 'http://movie.douban.com/top250/'
#  连接数据库
db = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='syb123', db='douban',
                     charset='utf8')
cur = db.cursor()


def download_page(url): # 下载页面
    return requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0'
    }).text


def execute_db(movies_info):  # 将获取的电影信息导入数据库
    sql = "INSERT INTO test(rank, NAME, score, country, year, " \
          "category, votes, douban_url) values(%s,%s,%s,%s,%s,%s,%s,%s)"
    try:
        cur.executemany(sql, movies_info)
        db.commit()
    except Exception as e:
        print("Error:", e)
        db.rollback()


def parse_html(html, writer, movies_info):  # 使用lxml爬取数据并清洗
    tree = lxml.html.fromstring(html)

    movies = tree.xpath("//ol[@class='grid_view']/li")
    for movie in movies:

        name_num = len(movie.xpath("descendant::span[@class='title']"))
        name = ''
        for num in range(0, name_num):
            name += movie.xpath("descendant::span[@class='title']")[num].text.strip()
        name = ' '.join(name.replace('/', '').split())  # 清洗数据
        # 排名，豆瓣页面
        num = movie.xpath("descendant::em/text()")[0]
        # < class 'lxml.etree._ElementUnicodeResult'>
        url = movie.xpath("descendant::div[@class='pic']/a/@href")[0]
        # 年份，国家，类型
        data = movie.xpath("descendant::div[@class='bd']/p")[0].xpath('string(.)')
        info = re.findall(r'\d.*', data)[0].split('/')
        year, country, category = info[0].strip(), info[1].strip(), info[2].strip()
        # 得分，投票数
        score = movie.xpath("descendant::div[@class='star']/span")[1].text
        voting_num = movie.xpath("descendant::div[@class='star']/span")[3].text

        movie_info = (int(num), name, float(score), country, int(year), category, int(voting_num[0:-3]), url)
        movies_info.append(movie_info)
        #print(movie_info)
        writer.writerow(movie_info)
    try:
        next_page = tree.xpath("//span[@class='next']/a/@href")[0]
        return DOWNLOAD_URL + next_page
    except:
        return None


def main():
    url = DOWNLOAD_URL
    # 将数据导入到csv文件中
    writer = csv.writer(open('movies.csv', 'w', newline='', encoding='utf-8'))
    fields = ('rank',  'name', 'score', 'country', 'year', 'category', 'votes', 'douban_url')
    writer.writerow(fields)
    movies_info = []
    while url:
        html = download_page(url)
        url = parse_html(html, writer, movies_info)
    execute_db(movies_info)

if __name__ == '__main__':
    main()
