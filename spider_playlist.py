import threading
import requests
import pymysql
import queue
import json
from urllib.parse import quote
from bs4 import BeautifulSoup


semaphore = threading.Semaphore(30)
types = queue.Queue()

# 网易云音乐
host = "http://music.163.com"

# 歌单种类 全部
all_playlist = "/discover/playlist/?order=hot"
type_page = "/discover/playlist/?cat={}&order=hot"
list_page = "/discover/playlist/?order=hot&cat={}&limit=35&offset={}"


header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6)'
                  ' AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
    'Cookie': 'appver=1.5.0.75771;',
    'Referer': 'http://music.163.com/'
}


def _get_proxy():
    """
    获取代理
    :return: 
    """
    url = 'http://127.0.0.1:50010/one'
    result = json.loads(requests.get(url).text)
    proxy = {
        'http': 'http://' + result['ip']
    }
    return proxy



def check_sql():
    """
    检查数据库连接，创建表
    :return: 
    """
    conn = pymysql.connect(host='127.0.0.1', db='moyu', user='root', passwd='root', charset='utf8')
    cursor = conn.cursor()
    try:
        sql = '''
                CREATE TABLE `moyu`.`playlist` (
                `id` VARCHAR(255) CHARACTER SET 'utf8' COLLATE 'utf8_unicode_ci' NOT NULL,
                `name` VARCHAR(255) NOT NULL,
                `num：万` FLOAT NOT NULL,
                `img` VARCHAR(255) NOT NULL,
                PRIMARY KEY (`id`));

                '''
        cursor.execute(sql)
        print('创建 playlist 表')
    except Exception as e:
        if '1050' in str(e):
            print('playlist 表已存在')
        else:
            print(e)
    conn.commit()
    cursor.close()
    conn.close()


def get_all_type():
    """
    抓取歌单种类及各歌单页数 
    :return: 
    typelist: 返回 种类 页数
    """
    typelist = []
    s = requests.session()
    result = s.get(host + all_playlist, headers=header)
    soup = BeautifulSoup(result.text, 'html.parser')
    types = soup.find('div', {'class': 'bd'}).select('dl')
    for dl in types:
        for a in dl.select('dd')[0].select('a'):
            typelist.append(a.string)
    threadList = []
    for type in typelist:
        t = threading.Thread(target=_get_listpage, args=(type,))
        threadList.append(t)
        t.start()
    for thread in threadList:
        thread.join()


def _get_listpage(name):
    """
    返回单个种类下的page
    :param name: 歌单名
    :return: 
    """
    Type = dict()
    s = requests.session()
    result = s.get(host+type_page.format(quote(name)), headers=header)
    soup = BeautifulSoup(result.text, 'html.parser')
    Type['name'] = name
    try:
        Type['page'] = int(soup.find('div', {'class': 'u-page'}).select('a')[-2].string)
        types.put(Type)
    except Exception as e:
        print(result.text)



def store_type(thread_name):
    """
    存储各分类
    :param threadname: 
    :return: 
    """
    while not types.empty():
        try:
            Type = types.get(timeout=10)
            print("%s 获得数据:%s " % (thread_name, Type['name']))
        except Exception as e:
            print('无数据%s线程退出' % thread_name)
            break
        threadList = []
        for page in range(Type['page']):
            t = threading.Thread(target=_store_playlist, args=(Type['name'], page))
            threadList.append(t)
            t.start()
        for thread in threadList:
            thread.join()
        types.task_done()
        print('%s 分类抓取完成' % Type['name'])


def _store_playlist(name, page):
    """
    抓取 name 分类下，第 page 页的歌单
    :param name: 
    :param page: 
    :return: 
    """
    semaphore.acquire()
    conn = pymysql.connect(host='127.0.0.1', db='moyu', user='root', passwd='root', charset='utf8')
    cursor = conn.cursor()
    url = host + list_page.format(quote(name), str(page * 35))
    s = requests.session()
    try:
        result = s.get(url, headers=header, proxies=_get_proxy())
    except Exception as e:
        result = s.get(url, headers=header, proxies=_get_proxy())
    soup = BeautifulSoup(result.text, 'html.parser')
    try:
        ul = soup.find('ul', {'class': 'm-cvrlst f-cb'}).select('li')
    except Exception as e:
        print(e)
        return
    for li in ul:
        img = (li.select('div')[0].select('img')[0]).attrs['src']
        name = (li.select('div')[0].select('a')[0]).attrs['title']
        _id = (li.select('div')[1].select('a')[0]).attrs['data-res-id']
        num = int(li.select('div')[1].select('span')[1].string.replace('万', '0000'))/10000
        try:
            sql = "insert into playlist value(%s,%s,%s,%s)"
            print(name)
            cursor.execute(sql, (_id, name, float(num), img))
        except Exception as e:
            print(e)
            continue
    conn.commit()
    cursor.close()
    conn.close()
    semaphore.release()


if __name__ == '__main__':
    check_sql()
    print('抓取分类信息')
    get_all_type()
    print('分类信息抓取完成')
    threadList_1 = []
    for i in range(10):
        name = 'thread-' + str(i)
        t = threading.Thread(target=store_type, args=(name,))
        threadList_1.append(t)
        t.start()

    # 设置一个守护线程 显示进度条
    # pass

    for thread in threadList_1:
        thread.join()

    print('采集完成')