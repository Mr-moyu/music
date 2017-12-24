import requests
import queue
import json
import threading
import pymysql
from DBUtils.PooledDB import PooledDB
from bs4 import BeautifulSoup

# 网易云音乐
host = "http://music.163.com"
playlist = "/playlist?id="

Lists = queue.Queue()
header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6)'
                  ' AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36'
}


# 连接池
pool = PooledDB(pymysql, 10, host='localhost', user='root', passwd='root', db='moyu', port=3306, blocking=True,
                charset='utf8mb4')


def _execute(s, arg=None):
    """
    使用数据库连接池
    :param s: 
    :param arg: 
    :return: 
    """
    conn = pool.connection()
    cur = conn.cursor()
    cur.execute('''set names utf8mb4;''')
    cur.execute('SET CHARACTER SET utf8mb4;')
    cur.execute('SET character_set_connection=utf8mb4;')
    cur.execute(s, arg)
    conn.commit()
    cur.close()
    conn.close()


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
    return proxy, result['ip']


def _del_proxy(ip):
    """
    删除不可用代理
    :param ip: 
    :return: 
    """
    url = 'http://127.0.0.1:50010/del'
    data = {
        'ip': ip
    }
    result = requests.post(url, data=data)
    if result.text == 'ok':
        print('删除ip %s 成功' % ip)
    else:
        print('删除ip %s 失败' % ip)


def check_sql():
    """
    创建 table 有的时候则跳过
    :return: 
    """
    conn = pymysql.connect(host='127.0.0.1', db='moyu', user='root', passwd='root', charset='utf8mb4')
    cursor = conn.cursor()
    try:
        sql = '''
                CREATE TABLE `moyu`.`song` (
                    `id` VARCHAR(255) CHARACTER SET 'utf8' COLLATE 'utf8_unicode_ci' NOT NULL,
                    `name` VARCHAR(255) NOT NULL,
                    `songer` VARCHAR(255) NOT NULL,
                    `num` FLOAT NOT NULL,
                    `img` VARCHAR(255) NOT NULL,
                    PRIMARY KEY (`id`));
    
        '''
        cursor.execute(sql)
        conn.commit()
        print('创建 song 表')
    except Exception as e:
        if '1050' in str(e):
            print('song 表已存在')
        else:
            print(e)
    cursor.close()
    conn.close()


def all_playlist():
    """
    将歌单放入队列
    :return: 
    """
    conn = pymysql.connect(host='127.0.0.1', db='moyu', user='root', passwd='root', charset='utf8')
    cursor = conn.cursor()
    count = cursor.execute('select * from playlist')
    i = 0
    while i <= count:
        Lists.put(cursor.fetchone())
        i += 1
    cursor.close()
    conn.close()


def get_songs(thread_name):
    """
    将歌单中的歌曲存进数据库
    :param thread_name: 
    :return: 
    """
    while not Lists.empty():
        try:
            List = Lists.get(timeout=10)
            print("%s 拿到数据 %s 还剩 %d" % (thread_name, List[1], Lists.qsize()))
        except Exception as e:
            print("%s 未拿到数据退出" % thread_name)
            continue
        else:
            s = requests.session()
            url = host + playlist + List[0]
            # print(url)
            i = 0
            while True:
                proxy,ip = _get_proxy()
                try:
                    result = s.get(url, headers=header, proxies=proxy)
                    # print(result.text)
                    break
                except Exception as e:
                    print(thread_name, e)
                    _del_proxy(ip)
                    i += 1
                    continue
            if i == 3:
                print("%s 放回数据 %s 还剩 %d" % (thread_name, List[1], Lists.qsize()))
                Lists.put(List)
                continue
            soup = BeautifulSoup(result.text, 'html.parser')
            try:
                ul = soup.findAll('ul', {'class': 'f-hide'})[0].select('li')
            except Exception as e:
                print("%s 放回数据 %s 还剩 %d" % (thread_name, List[1], Lists.qsize()))
                Lists.put(List)
                print(thread_name,e)
                continue

            for li in ul:
                _id = li.select('a')[0].attrs['href'].split('id=')[1]
                name = li.select('a')[0].string
                try:
                    sql = "insert into song value(%s,%s,%s,%s,%s)"
                    _execute(sql, (_id, name, '', 0, ''))
                except Exception as e:
                    if '1062' in str(e):
                        pass
                    else:
                        print(e)
            Lists.task_done()

if __name__ == '__main__':
    check_sql()
    all_playlist()
    print("歌单共：%d" % Lists.qsize())

    threadList_1 = []
    for i in range(50):
        name = 'thread-' + str(i)
        t = threading.Thread(target=get_songs, args=(name,), name=name)
        threadList_1.append(t)
        t.start()


    for thread in threadList_1:
        thread.join()

    print('采集完成')



