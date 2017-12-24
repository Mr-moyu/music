import requests
import queue
import json
import threading
import pymysql
import base64
from Crypto.Cipher import AES
from bs4 import BeautifulSoup
from DBUtils.PooledDB import PooledDB

# 网易云音乐
host = "http://music.163.com"
song = '/song?id='
comments = "/weapi/v1/resource/comments/R_SO_4_{}?csrf_token="
data = {
    'params': '',
    'encSecKey': ''
}

Songs = queue.Queue()
header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6)'
                  ' AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
    'Cookie': 'appver=1.5.0.75771;',
    'Referer': 'http://music.163.com/'
}

first_param = "{rid:\"\", offset:\"0\", total:\"true\", limit:\"20\", csrf_token:\"\"}"
second_param = "010001"
third_param = "00e0b509f6259df8642dbc35662901477df22677ec152b5ff68ace615bb7b725152b3ab17a876aea8a5aa76d2e417629ec4ee341f56135fccf695280104e0312ecbda92557c93870114af6c9d05c4f7f0c3685b7a46bee255932575cce10b424d813cfe4875d3e82047b97ddef52741d546b8e289dc6935b3ece0462db0a22b8e7"
forth_param = "0CoJUm6Qyw8W8jud"

# 连接池
pool = PooledDB(pymysql, 10, host='localhost', user='root', passwd='root', db='moyu', port=3306, blocking=True,
                charset='utf8mb4')


def _executeSql(s, arg=None):
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
        检查数据库连接，创建表
        :return: 
        """
    conn = pymysql.connect(host='127.0.0.1', db='moyu', user='root', passwd='root', charset='utf8')
    cursor = conn.cursor()
    try:
        sql = '''
                    CREATE TABLE `moyu`.`comments` (
                    `id` VARCHAR(255) CHARACTER SET 'utf8' COLLATE 'utf8_unicode_ci' NOT NULL,
                    `name` VARCHAR(255) NOT NULL,
                    `num` INT(10) NOT NULL,
                    `song_id` VARCHAR(255) NOT NULL,
                    `comment` VARCHAR(5000) CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_unicode_ci' NOT NULL,
                    `img` VARCHAR(255) NOT NULL,
                    PRIMARY KEY (`id`));

                    '''
        cursor.execute(sql)
        print('创建comments表')
    except Exception as e:
        if '1050' in str(e):
            print('comments表已创建')
        else:
            print(e)
    conn.commit()
    cursor.close()
    conn.close()


def all_songs():
    """
    将歌单放入队列
    :return: 
    """
    conn = pymysql.connect(host='127.0.0.1', db='moyu', user='root', passwd='root', charset='utf8mb4')
    cursor = conn.cursor()
    count = cursor.execute('select * from song')
    print("共 %d 首歌" % count)
    i = 0
    while i <= count:
        Songs.put(cursor.fetchone())
        i += 1
    cursor.close()
    conn.close()


def get_detail(thread_name):
    while not Songs.empty():
        try:
            Song = Songs.get(timeout=10)
            print("%s 拿到数据 %s 还剩 %d" % (thread_name, Song[1], Songs.qsize()))
        except Exception as e:
            print("%s 未拿到数据退出 还剩 %d" % (thread_name, Songs.qsize()))
            break
        else:
            get_comments(thread_name, Song)


def get_comments(thread_name, Song):
    s = requests.session()
    params = _get_params()
    encSecKey = _get_encSecKey()
    data = {
        "params": params,
        "encSecKey": encSecKey
    }
    i = 0
    while True:
        proxy, ip = _get_proxy()
        try:
            url = host + song + Song[0]
            # print(url)
            result = requests.get(url, headers=header, proxies=proxy)
            # print(result.text)
            soup = BeautifulSoup(result.text, 'html.parser')
            try:
                detail = json.loads(soup.find('script', {'type': 'application/ld+json'}).string)
                songer = detail['description'].split('。')[0].split('：')[1]
                img_song = detail['images'][0]
                break
            except Exception as e:
                print("%s 放回数据 %s 还剩 %d" % (thread_name, Song[1], Songs.qsize()))
                Songs.put(Song)
                return
        except Exception as e:
            print(thread_name, e)
            _del_proxy(ip)
            i += 1
            continue
    if i == 3:
        print("%s 放回数据 %s 还剩 %d" % (thread_name, Song[1], Songs.qsize()))
        Songs.put(Song)
        return
    i = 0
    while True:
        proxy, ip = _get_proxy()
        try:
            url = host + comments.format(Song[0])
            result = s.post(url, headers=header, data=data, proxies=proxy)
            break
        except Exception as e:
            print(thread_name, e)
            _del_proxy(ip)
            i += 1
            continue
    if i == 3:
        print("%s 放回数据 %s 还剩 %d" % (thread_name, Song[1], Songs.qsize()))
        Songs.put(Song)
        return
    try:
        json_dict = json.loads(result.content)
    except Exception as e:
        print("%s 放回数据 %s 还剩 %d" % (thread_name, Song[0], Songs.qsize()))
        Songs.put(Song)
        return
    comment_num = json_dict['total']
    try:
        _executeSql("update song set songer = %s,img = %s,num = %s where id = %s",
                    (songer, img_song, comment_num, Song[0]))
    except Exception as e:
        print(thread_name, e)
    for item in json_dict['hotComments']:
        _id = item['commentId']
        count = item['likedCount']
        content = item['content']
        username = item['user']['nickname']
        img_user = item['user']['avatarUrl']
        try:
            sql = '''insert into comments VALUES(%s,%s,%s,%s,%s,%s)'''
            _executeSql(sql, (_id, username, count, Song[0], content, img_user))
        except Exception as e:
            if '1062' in str(e):
                pass
            else:
                print(thread_name, e, content)
    Songs.task_done()


def _get_params():
    iv = "0102030405060708"
    first_key = forth_param
    second_key = 16 * 'F'
    h_encText = _AES_encrypt(first_param, first_key, iv)
    h_encText = _AES_encrypt(h_encText, second_key, iv)
    return h_encText


def _get_encSecKey():
    encSecKey = "257348aecb5e556c066de214e531faadd1c55d814f9be95fd06d6bff9f4c7a41f831f6394d5a3fd2e3881736d94a02ca919d952872e7d0a50ebfa1769a7a62d512f5f1ca21aec60bc3819a9c3ffca5eca9a0dba6d6f7249b06f5965ecfff3695b54e1c28f3f624750ed39e7de08fc8493242e26dbc4484a01c76f739e135637c"
    return encSecKey


def _AES_encrypt(text, key, iv):
    pad = 16 - len(text) % 16
    text = text + (pad * chr(pad))
    encryptor = AES.new(key, AES.MODE_CBC, iv)
    encrypt_text = encryptor.encrypt(text)
    encrypt_text = base64.b64encode(encrypt_text)
    encrypt_text = str(encrypt_text, encoding="utf-8")
    return encrypt_text


if __name__ == "__main__":
    check_sql()
    threadList_1 = []
    # thread_songs = threading.Thread(target=all_songs, name='thread_songs')
    # threadList_1.append(thread_songs)
    # thread_songs.start()
    # time.sleep(3)
    all_songs()

    for i in range(50):
        name = 'thread-' + str(i)
        t = threading.Thread(target=get_detail, args=(name,), name=name)
        threadList_1.append(t)
        t.start()

    for thread in threadList_1:
        thread.join()
        print(str(thread.name) + ' 退出 ' + str(Songs.qsize()))

    print('采集完成')
