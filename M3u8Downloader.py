# UTF-8
# author hdc
# desc 必须在终端直接执行，不能在pycharm等IDE中直接执行，否则看不到动态进度条效果

import os
import sys
import m3u8
import time
import requests
import traceback
import threadpool
from urllib.parse import urlparse
from Crypto.Cipher import AES


class M3u8Downloader:

    def __init__(self):
        # 初始化线程池
        self.task_thread_pool = threadpool.ThreadPool(processCountConf)
        # 当前下载的m3u8 url
        self.m3u8_url = None
        # url前缀
        self.root_url_path = None
        # title
        self.title = None
        # ts count
        self.sum_count = 0
        # 已处理的ts
        self.done_count = 0
        # cache path
        self.cache_path = saveRootDirPath + "/cache"
        self.logFile = None
        # download bytes(0.5/1 s)
        self.downloaded_bytes = 0
        # download speed
        self.download_speed = 0

        # 设置log file
        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)
        # log file
        self.logFile = open(self.cache_path + "/log.log", "w+", encoding="utf-8")

    # 1、下载m3u8文件
    def getM3u8Info(self):
        try_count = m3u8TryCountConf
        while True:
            if try_count < 0:
                print("\t{0}下载失败！".format(self.m3u8_url))
                self.logFile.write("\t{0}下载失败！".format(self.m3u8_url))
                return None
            try_count = try_count - 1
            try:
                response = requests.get(self.m3u8_url, headers=headers, timeout=20)
                if response.status_code == 301:
                    now_m3u8_url = response.headers["location"]
                    print("\t{0}重定向至{1}！".format(self.m3u8_url, now_m3u8_url))
                    self.logFile.write("\t{0}重定向至{1}！\n".format(self.m3u8_url, now_m3u8_url))
                    self.m3u8_url = now_m3u8_url
                    self.root_url_path = self.m3u8_url[0:self.m3u8_url.index('/', 10)]
                    continue

                content_length = response.headers.get('Content-Length')
                if content_length:
                    expected_length = int(content_length)
                    actual_length = len(response.content)
                    if expected_length > actual_length:
                        raise Exception("m3u8下载不完整")

                print("\t{0}下载成功！".format(self.m3u8_url))
                self.logFile.write("\t{0}下载成功！".format(self.m3u8_url))
                self.root_url_path = self.m3u8_url[0:self.m3u8_url.index('/', 10)]
                break
            except (Exception, ):
                print("\t{0}下载失败！正在重试".format(self.m3u8_url))
                self.logFile.write("\t{0}下载失败！正在重试".format(self.m3u8_url))
        # 解析m3u8中的内容
        m3u8_info = m3u8.loads(response.text)
        # 有可能m3u8Url是一个多级码流
        if m3u8_info.is_variant:
            print("\t{0}为多级码流！".format(self.m3u8_url))
            self.logFile.write("\t{0}为多级码流！".format(self.m3u8_url))
            for row_data in response.text.split('\n'):
                # 寻找响应内容的中的m3u8
                if row_data.endswith(".m3u8"):
                    scheme = urlparse(self.m3u8_url).scheme
                    netloc = urlparse(self.m3u8_url).netloc
                    self.m3u8_url = scheme + "://" + netloc + row_data
                    self.root_url_path = self.m3u8_url[0:self.m3u8_url.rindex('/')]

                    return self.getM3u8Info()
            # 遍历未找到就返回None
            print("\t{0}响应未寻找到m3u8！".format(response.text))
            self.logFile.write("\t{0}响应未寻找到m3u8！".format(response.text))
            return None
        else:
            return m3u8_info

    # 2、下载key文件
    def getKey(self, key_url):
        try_count = m3u8TryCountConf
        while True:
            if try_count < 0:
                print("\t{0}下载失败！".format(key_url))
                self.logFile.write("\t{0}下载失败！".format(key_url))
                return None
            try_count = try_count - 1
            try:
                response = requests.get(key_url, headers=headers, timeout=20, allow_redirects=True)
                if response.status_code == 301:
                    now_key_url = response.headers["location"]
                    print("\t{0}重定向至{1}！".format(key_url, now_key_url))
                    self.logFile.write("\t{0}重定向至{1}！\n".format(key_url, now_key_url))
                    key_url = now_key_url
                    continue
                expected_length = int(response.headers.get('Content-Length'))
                actual_length = len(response.content)
                if expected_length > actual_length:
                    raise Exception("key下载不完整")
                print("\t{0}下载成功！key = {1}".format(key_url, response.content.decode("utf-8")))
                self.logFile.write("\t{0}下载成功！ key = {1}".format(key_url, response.content.decode("utf-8")))
                break
            except (Exception,):
                print("\t{0}下载失败！".format(key_url))
                self.logFile.write("\t{0}下载失败！".format(key_url))
        return response.text

    # 3、多线程下载ts流
    def multiDownloadTs(self, playlist):
        task_list = []
        # 每个ts单独作为一个task
        for index in range(len(playlist)):
            d = {"playlist": playlist, "index": index}
            task_list.append((None, d))
        # 重新设置ts数量，已下载的ts数量
        self.done_count = 0
        self.sum_count = len(task_list)
        self.printProcessBar(self.sum_count, self.done_count, 50)
        # 构造thread pool
        reqs = threadpool.makeRequests(self.downloadTs, task_list)
        [self.task_thread_pool.putRequest(req) for req in reqs]
        # 等待所有任务处理完成
        while self.done_count < self.sum_count:
            # 统计1秒钟下载的byte
            before_downloaded_bytes = self.downloaded_bytes
            time.sleep(1)
            self.download_speed = self.downloaded_bytes - before_downloaded_bytes
            # 计算网速后打印一次
            self.printProcessBar(self.sum_count, self.done_count, 50, True)
        print("")
        return True

    # 4、下载单个ts playlists[index]
    def downloadTs(self, playlist, index):
        succeed = False
        while not succeed:
            # 文件名格式为 "00000001.ts"，index不足8位补充0
            output_path = self.cache_path + "/" + "{0:0>8}.ts".format(index)
            output_fp = open(output_path, "wb+")
            if playlist[index].startswith("http"):
                ts_url = playlist[index]
            else:
                ts_url = self.root_url_path + "/" + playlist[index]
            try:
                response = requests.get(ts_url, timeout=5, headers=headers, stream=True)
                if response.status_code == 200:
                    expected_length = int(response.headers.get('Content-Length'))
                    actual_length = len(response.content)
                    # 累计下载的bytes
                    self.downloaded_bytes += actual_length
                    if expected_length > actual_length:
                        raise Exception("分片下载不完整")
                    output_fp.write(response.content)
                    self.done_count += 1
                    self.printProcessBar(self.sum_count, self.done_count, 50, is_print_download_speed=True)
                    self.logFile.write("\t分片{0:0>8} url = {1} 下载成功！".format(index, ts_url))
                    succeed = True
            except Exception as e:
                self.logFile.write("\t分片{0:0>8} url = {1} 下载失败！正在重试...msg = {2}".format(index, ts_url, e))
            output_fp.close()

    # 5、合并ts
    def mergeTs(self, ts_file_dir, output_file_path, cryptor, count):
        output_fp = open(output_file_path, "wb+")
        for index in range(count):
            self.printProcessBar(count, index + 1, 50)
            self.logFile.write("\t{0}\n".format(index))
            input_file_path = ts_file_dir + "/" + "{0:0>8}.ts".format(index)
            if not os.path.exists(input_file_path):
                print("\n分片{0:0>8}.ts, 不存在，已跳过！".format(index))
                self.logFile.write("分片{0:0>8}.ts, 不存在，已跳过！\n".format(index))
                continue
            input_fp = open(input_file_path, "rb")
            file_data = input_fp.read()
            try:
                if cryptor is None:
                    output_fp.write(file_data)
                else:
                    output_fp.write(cryptor.decrypt(file_data))
            except Exception as e:
                input_fp.close()
                output_fp.close()
                print(e)
                return False
            input_fp.close()
        print("")
        output_fp.close()
        return True

    # 6、删除ts文件
    def removeTsDir(self, ts_file_dir):
        # 先清空文件夹
        for root, dirs, files in os.walk(ts_file_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(ts_file_dir)
        return True

    # 7、convert to mp4（调用了FFmpeg，将合并好的视频内容放置到一个mp4容器中）
    def ffmpegConvertToMp4(self, input_file_path, output_file_path):
        if not os.path.exists(input_file_path):
            print(input_file_path + " 路径不存在！")
            self.logFile.write(input_file_path + " 路径不存在！\n")
            return False
        cmd = r'.\lib\ffmpeg -i "{0}" -vcodec copy -acodec copy "{1}"'.format(input_file_path, output_file_path)
        if sys.platform == "darwin":
            cmd = r'./lib/ffmpeg -i "{0}" -vcodec copy -acodec copy "{1}"'.format(input_file_path, output_file_path)
        if sys.platform == "linux":
            cmd = r'ffmpeg -i "{0}" -vcodec copy -acodec copy "{1}"'.format(input_file_path, output_file_path)
        if os.system(cmd) == 0:
            print(input_file_path + "转换成功！")
            self.logFile.write(input_file_path + "转换成功！\n")
            return True
        else:
            print(input_file_path + "转换失败！")
            self.logFile.write(input_file_path + "转换失败！\n")
            return False

    # 8、模拟输出进度条(默认不打印网速)
    def printProcessBar(self, sum_count, done_count, width, is_print_download_speed=False):
        p_percent = done_count / sum_count
        use_count = int(p_percent * width)
        space_count = int(width - use_count)
        p_percent = p_percent * 100
        if is_print_download_speed:
            # downloadSpeed的单位是B/s, 超过1024*1024转换为MiB/s, 超过1024转换为KiB/s
            if self.download_speed > 1048576:
                print('\r\t{0}/{1} {2}{3} {4:.2f}% {5:>7.2f}MiB/s'.format(sum_count, done_count, use_count * '■',
                                                                          space_count * '□', p_percent,
                                                                          self.download_speed / 1048576),
                      file=sys.stdout, flush=True, end='')
            elif self.download_speed > 1024:
                print('\r\t{0}/{1} {2}{3} {4:.2f}% {5:>7.2f}KiB/s'.format(sum_count, done_count, use_count * '■',
                                                                          space_count * '□', p_percent,
                                                                          self.download_speed / 1024),
                      file=sys.stdout, flush=True, end='')
            else:
                print('\r\t{0}/{1} {2}{3} {4:.2f}% {5:>7.2f}B/s  '.format(sum_count, done_count, use_count * '■',
                                                                          space_count * '□', p_percent,
                                                                          self.download_speed),
                      file=sys.stdout, flush=True, end='')
        else:
            print(
                '\r\t{0}/{1} {2}{3} {4:.2f}%'.format(sum_count, done_count, use_count * '■', space_count * '□',
                                                     p_percent),
                file=sys.stdout, flush=True, end='')

    # m3u8下载器
    def m3u8VideoDownloader(self):
        # 1、下载m3u8
        print("\t1、开始下载m3u8...")
        self.logFile.write("\t1、开始下载m3u8...\n")
        m3u8_info = self.getM3u8Info()
        if m3u8_info is None:
            return False
        ts_list = []
        for playlist in m3u8_info.segments:
            ts_list.append(playlist.uri)
        # 2、获取key
        key_text = ""
        cryptor = None
        # 判断是否加密
        if (len(m3u8_info.keys) != 0) and (m3u8_info.keys[0] is not None):
            # 默认选择第一个key，且AES-128算法
            key = m3u8_info.keys[0]
            if key.method != "AES-128":
                print("\t{0}不支持的解密方式！".format(key.method))
                self.logFile.write("\t{0}不支持的解密方式！\n".format(key.method))
                return False
            # 如果key的url是相对路径，加上m3u8Url的路径
            key_url = key.uri
            if not key_url.startswith("http"):
                # keyUrl = m3u8Url.replace("index.m3u8", keyUrl)
                key_url = self.root_url_path + key_url
            print("\t2、开始下载key...")
            self.logFile.write("\t2、开始下载key...\n")
            key_text = self.getKey(key_url)
            if key_text is None:
                return False
            # 判断是否有偏移量
            if key.iv is not None:
                cryptor = AES.new(bytes(key_text, encoding='utf8'), AES.MODE_CBC, bytes(key.iv, encoding='utf8'))
            else:
                cryptor = AES.new(bytes(key_text, encoding='utf8'), AES.MODE_CBC, bytes(key_text, encoding='utf8'))
        # 3、下载ts
        print("\t3、开始下载ts...")
        self.logFile.write("\t3、开始下载ts...\n")
        # 清空bytes计数器
        self.download_speed = 0
        self.downloaded_bytes = 0

        if self.multiDownloadTs(ts_list):
            self.logFile.write("\tts下载完成---------------------\n")
        # 4、合并ts
        print("\t4、开始合并ts...")
        self.logFile.write("\t4、开始合并ts...\n")
        if self.mergeTs(self.cache_path, self.cache_path + "/cache.flv", cryptor, len(ts_list)):
            self.logFile.write("\tts合并完成---------------------\n")
        else:
            print(key_text)
            print("\tts合并失败！")
            self.logFile.write("\tts合并失败！\n")
            return False
        # 5、开始转换成mp4
        print("\t5、开始mp4转换...")
        self.logFile.write("\t5、开始mp4转换...\n")
        if not self.ffmpegConvertToMp4(self.cache_path + "/cache.flv",
                                       saveRootDirPath + "/" + self.title + ".mp4"):
            return False
        return True

    def run(self):
        # 判断m3u8文件是否存在
        if not (os.path.exists(m3u8InputFilePath)):
            print("{0}文件不存在！".format(m3u8InputFilePath))
            exit(0)
        # 如果输出目录不存在就创建
        if not (os.path.exists(saveRootDirPath)):
            os.mkdir(saveRootDirPath)

        # 如果记录错误文件不存在就创建
        if not (os.path.exists(errorM3u8InfoDirPath)):
            open(errorM3u8InfoDirPath, 'w+')

        m3u8_input_fp = open(m3u8InputFilePath, "r", encoding="utf-8")
        # 设置error的m3u8 url输出
        error_m3u8_info_fp = open(errorM3u8InfoDirPath, "a+", encoding="utf-8")

        while True:
            row_data = m3u8_input_fp.readline()
            row_data = row_data.strip('\n')
            if row_data == "":
                break
            m3u8_info = row_data.split('|')
            title = m3u8_info[0]
            self.m3u8_url = m3u8_info[1]

            # title中去除 \  /  :  *  ?  "  <  >  |字符，Windows系统中文件命名不能包含这些字符
            title = title.replace('\\', ' ', sys.maxsize)
            title = title.replace('/', ' ', sys.maxsize)
            title = title.replace(':', ' ', sys.maxsize)
            title = title.replace('*', ' ', sys.maxsize)
            title = title.replace('?', ' ', sys.maxsize)
            title = title.replace('"', ' ', sys.maxsize)
            title = title.replace('<', ' ', sys.maxsize)
            title = title.replace('>', ' ', sys.maxsize)
            title = title.replace('|', ' ', sys.maxsize)
            self.title = title
            try:
                print("{0} 开始下载:".format(m3u8_info[0]))
                self.logFile.write("{0} 开始下载:\n".format(m3u8_info[0]))
                if self.m3u8VideoDownloader():
                    # 成功下载完一个m3u8则清空logFile
                    self.logFile.seek(0)
                    self.logFile.truncate()
                    print("{0} 下载成功！".format(m3u8_info[0]))
                else:
                    error_m3u8_info_fp.write(title + "," + self.m3u8_url + '\n')
                    error_m3u8_info_fp.flush()
                    print("{0} 下载失败！".format(m3u8_info[0]))
                    self.logFile.write("{0} 下载失败！\n".format(m3u8_info[0]))
            except Exception as exception:
                print(exception)
                traceback.print_exc()
        # 关闭文件
        self.logFile.close()
        m3u8_input_fp.close()
        error_m3u8_info_fp.close()
        print("----------------下载结束------------------")


if __name__ == '__main__':
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Connection": "Keep-Alive",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36"
    }

    # m3u8链接批量输入文件(必须是utf-8编码)
    m3u8InputFilePath = "/home/hdc/Videos/m3u8_input.txt"
    # 设置视频保存路径
    saveRootDirPath = "/home/hdc/Videos"
    # 下载出错的m3u8保存文件
    errorM3u8InfoDirPath = "/home/hdc/Videos/error.txt"
    # m3u8文件、key文件下载尝试次数，ts流默认无限次尝试下载，直到成功
    m3u8TryCountConf = 10
    # 线程数（同时下载的分片数）
    processCountConf = 50
    M3u8Downloader().run()
