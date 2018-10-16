# coding = utf-8
import re
from urllib.request import urlretrieve
import requests
from bs4 import BeautifulSoup
import lxml

"""
download images from pixabay in one page 
"""

# browser header
headers = {
    'Cookie': 'isp=true; isp=true; ctid=25; aQQ_ajkguid=56732C03-B40C-F61A-8414-SX1016093915; sessid=9FD6F823-6099-C14E-C454-SX1016093915; isp=true; lps=http%3A%2F%2Fuser.anjuke.com%2Fajax%2FcheckMenu%2F%3Fr%3D0.9737112057446529%26callback%3DjQuery1113029601356449765337_1539653955590%26_%3D1539653955591%7Chttps%3A%2F%2Fwx.fang.anjuke.com%2Fwuye%2F; twe=2; Hm_lvt_c5899c8768ebee272710c9c5f365a6d8=1539653956; 58tj_uuid=684ee43a-4409-40d9-aa07-640d3d58e750; new_uv=1; als=0; Hm_lvt_1cf880de4bc3c11500482f152b3353c0=1539653963; Hm_lpvt_c5899c8768ebee272710c9c5f365a6d8=1539654548; Hm_lpvt_1cf880de4bc3c11500482f152b3353c0=1539654556',
    'User_Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'
}


def getHtml(url):
    html = requests.get(url, headers=headers)
    return html


def getImg(html):
    """
    get url:image.jpg from html by Regular expression

    :param html: html
    :return [urls]
    """
    reg = '(https.+?\.jpg){1}'
    imgre = re.compile(reg)
    imglist = re.findall(imgre, html.text)

    x = 0
    for imgurl in imglist:
        # avoid too lang
        if len(imgurl) < 100:
            x += 1
            print(imgurl)
    print('total image:', x)
    return imglist


def download(imgadresses):
    """
    download images to local disk

    :param imgadresses: local directory
    :return:
    """
    localPath = r"D://爬虫/download/"
    baseName = 'image_'
    x = 0
    for adress in imgadresses:
        try:
            urlretrieve(adress, localPath + baseName + str(x) + '.jpg')
            x += 1
        except:
            print('下载失败')


if __name__ == '__main__':
    print("fill in path and cookie")

    urls = ["https://wx.fang.anjuke.com/wuye/p{}/".format(number) for number in range(1, 2)]

    print(urls)

    for url in urls:
        html = getHtml(url)
        soup = BeautifulSoup(html.text, 'lxml')

        print(soup)

        communityName = soup.select('a > h3')

        print(communityName)
