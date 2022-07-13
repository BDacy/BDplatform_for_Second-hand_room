import csv
from lxml import etree
import requests
import pandas


def set_header(path):
    df = pandas.read_csv(path, encoding='utf-8', header=None,
                         names=['总价', '平米单价', '小区', '所在区', '户型', '楼层', '面积', '户型结构', '建筑类型', '朝向', '建筑结构', '装修情况',
                                '梯户比例', '电梯与否', '发布年', '发布月', '发布日'])
    df = df.reset_index()
    df.to_csv(path, encoding='utf-8', index=False)


def empty_dic(dic):
    flag = True
    for key, value in dic.items():
        if not value:
            flag = False
            break
    return flag


def set_parameters():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.80 Safari/537.36 Edg/98.0.1108.43'
    }
    fieldnames = ['zong_jia', 'jiage_meiping', 'wei_zhi_0', 'wei_zhi_1', 'fangwu_huxing',
                  'suozai_louceng', 'jianzhu_mianji', 'huxing_jiegou', 'jianzhu_leixing',
                  'fangwu_chaoxiang', 'jianzhu_jiegou', 'zhuangxiu_qingkuang', 'tihu_bili',
                  'peibei_dianti', 'guapai_nian', 'guapai_yue', 'guapai_ri']
    parameters = {
        'headers': headers,
        'fieldnames': fieldnames
    }
    return parameters


def parse_data(li):
    href = li.xpath('./div[@class="info clear"]/div[@class="title"]/a/@href')[0]
    detail_response = requests.get(url=href, headers=parameters['headers'])
    detail_page_text = detail_response.text
    detail_tree = etree.HTML(detail_page_text)

    zhuangxiu_qingkuang = detail_tree.xpath('//div[@class="base"]/div[@class="content"]/ul/li[9]/text()')

    zong_jia = detail_tree.xpath('//div[@class="price "]/span[1]/text()')

    jiage_meiping = detail_tree.xpath(
        '//div[@class="price "]//span[@class="unitPriceValue"]/text()')

    wei_zhi_0 = detail_tree.xpath('//div[@class="communityName"]/a/text()')

    wei_zhi_1 = detail_tree.xpath(
        '//div[@class="areaName"]/span[@class="info"]/a[1]/text()')

    fangwu_huxing = detail_tree.xpath('//div[@class="base"]/div[@class="content"]/ul/li[1]/text()')
    suozai_louceng = detail_tree.xpath('//div[@class="base"]/div[@class="content"]/ul/li[2]/text()')
    jianzhu_mianji = detail_tree.xpath('//div[@class="base"]/div[@class="content"]/ul/li[3]/text()')
    huxing_jiegou = detail_tree.xpath('//div[@class="base"]/div[@class="content"]/ul/li[4]/text()')
    jianzhu_leixing = detail_tree.xpath('//div[@class="base"]/div[@class="content"]/ul/li[6]/text()')
    fangwu_chaoxiang = detail_tree.xpath('//div[@class="base"]/div[@class="content"]/ul/li[7]/text()')
    jianzhu_jiegou = detail_tree.xpath('//div[@class="base"]/div[@class="content"]/ul/li[8]/text()')
    tihu_bili = detail_tree.xpath('//div[@class="base"]/div[@class="content"]/ul/li[10]/text()')
    peibei_dianti = detail_tree.xpath('//div[@class="base"]/div[@class="content"]/ul/li[11]/text()')

    guapai_shijian = detail_tree.xpath(
        '//div[@class="transaction"]/div[2]/ul/li[1]/span[2]/text()')

    dic = {'zong_jia': zong_jia, 'jiage_meiping': jiage_meiping,
           'wei_zhi_0': wei_zhi_0, 'wei_zhi_1': wei_zhi_1,
           'fangwu_huxing': fangwu_huxing, 'suozai_louceng': suozai_louceng,
           'jianzhu_mianji': jianzhu_mianji,
           'huxing_jiegou': huxing_jiegou, 'jianzhu_leixing': jianzhu_leixing,
           'fangwu_chaoxiang': fangwu_chaoxiang, 'jianzhu_jiegou': jianzhu_jiegou,
           'zhuangxiu_qingkuang': zhuangxiu_qingkuang, 'tihu_bili': tihu_bili,
           'peibei_dianti': peibei_dianti,
           'guapai_shijian': guapai_shijian, }

    return dic


def write_to_csv(dic, f):
    writer = csv.DictWriter(f, fieldnames=parameters['fieldnames'])
    writer.writerow(dic)


def crawl():
    for i in range(1, 5):
        print('爬取第%d页' % i)
        url = 'https://cd.lianjia.com/ershoufang/pg%dco32' % i
        response = requests.get(url=url, headers=parameters['headers'])
        page_text = response.text
        tree = etree.HTML(page_text)
        li_list = tree.xpath('//ul[@class="sellListContent"]/li')
        for li in li_list:
            dic = parse_data(li)
            if not empty_dic(dic):
                continue
            dic = {
                'zong_jia': dic['zong_jia'][0],
                'jiage_meiping': dic['jiage_meiping'][0], 'wei_zhi_0': dic['wei_zhi_0'][0],
                'wei_zhi_1': dic['wei_zhi_1'][0], 'fangwu_huxing': dic['fangwu_huxing'][0],
                'suozai_louceng': dic['suozai_louceng'][0], 'jianzhu_mianji': dic['jianzhu_mianji'][0],
                'huxing_jiegou': dic['huxing_jiegou'][0], 'jianzhu_leixing': dic['jianzhu_leixing'][0],
                'fangwu_chaoxiang': dic['fangwu_chaoxiang'][0], 'jianzhu_jiegou': dic['jianzhu_jiegou'][0],
                'zhuangxiu_qingkuang': dic['zhuangxiu_qingkuang'][0], 'tihu_bili': dic['tihu_bili'][0],
                'peibei_dianti': dic['peibei_dianti'][0],
                'guapai_nian': dic['guapai_shijian'][0].split('-')[0],
                'guapai_yue': dic['guapai_shijian'][0].split('-')[1],
                'guapai_ri': dic['guapai_shijian'][0].split('-')[2]
            }
            write_to_csv(dic, f)
        print('第%d页over!' % i)


if __name__ == "__main__":
    parameters = set_parameters()
    path = './new_results.csv'
    f = open(path, 'w', newline='', encoding='utf-8')

    crawl()
    f.close()
    print('全部结束！')
    set_header('./new_results.csv')
