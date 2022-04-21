# coding:utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2021 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import datetime
import json
import re
import time
import pymongo
import pandas as pd
import tushare as ts
from QUANTAXIS.QAUtil import Parallelism_Thread
from QUANTAXIS.QAUtil.QACache import QA_util_cache
import QUANTAXIS as QA
from multiprocessing import cpu_count

from QUANTAXIS.QAFetch.QATushare import (
    QA_fetch_get_index_basic,
    QA_fetch_get_index_weight,
    # QA_fetch_get_index_day,
    QA_fetch_get_stock_day,
    QA_fetch_get_stock_info,
    QA_fetch_get_stock_list,
    QA_fetch_get_stock_block,
    QA_fetch_get_trade_date,
    QA_fetch_get_lhb,
)
from QUANTAXIS.QAUtil import (
    QA_util_date_stamp,
    QA_util_log_info,
    QA_util_log_error,
    QA_util_time_stamp,
    QA_util_to_json_from_pandas,
    trade_date_sse,
    QA_util_get_real_date,
    QA_util_get_last_day,
    QA_util_get_next_day
)
from QUANTAXIS.QAUtil.QASetting import DATABASE

import tushare as QATs


def date_conver_to_new_format(date_str):
    time_now = time.strptime(date_str[0:10], '%Y-%m-%d')
    return '{:0004}{:02}{:02}'.format(
        int(time_now.tm_year),
        int(time_now.tm_mon),
        int(time_now.tm_mday)
    )


def get_coll(client=None):
    cache = QA_util_cache()
    results = cache.get('tushare_coll')
    if results:
        return results
    else:
        _coll = client.stock_day_ts
        _coll.create_index(
            [('code',
              pymongo.ASCENDING),
             ('date_stamp',
              pymongo.ASCENDING)]
        )
        cache.set('tushare_coll', _coll, age=86400)
        return _coll

# TODO: 和sav_tdx.py中的now_time一起提取成公共函数


def now_time():
    real_date = str(QA_util_get_real_date(str(datetime.date.today() -
                                              datetime.timedelta(days=1)),
                                          trade_date_sse, -1))
    str_now = real_date + ' 17:00:00' if datetime.datetime.now().hour < 15 \
        else str(QA_util_get_real_date(str(datetime.date.today()),
                                       trade_date_sse, -1)) + ' 15:00:00'

    return date_conver_to_new_format(str_now)


def QA_SU_save_index_basic(client=DATABASE, ui_log=None, ui_progress=None):
    index_list = QA_util_to_json_from_pandas(QA_fetch_get_index_basic())
    coll_index_list = client.index_basic
    coll_index_list.create_index("code", unique=True)

    for bs in index_list:
        coll_index_list.update_one({'code': bs['code']}, {
                                   '$setOnInsert': bs}, upsert=True)


def QA_SU_save_index_weight(client=DATABASE, ui_log=None, ui_progress=None):
    index_list = QA.QA_fetch_index_basic_adv()
    # 这里要过滤掉没有数据的指数，省得每次都从头开始更新无数据的指数
    index_list = index_list[index_list.nodata != True]

    def _fetch_data(c, s, e):
        index_weights = []
        interval = pd.date_range(s, e,
                                 freq=pd.tseries.offsets.MonthBegin(1)).strftime("%Y%m%d").tolist()

        for i in range(len(interval) - 1):
            QA_util_log_info("{} 获取数据{} {} 长度: {}".format(
                c, interval[i], interval[i+1], len(index_weights)))
            index_weight = QA_fetch_get_index_weight(
                c, interval[i], interval[i+1])
            if not index_weight.empty:
                index_weight['code'] = c.split('.')[0]
                index_weights.append(index_weight)
            else:
                QA_util_log_info("{} 为空 {} {}".format(
                    c, interval[i], interval[i+1]))

            time.sleep(0.08)

        if len(index_weights):
            data = pd.concat(index_weights)
            client.index_weight.create_index(
                [("code", pymongo.ASCENDING), ("trade_date", pymongo.ASCENDING)])

            try:
                client.index_weight.insert_many(
                    QA_util_to_json_from_pandas(data),
                    ordered=False
                )
            except:
                QA_util_log_error('保存数据库数据出错 {} {}'.format(c))
                pass

    for code in index_list.index:
        index_code = index_list.loc[code]['ts_code']
        search_cond = {'code': code}
        ref = client.index_weight.find(search_cond, {'_id': 0, 'index_code': 1, 'trade_date': 1}).sort(
            'trade_date', pymongo.ASCENDING)
        ref_count = client.index_weight.count_documents(search_cond)
        end_date = datetime.date.today().strftime('%Y%m%d')

        if ref_count > 0:
            last_day = ref[ref_count - 1]['trade_date']
            QA_util_log_info('{} 有数据{} {}'.format(
                index_code, ref_count, last_day))
        else:
            QA_util_log_info('{} 没有数据, 从头开始获取'.format(index_code))
            # tushare 貌似只有2011-9月份之后的数据
            last_day = index_list.loc[code]['list_date']
            if not last_day:
                QA_util_log_info('{} 没有list_date, 跳过'.format(index_code))
                continue
            if last_day < '20110830':
                last_day = '20110830'

        _fetch_data(index_code, last_day, end_date)


def QA_save_stock_day_all(client=DATABASE):
    stock_list = QA_fetch_get_stock_list()
    __coll = client.stock_day
    __coll.create_index('code')

    def saving_work(i):
        QA_util_log_info('Now Saving ==== %s' % (i))
        try:
            data_json = QA_fetch_get_stock_day(i, start='1990-01-01')

            __coll.insert_many(data_json)
        except Exception as e:
            print(e)
            QA_util_log_info('error in saving ==== %s' % str(i))

    for i_ in range(len(stock_list)):
        QA_util_log_info('The %s of Total %s' % (i_, len(stock_list)))
        QA_util_log_info(
            'DOWNLOAD PROGRESS %s ' %
            str(float(i_ / len(stock_list) * 100))[0:4] + '%'
        )
        saving_work(stock_list[i_])

    # saving_work('hs300')
    # saving_work('sz50')


def QA_SU_save_stock_list(client=DATABASE):
    data = QA_fetch_get_stock_list()
    date = str(datetime.date.today())
    date_stamp = QA_util_date_stamp(date)
    coll = client.stock_info_tushare
    coll.insert(
        {
            'date': date,
            'date_stamp': date_stamp,
            'stock': {
                'code': data
            }
        }
    )


def QA_SU_save_stock_terminated(client=DATABASE):
    '''
    获取已经被终止上市的股票列表，数据从上交所获取，目前只有在上海证券交易所交易被终止的股票。
    collection：
        code：股票代码 name：股票名称 oDate:上市日期 tDate:终止上市日期
    :param client:
    :return: None
    '''

    # 🛠todo 已经失效从wind 资讯里获取
    # 这个函数已经失效
    print("！！！ tushare 这个函数已经失效！！！")
    df = QATs.get_terminated()
    #df = QATs.get_suspended()
    print(
        " Get stock terminated from tushare,stock count is %d  (终止上市股票列表)" %
        len(df)
    )
    coll = client.stock_terminated
    client.drop_collection(coll)
    json_data = json.loads(df.reset_index().to_json(orient='records'))
    coll.insert(json_data)
    print(" 保存终止上市股票列表 到 stock_terminated collection， OK")


def QA_SU_save_stock_info_tushare(client=DATABASE):
    '''
        获取 股票的 基本信息，包含股票的如下信息

        code,代码
        name,名称
        industry,所属行业
        area,地区
        pe,市盈率
        outstanding,流通股本(亿)
        totals,总股本(亿)
        totalAssets,总资产(万)
        liquidAssets,流动资产
        fixedAssets,固定资产
        reserved,公积金
        reservedPerShare,每股公积金
        esp,每股收益
        bvps,每股净资
        pb,市净率
        timeToMarket,上市日期
        undp,未分利润
        perundp, 每股未分配
        rev,收入同比(%)
        profit,利润同比(%)
        gpr,毛利率(%)
        npr,净利润率(%)
        holders,股东人数

        add by tauruswang

    在命令行工具 quantaxis 中输入 save stock_info_tushare 中的命令
    :param client:
    :return:
    '''
    df = QATs.get_stock_basics()
    print(" Get stock info from tushare,stock count is %d" % len(df))
    coll = client.stock_info_tushare
    client.drop_collection(coll)
    json_data = json.loads(df.reset_index().to_json(orient='records'))
    coll.insert(json_data)
    print(" Save data to stock_info_tushare collection， OK")


def QA_SU_save_trade_date_all(client=DATABASE):
    data = QA_fetch_get_trade_date('', '')
    coll = client.trade_date
    coll.insert_many(data)


def QA_SU_save_stock_info(client=DATABASE):
    data = QA_fetch_get_stock_info('')
    client.drop_collection('stock_info')
    coll = client.stock_info
    coll.create_index('code')
    coll.insert_many(QA_util_to_json_from_pandas(data.reset_index()))


def QA_save_stock_day_all_bfq(client=DATABASE):
    df = QA_fetch_get_stock_list()

    __coll = client.stock_day_bfq
    __coll.ensure_index('code')

    def saving_work(i):
        QA_util_log_info('Now Saving ==== %s' % (i))
        try:
            df = QA_fetch_get_stock_day(i, start='1990-01-01', if_fq='bfq')

            __coll.insert_many(json.loads(df.to_json(orient='records')))
        except Exception as e:
            print(e)
            QA_util_log_info('error in saving ==== %s' % str(i))

    for i_ in range(len(df)):
        QA_util_log_info('The %s of Total %s' % (i_, len(df)))
        QA_util_log_info(
            'DOWNLOAD PROGRESS %s ' %
            str(float(i_ / len(df) * 100))[0:4] + '%'
        )
        saving_work(df[i_])

    # saving_work('hs300')
    # saving_work('sz50')


def QA_save_stock_day_with_fqfactor(client=DATABASE):
    df = QA_fetch_get_stock_list()

    __coll = client.stock_day
    __coll.ensure_index('code')

    def saving_work(i):
        QA_util_log_info('Now Saving ==== %s' % (i))
        try:
            data_hfq = QA_fetch_get_stock_day(
                i,
                start='1990-01-01',
                if_fq='bfq',
                type_='pd'
            )
            data_json = QA_util_to_json_from_pandas(data_hfq)
            __coll.insert_many(data_json)
        except Exception as e:
            print(e)
            QA_util_log_info('error in saving ==== %s' % str(i))

    for i_ in range(len(df)):
        QA_util_log_info('The %s of Total %s' % (i_, len(df)))
        QA_util_log_info(
            'DOWNLOAD PROGRESS %s ' %
            str(float(i_ / len(df) * 100))[0:4] + '%'
        )
        saving_work(df[i_])

    # saving_work('hs300')
    # saving_work('sz50')

    QA_util_log_info('Saving Process has been done !')
    return 0


def QA_save_lhb(client=DATABASE):
    __coll = client.lhb
    __coll.ensure_index('code')

    start = datetime.datetime.strptime("2006-07-01", "%Y-%m-%d").date()
    end = datetime.date.today()
    i = 0
    while start < end:
        i = i + 1
        start = start + datetime.timedelta(days=1)
        try:
            pd = QA_fetch_get_lhb(start.isoformat())
            if pd is None:
                continue
            data = pd\
                .assign(pchange=pd.pchange.apply(float))\
                .assign(amount=pd.amount.apply(float))\
                .assign(bratio=pd.bratio.apply(float))\
                .assign(sratio=pd.sratio.apply(float))\
                .assign(buy=pd.buy.apply(float))\
                .assign(sell=pd.sell.apply(float))
            # __coll.insert_many(QA_util_to_json_from_pandas(data))
            for i in range(0, len(data)):
                __coll.update(
                    {
                        "code": data.iloc[i]['code'],
                        "date": data.iloc[i]['date']
                    },
                    {"$set": QA_util_to_json_from_pandas(data)[i]},
                    upsert=True
                )
            time.sleep(2)
            if i % 10 == 0:
                time.sleep(60)
        except Exception as e:
            print("error codes:")
            time.sleep(2)
            continue


class QA_SU_save_day_parallelism_thread(Parallelism_Thread):
    def __init__(self, processes=cpu_count(), client=DATABASE, ui_log=None,
                 ui_progress=None):
        super(QA_SU_save_day_parallelism_thread, self).__init__(processes)
        self.client = client
        self.ui_log = ui_log
        self.ui_progress = ui_progress
        self.err = []
        self.__total_counts = 0
        self.__code_counts = 0

    @property
    def code_counts(self):
        return self.__code_counts

    @code_counts.setter
    def code_counts(self, value):
        self.__code_counts = value

    @property
    def total_counts(self):
        return self.__total_counts

    @total_counts.setter
    def total_counts(self, value):
        if value > 0:
            self.__total_counts = value
        else:
            raise Exception('value must be great than zero.')

    def loginfo(self, code, listCounts=10):
        if len(self._loginfolist) < listCounts:
            self._loginfolist.append(code)
        else:
            str = ''
            for i in range(len(self._loginfolist)):
                str += + self._loginfolist[i] + ' '
            str += code
            QA_util_log_info(
                '##JOB02 Now Saved ==== {}'.format(
                ),
                self.ui_log
            )
            self._loginfolist.clear()


class QA_SU_save_stock_day_parallelism(QA_SU_save_day_parallelism_thread):
    def __saving_work(self, code):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving STOCK_DAY==== {}'.format(str(code)),
                self.ui_log
            )

            time.sleep(0.2)

            # 首选查找数据库 是否 有 这个代码的数据
            search_cond = {'code': str(code)[0:6]}
            ref = get_coll(self.client).find(search_cond)
            ref_count = get_coll(self.client).count_documents(search_cond)
            end_date = now_time()

            # 当前数据库已经包含了这个代码的数据， 继续增量更新
            # 加入这个判断的原因是因为如果股票是刚上市的 数据库会没有数据 所以会有负索引问题出现
            if ref_count > 0:
                # 接着上次获取的日期继续更新
                start_date_new_format = ref[ref_count - 1]['trade_date']
                start_date = ref[ref_count - 1]['date']

                QA_util_log_info(
                    'UPDATE_STOCK_DAY: Trying updating {} {} from {} to {}'
                    .format(ref_count, code,
                            start_date_new_format,
                            end_date),
                    self.ui_log
                )

                if start_date_new_format != end_date:
                    df = QA_fetch_get_stock_day(
                        str(code),
                        date_conver_to_new_format(
                            QA_util_get_next_day(start_date)
                        ),
                        end_date,
                        'bfq'
                    )
                    if not (df is None) and len(df) > 0:
                        get_coll(self.client).insert_many(
                            QA_util_to_json_from_pandas(df)
                        )
                    else:
                        QA_util_log_info(
                            'UPDATE_STOCK_DAY: Trying updating {} {} from {} to {} ---- No Data'
                            .format(ref_count, code,
                                    start_date_new_format,
                                    end_date),
                            self.ui_log
                        )

            # 当前数据库中没有这个代码的股票数据， 从1990-01-01 开始下载所有的数据
            # 一次只返回 5000 条，所以有些股票得获取两次
            else:
                start_date = '19900101'
                QA_util_log_info(
                    'FETCH_ALL_STOCK_DAY: Trying fetching all data {} from {} to {}'
                    .format(code,
                            start_date,
                            end_date),
                    self.ui_log
                )
                if start_date != end_date:
                    # 第一次获取
                    df = QA_fetch_get_stock_day(
                        str(code),
                        start_date,
                        end_date,
                        'bfq'
                    )

                    if not (df is None):
                        if len(df) >= 5000:
                            again_date = QA_util_get_last_day(df['date'].min())
                            QA_util_log_info(
                                'FETCH_ALL_STOCK_DAY: Trying updating again {} from {} to {}'
                                .format(code,
                                        start_date,
                                        again_date),
                                self.ui_log
                            )
                            df2 = QA_fetch_get_stock_day(
                                str(code),
                                start_date,
                                again_date,
                                'bfq'
                            )

                            df = df.append(df2)

                        get_coll(self.client).insert_many(
                            QA_util_to_json_from_pandas(df)
                        )
                    else:
                        QA_util_log_info(
                            'FETCH_ALL_STOCK_DAY: Trying fetching all data {} from {} to {} ---- No Data'
                            .format(code,
                                    start_date,
                                    end_date),
                            self.ui_log
                        )
        except Exception as e:
            print(e)
            self.err.append(str(code))

    def do_working(self, code):
        if self.total_counts > 0:
            self.code_counts += 1
            QA_util_log_info(
                'The {} of Total {}'.format(self.code_counts,
                                            self.total_counts),
                ui_log=self.ui_log
            )
            strLogProgress = 'DOWNLOAD PROGRESS {0:.2f}% '.format(
                self.code_counts / self.total_counts * 100
            )

            intLogProgress = int(
                float(self.code_counts / self.total_counts * 10000.0))
            QA_util_log_info(
                strLogProgress,
                ui_log=self.ui_log,
                ui_progress=self.ui_progress,
                ui_progress_int_value=intLogProgress
            )
        self.__saving_work(code)
        return code

    def complete(self, result):
        if len(self.err) < 1:
            QA_util_log_info('SUCCESS', ui_log=self.ui_log)
        else:
            QA_util_log_info('ERROR CODE: ', ui_log=self.ui_log)
            QA_util_log_info(self.err, ui_log=self.ui_log)


max_processes = 8  # 第一次获取可以用全部的32线程，后续增量更新就用8个线程好了


def QA_SU_save_stock_day(client=DATABASE, ui_log=None, ui_progress=None):
    '''
     save stock_day
    保存日线数据
    :param client:
    :param ui_log:  给GUI qt 界面使用
    :param ui_progress: 给GUI qt 界面使用
    :param ui_progress_int_value: 给GUI qt 界面使用
    '''
    stock_list = QA_fetch_get_stock_list()
    num_stocks = len(stock_list)

    ps = QA_SU_save_stock_day_parallelism(
        processes=max_processes if num_stocks >= max_processes else num_stocks,
        client=client, ui_log=ui_log)

    ps.total_counts = num_stocks
    ps.run(stock_list)


def QA_SU_save_stock_block(client=DATABASE, ui_log=None, ui_progress=None):
    """
    Tushare的版块数据

    Returns:
        [type] -- [description]
    """
    coll = client.stock_block
    coll.create_index('code')
    try:
        # 暂时先只有中证500
        csindex500 = QA_fetch_get_stock_block()
        coll.insert_many(
            QA_util_to_json_from_pandas(csindex500))
        QA_util_log_info('SUCCESS save stock block ^_^', ui_log)
    except Exception as e:
        QA_util_log_info('ERROR CODE: ', ui_log)
        QA_util_log_info(e, ui_log)


if __name__ == '__main__':
    from pymongo import MongoClient
    client = MongoClient('localhost', 27017)
    db = client['quantaxis']
    # QA_SU_save_stock_day(client=db)
    QA_SU_save_index_weight(client=db)
    # print(QA_fetch_get_stock_day('000001.SZ', start='20211021'))
