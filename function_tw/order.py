import pandas as pd
import time,json,calendar,os
import queue,argparse
from datetime import datetime,timedelta
import requests as rs
import itertools
import numpy as np
from enum import Enum
from hashlib import sha256
#from flask import Flask, request, jsonify
#環境domain
#domain = 'http://pre2.top.one'

#domain = 'http://dev.top.one'
#domain = 'http://test.top.one'
# 定义文件路径
ospath = os.path.abspath(os.path.dirname(os.getcwd()))
parser = argparse.ArgumentParser(description='參數專用')
parser.add_argument('--divisor', type=int, default=10, help='用於起跑速度')
parser.add_argument('--env', type=str, default='test', help='用於要跑的環境')
parser.add_argument('--donetime', type=str, default='15:00:00', help='用於前置處理時間監聽')
parser.add_argument('--filename', type=str, default='2024-08-13', help='文件名稱')
args = parser.parse_args()
domain = 'http://' + args.env + '.top.one'
now = datetime.now()
dataetime = now.strftime('%H%M%S')
#報告路徑
#/qa-simulated-trade-python
#file_path = ospath +'/qa-simulated-trade-python/outputCSV/' + str(args.filename) + '_output_tw.csv'
file_path = ospath +'/outputCSV/' + str(datetime.now().strftime('%Y-%m-%d')) + '_output_tw.csv'
log_file_name = str(datetime.now().strftime('%Y-%m-%d_%H%M%S')) + '_logs_tw.txt'
debug_file_name = str(datetime.now().strftime('%Y-%m-%d_%H%M%S')) + '_DEBUG_logs_tw.txt'
log_path = ospath + '/logs/' + log_file_name
debuglog_path = ospath + '/logs/' + debug_file_name
#報告log參數
logReport_name_txt = ospath + '/logs/ReportLogName_tw.txt' 
logReportFile = open(logReport_name_txt,'w')
logReportFile.write(log_file_name)
logReportFile.close()
#參數
orderData = []
filelist = []
debuglist = []
global open_pass_count
global open_fail_count
global close_count

class soucreExcelCol(Enum):
    tradeTime = 0,      #合约委托时间
    uid = 1,            #UID
    pair = 2,           #合約交易對
    posType = 3,        #倉位類型
    leverage = 4,       #槓桿倍數
    price = 5,          #委託單價
    margin = 6,         #保證金金額
    dealPrice = 7,      #成交单价
    taker = 8,          #匹配Taker委託單號
    maker = 9,          #匹配Taker持倉單號
    newAccount = 10,    #newAccount
    toponeUID = 11,     #toponeUID
    posID = 12,         #倉位單號
    take_profit = 13,   #止盈價格
    stop_profit = 14,   #止損價格
    orderType = 15,     #訂單類型
    orderID = 16        #委託單號

secret = 'hGTBVUeYv' #csrf 密鑰
entrust_default_body = {
        "uid": "",
        "pair": "",
        "side": "",
        "position_side": "",
        "quantity": "0",
        "margin": "",
        "leverage": 10,
        "take_profit_price": '',
        "stop_loss_price": '',
        "price": "0",
        "is_simulate": 2
}
def check_csv_against_current_time():
    open_pass_count = 0
    open_fail_count = 0
    close_count = 0
    df = pd.read_csv(file_path)
    #這邊要修改
    cdTime = timedelta(hours=-0,minutes=-0) #縮短時間 +往後 -往前
    magnTime = args.divisor
    #timed = timedelta(seconds=3)
    start_datetime = datetime.strptime(df['合約委託時間'].iloc[0], '%Y-%m-%d %H:%M:%S') - cdTime #開始時間減掉要縮短的時間
    end_datetime = datetime.strptime(df['合約委託時間'].iloc[-1], '%Y-%m-%d %H:%M:%S') - cdTime #結束時間減掉要縮短的時間
    #today = datetime.now().date()
    #base_datetime = datetime.combine(start_datetime.date(), datetime.min.time())
    print('=========================',flush=True)
    print('資料第一筆下單開始時間：',start_datetime,flush=True)
    print('資料最後一筆結束時間：',end_datetime,flush=True)

    #這邊要修改
    #抓取時間，第一筆資料預設把個位數字刪減去
    current_datetime = (start_datetime + timedelta(minutes=-int(start_datetime.minute % 10),seconds=-int(start_datetime.second))).strftime("%Y-%m-%d %H:%M:%S") #開始時間前置作業
    #========
    current_datetime = datetime.strptime(current_datetime,'%Y-%m-%d %H:%M:%S')
    print('加速開始時間：',current_datetime,flush=True)
    print('=========================',flush=True)
    #倒數計時
    #######
    #done = datetime.strptime(args.donetime,'%H:%M:%S') #前置時間結束後，開始進入加速
    #倒數計時
    timeCount = 0
    print('會在：' + str(args.donetime) + '開始執行！！！',flush=True)
    while True:
        timeCount += 1
        now = datetime.now().strftime('%H:%M:%S')
        if timeCount == 10:
            timeCount = 0
            print('現在時間：',now,flush=True)
        if now == args.donetime:
            break
        time.sleep(1)
    print('=========================',flush=True)
    if args.divisor == 1:
        print('一倍速，啟動中',flush=True)
    else:
        print('現在開始加速',flush=True)
    timenow = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    timenow = datetime.strptime(timenow,'%Y-%m-%d %H:%M:%S') #前置時間開始計算
    tmp = current_datetime  
    array = np.array(df.values)
    #######
    #進入開始加速時間，時間到資料的最後一筆
    while current_datetime <= end_datetime:
        f = open(log_path,'a',encoding='utf-8')
        #q = queue.Queue()
        # row_iterator = iter(df.iterrows())
        # row_iterator = itertools.dropwhile(lambda x: x[0] <= last_index, row_iterator)
    #     # 过滤出时间列等于当前时间的行
        #print(len(array))
        current_datetime = tmp + timedelta(seconds=(datetime.now() - timenow ).seconds * magnTime)
        #f.write('當下時間:' + str(current_datetime) + '\n')
        condition = (array[:, 0] == str(current_datetime))
        #print(condition)
        matching_lists = array[condition]
        #print(matching_lists)
        if matching_lists.size > 0:
            debuglist.append(str(matching_lists))
            for  data in matching_lists:
                print('=========================',flush=True)
                if '開多' in data:
                    print('模擬時間：' + str(current_datetime),flush=True)
                    threading.Thread(target=trad_api, args=(data,)).start()
                    print('用戶' + str(matching_lists[0][11]) + '已開多',flush=True)
                    print('下單後，系統時間：' + str(datetime.now().strftime('%H:%M:%S')),flush=True)
                    open_pass_count += 1
                if '開空' in data:
                    print('模擬時間：' + str(current_datetime),flush=True)
                    threading.Thread(target=trad_api, args=(data,)).start()
                    print('用戶' + str(matching_lists[0][11]) + '已開空',flush=True)
                    print('下單後，系統時間：' + str(datetime.now().strftime('%H:%M:%S')),flush=True)
                    open_pass_count += 1
                if '平多' in data:
                    print('模擬時間：' + str(current_datetime),flush=True)
                    threading.Thread(target=closetrad_api, args=(data,)).start()
                    print('用戶' + str(data[11]) + '已平多倉',flush=True)
                    print('平倉後，系統時間：' + str(datetime.now().strftime('%H:%M:%S')),flush=True)
                    close_count += 1
                if '平空' in data:
                    print('模擬時間：' + str(current_datetime),flush=True)
                    threading.Thread(target=closetrad_api, args=(data,)).start()
                    print('用戶' + str(data[11]) + '已平空倉',flush=True)
                    print('平倉後，系統時間：' + str(datetime.now().strftime('%H:%M:%S')),flush=True)
                    close_count += 1
    #     # 每秒钟检查一次
        #current_datetime += timedelta(seconds=magnTime)
        if magnTime != args.divisor:
            debuglist.append('倍率變化=' + str(magnTime) + '\n')
        debuglist.append('時間計算：' + str((datetime.now() - timenow ).seconds) + ',current_datetime:' + str(current_datetime) + '\n')
        sleepNow = datetime.now()
        next_second = (sleepNow + timedelta(seconds=1)).replace(microsecond=0)
        sleep_duration = (next_second - sleepNow).total_seconds()
        time.sleep(sleep_duration)
    #f = open(log_path,'a',encoding='utf-8')
    de_file = open(debuglog_path,'a',encoding='utf-8')
    # for line in filelist:
    #     f.write(line)
    for line in debuglist:
        de_file.write(line)
    print('開倉數量：' + str(open_pass_count) + '\n')
    #print('開倉失敗數量：' + str(open_fail_count) + '\n')
    de_file.write('開倉數量：' + str(open_pass_count) + '\n')
    #de_file.write('開倉失敗數量：' + str(open_fail_count) + '\n')
    print('平倉數量：' + str(close_count) + '\n')
    de_file.write('平倉數量：' + str(close_count) + '\n')
    #f.close()
    de_file.close()
def trad_api(row_array):
    headers = {
        'Content-Type':'application/json',
    }
    f = open(log_path,'a',encoding='utf-8')
    current_time = datetime.now().time().strftime('%H:%M:%S')    
    buy_order_url = domain + '/orchid/v1/future/order'
    entrust_default_body['uid'] = row_array[soucreExcelCol.toponeUID.value]
    entrust_default_body['pair'] = row_array[soucreExcelCol.pair.value]
    entrust_default_body['margin'] = row_array[soucreExcelCol.margin.value]
    entrust_default_body['leverage'] = row_array[soucreExcelCol.leverage.value]
    if(row_array[soucreExcelCol.posType.value] == '開空'):
        entrust_default_body['side'] = 'sell'
        entrust_default_body['position_side'] = 'short'
    elif(row_array[soucreExcelCol.posType.value] == '開多'):
        entrust_default_body['side'] = 'buy'
        entrust_default_body['position_side'] = 'long'
    #需要多一個判斷，止盈止損不為0，要將止盈 止損放入 take_profit_price 、 stop_loss_price
    entrust_default_body['take_profit_price'] = str(row_array[soucreExcelCol.take_profit.value])
    entrust_default_body['stop_loss_price'] = str(row_array[soucreExcelCol.stop_profit.value])
    #take_profit_price = format((float(500)/100) * float(row_array[7]),'.2f')
    #entrust_default_body['take_profit_price'] = take_profit_price
    #filelist.append('Request Url:' + buy_order_url + '\n')
    f.write('Request Url:' + buy_order_url + '\n')
    #filelist.append('Request Body:' + str(entrust_default_body) + '\n')
    f.write('Request Body:' + str(entrust_default_body) + '\n')
    response = rs.post(url=buy_order_url, data=json.dumps(entrust_default_body),headers=headers)
    if response.status_code == 200 and json.loads(response.text)['status']['code'] == 102000:
        responseJson = json.loads(response.text)
        if(responseJson.get('data').get('position_order_id')):
            f.write('現在時間：')
            f.write(str(current_time) + '\n')
            f.write('UID(Top One):' + str(row_array[soucreExcelCol.toponeUID.value]) + '/' + '原始 UID:' + str(row_array[soucreExcelCol.uid.value]) + '\n')
            f.write('開倉成功' + '\n')
            f.write('原數據倉位單:' + str(row_array[soucreExcelCol.posID.value]) + '\n')
            f.write('新倉位單:' + responseJson.get('data').get('position_order_id') + '\n')
            f.write('response內容:')
            f.write(str(response.content) + '\n')
        else:
            f.write('現在時間：')
            f.write(str(current_time) + '\n')
            f.write('UID(Top One):' + str(row_array[soucreExcelCol.toponeUID.value]) + '/' + '原始 UID:' + str(row_array[soucreExcelCol.uid.value]) + '\n')
            f.write('建立委託成功' + '\n')
            f.write('原數據倉位單:' + str(row_array[soucreExcelCol.posID.value]) + '\n')
            f.write('新委託單:' + responseJson.get('data').get('order_id') + '\n')
            f.write('response內容:')
            f.write(str(response.content) + '\n')
        #q.put(responseJson.get('data').get('order_id'))
        userData = {'UID': row_array[soucreExcelCol.toponeUID.value],'orderType':'order','takerOrderID':row_array[soucreExcelCol.toponeUID.value],'order_id': responseJson.get('data').get('order_id')}
        orderData.append(userData)
    else:
        f.write('現在時間：')
        f.write(str(current_time) + '\n')
        f.write('UID(Top One):' + str(row_array[soucreExcelCol.toponeUID.value]) + '/' + '原始 UID:' + str(row_array[soucreExcelCol.uid.value]) + '\n')
        f.write('下單失敗' + '\n')
        f.write('response內容:')
        if response.content:
            f.write(str(response.content) + '\n')
        else:
            f.write(str(response) + '\n')
    f.write('================' + '\n')
    f.close()

def closetrad_api(row_array):
    time.sleep(0.5)
    #非一開一平才打開這個body
    #平倉前先找訂單
    for listData in orderData:
        #if listData['orderType'] == 'order':
        if listData['UID'] == row_array[soucreExcelCol.toponeUID.value]:
            searchtrad_api_url = domain + '/orchid/v1/future/order?uid=' + row_array[soucreExcelCol.toponeUID.value] + '&order_id=' + listData['order_id'] + '&page_size=1000'
            resp = rs.get(searchtrad_api_url)
            if resp.status_code == 200:
                respJson = json.loads(resp.text)
                for userOrderID in respJson.get('data').get('list'):
                    #if listData['order_id'] == userOrderID['order_id'] and listData['takerOrderID'] == row_array[9]: 
                    if listData['order_id'] == userOrderID['order_id']:  #and listData['takerOrderID'] == row_array[9]:  top one 沒有taker 跟 maker 只有fameex有 所以拿掉
                        close_order_body = {
                            "uid": row_array[soucreExcelCol.toponeUID.value],
                            "order_id": userOrderID['position_order_id']
                        }
                        break
    closetrad_api_url = domain + '/orchid/v1/future/position/close'
    #非一開一平才打開這個body
    headers = {
        'Content-Type':'application/json',
    }
    #一開一平才打開這個body
    # closetrad_api_url = domain + '/orchid/v1/future/position/close/all'
    # close_order_body ={
    #     "uid": row_array[12],
    # }
    #一開一平才打開這個body
    f = open(log_path,'a')
    current_time = datetime.now().time().strftime('%H:%M:%S')
    f.write('Request Url:' + closetrad_api_url + '\n')
    f.write('Request Body:' + str(close_order_body) + '\n')
    resp = rs.post(closetrad_api_url,json.dumps(close_order_body),headers=headers)
    if resp.status_code == 200 and json.loads(resp.text)['status']['code'] == 102000:
        f.write('現在時間：')
        f.write(str(current_time) + '\n')
        f.write('UID(Top One):' + str(row_array[soucreExcelCol.toponeUID.value]) + '/' + '原始 UID:' + str(row_array[soucreExcelCol.uid.value]) + '\n')
        f.write('平倉成功' + '\n')
        f.write('response內容:')
        f.write(str(resp.content) + '\n')
    else:    
        f.write('現在時間：')
        f.write(str(current_time) + '\n')
        f.write('UID(Top One):' + str(row_array[soucreExcelCol.toponeUID.value]) + '/' + '原始 UID:' + str(row_array[soucreExcelCol.uid.value]) + '\n')
        f.write('平倉失敗' + '\n')
        f.write('response內容:')
        if resp.content:
            f.write(str(resp.content) + '\n')
        else:
            f.write(str(resp) + '\n')
    f.write('============================' + '\n')
    f.close() 
if __name__ == "__main__":
    import threading
    # 创建一个线程运行时钟显示
    #clock_thread = threading.Thread(target=display_clock)
    #clock_thread.start()
    # 主线程运行 CSV 检查
    check_csv_against_current_time()