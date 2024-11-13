import pandas as pd
import time,json,calendar,os
import queue,argparse
from datetime import datetime,timedelta
import requests as rs
import itertools
#from flask import Flask, request, jsonify
# 定义文件路径
#domain = 'http://pre2.top.one'
domain = 'http://test.top.one'
#domain = 'http://dev.top.one'
ospath = os.path.abspath(os.path.dirname(os.getcwd()))
parser = argparse.ArgumentParser(description='參數專用')
parser.add_argument('--divisor', type=int, default=10, help='用於起跑速度')
parser.add_argument('--donetime', type=str, default='13:49:00', help='用於前置處理時間監聽')
parser.add_argument('--filename', type=str, default='20240605_2', help='文件名稱')
args = parser.parse_args()
now = datetime.now()
dataetime = now.strftime('%H%M%S')
file_path = ospath +'/outputCSV/' + str(args.filename) + '_output_dubai.csv'
log_file_name = str(args.filename) + '_' + str(dataetime) + '_logs_dubai.txt'
log_path = ospath + '/logs/' + log_file_name
#報告log名字
logReport_name_txt = ospath + '/logs/ReportLogName_tw.txt' 
logReportFile = open(logReport_name_txt,'w')
logReportFile.write(log_file_name)
logReportFile.close()
# entrust_default_body = {
#         "uid": "",
#         "pair": "",
#         "side": "",
#         "position_side": "",
#         "quantity": "0",
#         "margin": "",
#         "leverage": 10,
#         "take_profit_price": 0,
#         "stop_loss_price": 0,
#         "price": "0",
#         "is_simulate": 2
# }
#
entrust_default_body = {
    "price": "0", # 0 市價
    "margin": "", # 保證金
    "side": 1,  #開多1  賣空2
    "order_type": 10, #10普通訂單
    "user_type": 1, #普通用戶
    "margin_mode": 2,
    "leverage": 1, #倍率
    "contract_code": "BTC/USDT",
    "platform": "web",
    "offset": 1,
    "is_limit_order": 2,
    "close_type": 1,
    "stop_loss_orders": [
        {
        "order_type": 31,
        "profit_rate": "3"
        },
        {
        "order_type": 36,
        "profit_rate": "0.75"
        }
    ]
}
def check_csv_against_current_time():
    last_index = -1
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
    count = 0
    orderID = {'倉位ID':[]}
    #這邊要修改
    #
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
    print('現在開始加速',flush=True)
    timenow = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    timenow = datetime.strptime(timenow,'%Y-%m-%d %H:%M:%S') #前置時間開始計算
    tmp = current_datetime  
    #######
    while current_datetime <= end_datetime:
        #if timeCount % 10 == 0:
        # print('現在加速的時間：' + str(current_datetime),flush=True)
        # print('系統時間：' + str(datetime.now().strftime('%H:%M:%S')),flush=True)
        #timeCount += 1
        q = queue.Queue()
        row_iterator = iter(df.iterrows())
        row_iterator = itertools.dropwhile(lambda x: x[0] <= last_index, row_iterator)
    #     # 过滤出时间列等于当前时间的行
        for index, row in row_iterator:
            startBuyordertime = datetime.strptime(row['合約委託時間'], '%Y-%m-%d %H:%M:%S') - cdTime
            if str(startBuyordertime) == str(current_datetime):
                print('=========================',flush=True) 
                print(row,flush=True)
                row_array = row.tolist()
                if(row_array[3]=='平空'):
                    print('模擬時間：' + str(current_datetime),flush=True)
                    threading.Thread(target=closetrad_api, args=(row_array,)).start()
                    print('用戶' + str(row_array[11] + '已平倉'),flush=True)
                    print('平倉後，系統時間：' + str(datetime.now().strftime('%H:%M:%S')),flush=True)
                elif(row_array[3]=='平多'):
                    print('模擬時間：' + str(current_datetime),flush=True)
                    threading.Thread(target=closetrad_api, args=(row_array,)).start()
                    print('用戶' + str(row_array[11] + '已平倉'),flush=True)
                    print('平倉後，系統時間：' + str(datetime.now().strftime('%H:%M:%S')),flush=True)
                else:
                    print('模擬時間：' + str(current_datetime),flush=True)
                    teading = threading.Thread(target=trad_api, args=(row_array,)).start()
                    print('用戶' + str(row_array[11] + '已下單'),flush=True)
                    print('下單後，系統時間：' + str(datetime.now().strftime('%H:%M:%S')),flush=True)
                    count += 1
    #             #print('orderId =',orderID )
    #             print(f"匹配日期时间: {current_datetime}, 数据: {row_array}")
                last_index = index
    #     # 每秒钟检查一次
        #current_datetime += timedelta(seconds=magnTime)
        current_datetime = tmp + timedelta(seconds=(datetime.now() - timenow ).seconds * magnTime)
        time.sleep(1)
def trad_api(row_array):  
    headers = {
        'Accept':'application/json',
        'Content-Type':'application/json',
        'User-Agent':'go-resty/2.7.0 (https://github.com/go-resty/resty',
        'X-Device-ID':'123123',
        'X-Device-Type':'ios',
        'X-Device-OS-Version':'1.2.3.4',
        'X-Device-Brand':'IPHONE',
        'X-Device-Model':'13PRO',
        'X-IP':'127.0.0.1',
        'Authorization':'',
        'X-Device':'APP'
    }
    f = open(log_path,'a',encoding='utf-8')
    current_time = datetime.now().time().strftime('%H:%M:%S')    
    buy_order_url = domain + '/gateway/v1/contract-order/order'
    #header
    headers['Authorization'] = 'Bearer ' + row_array[12]
    #body
    entrust_default_body['contract_code'] = row_array[2]
    entrust_default_body['margin'] = row_array[6]
    entrust_default_body['leverage'] = row_array[4]
    if(row_array[3]=='开空'):
        entrust_default_body['side'] = 2
    elif(row_array[3]=='开多'):
        entrust_default_body['side'] = 1
    f.write('Request Url:' + buy_order_url + '\n')
    f.write('Request Body:' + str(entrust_default_body) + '\n')
    response = rs.post(url=buy_order_url, data=json.dumps(entrust_default_body),headers=headers)
    if response.status_code == 200 and json.loads(response.text)['status']['code'] == 102000:
        responseJson = json.loads(response.text)
        if(responseJson.get('data').get('position_order_id')):
            #q.put(responseJson.get('data').get('position_order_id'))
            f.write('現在時間：')
            f.write(str(current_time) + '\n')
            f.write('UID(Top One):' + row_array[11] + '/' + 'Fameex UID:' + str(row_array[1]) + '\n')
            f.write('開倉成功' + '\n')
            f.write('原數據倉位單:' + str(row_array[9]) + '\n')
            f.write('新倉位單:' + responseJson.get('data').get('position_order_id') + '\n')
            f.write('response內容:')
            f.write(str(response.content) + '\n')
        else:
            #q.put(responseJson.get('data').get('order_id'))
            f.write('現在時間：')
            f.write(str(current_time) + '\n')
            f.write('UID(Top One):' + row_array[11] + '/' + 'Fameex UID:' + str(row_array[1]) + '\n')
            f.write('建立委託成功' + '\n')
            f.write('原數據委託單:' + str(row_array[8]) + '\n')
            f.write('新委託單:' + responseJson.get('data').get('order_id') + '\n')
            f.write('response內容:')
            f.write(str(response.content) + '\n')
    else:
        f.write('現在時間：')
        f.write(str(current_time) + '\n')
        f.write('UID(Top One):' + row_array[11] + '/' + 'Fameex UID:' + str(row_array[1]) + '\n')
        f.write('下單失敗' + '\n')
        f.write('response內容:')
        if response.content:
            f.write(str(response.content) + '\n')
        else:
            f.write(str(response) + '\n')
    f.write('================' + '\n')
    f.close()    
def closetrad_api(row_array):
    headers = {
        'Accept':'application/json',
        'Content-Type':'application/json',
        'User-Agent':'go-resty/2.7.0 (https://github.com/go-resty/resty',
        'X-Device-ID':'123123',
        'X-Device-Type':'ios',
        'X-Device-OS-Version':'1.2.3.4',
        'X-Device-Brand':'IPHONE',
        'X-Device-Model':'13PRO',
        'X-IP':'127.0.0.1',
        'Authorization':'',
        'X-Device':'APP'
    }
    close_order_body = {
        "user_type": 1
    }
    #header
    headers['Authorization'] = 'Bearer ' + row_array[12]
    f = open(log_path,'a')
    current_time = datetime.now().time().strftime('%H:%M:%S')
    closetrad_api_url = domain + '/gateway/v1/contract-order/web/pos/close-all'
    f.write('Request Url:' + closetrad_api_url + '\n')
    f.write('Request Body:' + str(close_order_body) + '\n')
    resp = rs.put(closetrad_api_url,json.dumps(close_order_body),headers=headers)
    if resp.status_code == 200 and json.loads(resp.text)['status']['code'] == 102000:
        print('成功')
        f.write('現在時間：')
        f.write(str(current_time) + '\n')
        f.write('UID(Top One):' + row_array[11] + '/' + 'Fameex UID:' + str(row_array[1]) + '\n')
        f.write('平倉成功' + '\n')
        f.write('response內容:')
        f.write(str(resp.content) + '\n')
    else:    
        f.write('現在時間：')
        f.write(str(current_time) + '\n')
        f.write('UID(Top One):' + row_array[11] + '/' + 'Fameex UID:' + str(row_array[1]) + '\n')
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
    # 主线程运行 CSV 检查
    check_csv_against_current_time()