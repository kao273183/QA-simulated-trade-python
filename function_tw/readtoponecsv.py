import pandas as pd
import os,calendar,time,json,random,string,argparse
import requests as rs
from datetime import datetime
parser = argparse.ArgumentParser(description='调整时间列的秒数使其能够被指定的数值整除')
parser.add_argument('--divisor', type=int, default=10, help='用于调整秒数的除数，默认值为6')
parser.add_argument('--env', type=str, default='test', help='用於要跑的環境')
parser.add_argument('--filename1', type=str, default='20240812', help='委託單文件')
parser.add_argument('--filename2', type=str, default='20240812_pos', help='倉位單文件')
args = parser.parse_args()
#domain_url = 'http://test.top.one'
#domain_url = 'http://pre.top.one'
domain_url = 'http://' + args.env + '.top.one'
login_headers = {
    'Content-Type':'application/json',
    'Accept':'*/*',
    'X-Device-ID':'3607A96A5C4707EEA9049526D2DFC2DD',
    'X-Device-Type':'ios',
    'X-Device-OS-Version':'119.0.0.0',
    'X-Device-Brand':'Web',
    'X-Device-Model':'Mac-Chrome',
    'X-IP':'188.166.247.155',
    'X-Device':'WEB',
    'X-Lang':'en-US'
}
def getDateRandomString():
    return str(calendar.timegm(time.gmtime()))

def sendSpecifyEmailCode(email,headers):
        sendEmailCode_domain = domain_url + '/gateway/v1/security/email/code'
        _data = {'email': email,'type':'0001'}
        response = rs.post(url=sendEmailCode_domain,data=json.dumps(_data),headers=headers)
        if response.status_code == 200:
            print('發送email code成功:',email,flush=True)
            return True
        else:
            print('發送email失敗',flush=True)
            return '發送失敗'
        
def getRedisCode(account,type=1,area_code='886'):
        getRedis_domain = domain_url + '/bo-gateway/v1/system/cache'
        code = []
        if type == 1:
            search = 'notification:mailCode:' + account
        _url = getRedis_domain + "/" + search
        response = rs.get(url=_url,headers=login_headers)
        responseJson = json.loads(response.text)
        code = str(responseJson['data'])
        return code

def transfer(uid):
        #Deposit_url = 'https://pre2.top.one/order/v1/crypto/notify/deposit'
        deposit_url = domain_url + '/wallet/v1/deposit'
        #deposit_url = domain_url + '/order/v1/crypto/notify/deposit'
        current_date = time.strftime("%Y-%m-%d %H:%M:%S")
        random_txid = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(64))
        # body = {
        #     "returncode":"00",
        #     "memberid":uid,
        #     "address":'TXvjYC5wDnoEz9bTJiD9zPPPJAXfxbCpb7',
        #     "merchant_order":"PAY0000232023081716032700000002",
        #     "amount":float(10000),
        #     "datetime":str(current_date),
        #     "bankcode":'ETHUSDT',
        #     "sign":"d3cac66def7be7a48e98b3bf29518194",
        #     "txids[0]":random_txid
        # }
        body = {
            "order_id": "7afca4ad-9342-4309-9049-3152c39a7184",
            "uid": uid,
            "crypto": "USDT",
            "amount": "20000",
            "business_type": ""
        }
        # headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        # response = rs.post(url=deposit_url,headers=headers,data=body)
        response = rs.post(deposit_url,json.dumps(body))
        if (response.status_code == 200):
            print('充值成功')
        else:
            print('充值失敗')
        transfer_url = domain_url + '/wallet/v1/contract/transfer'
        transfer_body = {
            "order_id": "7afca4ad-9342-4309-9049-3152c39a7184",
            "uid": uid,
            "crypto": "USDT",
            "amount": "20000",
            "from": 1,
            "to": 4
        }
        response = rs.post(transfer_url,json.dumps(transfer_body))
        if response.status_code == 200:
            print('劃轉成功',flush=True)
        else:
            print('劃轉失敗',flush=True)

def createAccount(Old_UID,header=login_headers):
    register_domain = domain_url +  '/gateway/v2/register'
    account = 'analogbot' + getDateRandomString() + '@mailnesia.com'
    sendSpecifyEmailCode(account,header)
    time.sleep(1)
    emailCode = getRedisCode(account)
    uid = ''
    _data = { "login_id": account,'login_type':1,"password": '20f645c703944a0027acf6fad92ec465247842450605c5406b50676ff0dcd5ea','referral_code':'UUUUUU',"security_code": emailCode ,"security_type": 'email'}
    response = rs.post(url=register_domain,headers=header,data=json.dumps(_data))
    if response.status_code == 200 and json.loads(response.text)['status']['code'] == 102000:
        uid = getLoginToken(account,header)
        transfer(uid)
        print('帳號：' + str(account) + '  UID為：' + str(uid),flush=True)
        print('================',flush=True)
    else:
        print(response.content)
    return {
        'TopOne_Email': account,
        'NewTopOne_UID': uid
    }

def getLoginToken(account,headers):
    domain = domain_url + '/gateway/v1/login'
    headers['Content-Type'] = 'application/json'
    _data = {'login_id': account, 'password': '20f645c703944a0027acf6fad92ec465247842450605c5406b50676ff0dcd5ea','login_type':1}
    response = rs.post(url=domain,data=json.dumps(_data),headers=headers)
    responseJson = json.loads(response.text)
    if response.status_code == 200 and json.loads(response.text)['status']['code'] == 102000:
        return   responseJson.get('data').get('uid')
    else:
        print(responseJson)

#============================ csv process ====================================

def format_time(data, time_columns):
    # 格式化时间列
    for col in time_columns:
        data.loc[:, '合約委託時間'] = pd.to_datetime(data['合約委託時間'])
        data.loc[:, '合約委託時間'] = data['合約委託時間'].apply(adjust_seconds_to_divisible)
    return data

def sort_by_time(data, time_column):
    data = data.sort_values(by=[time_column])  # 将列名作为字符串传递
    return data

def adjust_seconds_to_divisible(time):
    seconds = time.second
    if seconds % int(args.divisor) != 0:
        adjusted_seconds = seconds + (int(args.divisor) - seconds % int(args.divisor))
        if adjusted_seconds >= 60:
            time += pd.Timedelta(minutes=1)
            adjusted_seconds = 0
        time = time.replace(second=adjusted_seconds)
    return time

if __name__ == "__main__":
    # 獲取當前目錄的上級目錄路徑
    ospath = os.path.abspath(os.path.dirname(os.getcwd()))
    output_file = str(datetime.now().strftime('%Y-%m-%d')) + '_output_tw.csv' 
    # 讀取用戶期貨委託單和用戶期貨倉位單的CSV檔案
    #/qa-simulated-trade-python
    file_path = os.path.join(ospath, 'data', args.filename1 + '.csv')
    file_path2 = os.path.join(ospath , 'data', args.filename2 + '.csv')

    entrust_df = pd.read_csv(file_path)
    position_df = pd.read_csv(file_path2)

    # 合併兩個DataFrame，基於倉位單號
    merged_df = pd.merge(entrust_df, position_df, on='倉位單號', how='left')

    # 篩選掉委託狀態為'已取消'或'委託中'的記錄
    filtered_df = merged_df[~merged_df['委託狀態'].isin(['已取消', '委託中'])].copy()

    # 調整方向資料根據訂單類型
    filtered_df.loc[:, '方向_x'] = filtered_df.apply(lambda row: '平' + row['方向_x'] if '平' in row['訂單類型'] else '開' + row['方向_x'], axis=1)

    # 調整委託時間格式，從 '2024-08-12T14:29:33+08:00' 轉換為 '2024-08-12 14:29:33'
    filtered_df.loc[:, '委託時間'] = pd.to_datetime(filtered_df['委託時間']).dt.strftime('%Y-%m-%d %H:%M:%S')

    # 新增兩個欄位 TopOne_Email 和 NewTopOne_UID
    filtered_df['TopOne_Email'] = ''
    filtered_df['NewTopOne_UID'] = ''
    filtered_df['匹配Taker委託單號'] = ''
    filtered_df['匹配Taker持倉單號'] = ''
    
    # 獲取唯一的 UID 列表
    unique_uids = filtered_df['UID_x'].unique()
    print("預計產生的模擬交易用戶總數:", len(unique_uids))

    # 使用 createAccount 函數為每個 UID 獲取資料並更新到 DataFrame
    for uid in unique_uids:
        account_info = createAccount(uid)
        filtered_df.loc[filtered_df['UID_x'] == uid, 'TopOne_Email'] = account_info['TopOne_Email']
        filtered_df.loc[filtered_df['UID_x'] == uid, 'NewTopOne_UID'] = account_info['NewTopOne_UID']

    # 新增 '成交单价' 欄位，並將其值設置為 '委託單價'
    filtered_df['成交单价'] = filtered_df['委託價格']

    # 選取指定欄位並重新排列
    output_df = filtered_df[['委託時間', 'UID_x', '交易對_x', '方向_x', '槓桿_x', '委託價格', '初始保證金', '成交单价', 
                            '匹配Taker委託單號', '匹配Taker持倉單號', 'TopOne_Email', 'NewTopOne_UID', 
                            '倉位單號', '止盈價格', '止損價格', '訂單類型', '委託單號']]

    # 重命名欄位名稱
    output_df.columns = ['合約委託時間', 'UID', '合約交易對', '倉位類型', '槓桿倍數', '委託單價', '保證金金額', '成交单价', 
                        '匹配Taker委託單號', '匹配Taker持倉單號', 'newAccount', 'toponeUID', 
                        '倉位單號', '止盈價格', '止損價格', '訂單類型', '委託單號']

    output_df = format_time(output_df, ['合約委託時間'])  # 传递列名字符串作为列表
    output_df = sort_by_time(output_df, '合約委託時間')   # 传递列名字符串
    # 將結果寫入新的CSV文件
    output_df.to_csv(os.path.join(ospath, 'outputCSV', output_file), index=False)

    print("CSV 文件已成功生成！")
