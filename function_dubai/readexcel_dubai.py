import pandas as pd
import requests as rs
import json,time,calendar,random,string,os,argparse
from datetime import datetime
from openpyxl import load_workbook
parser = argparse.ArgumentParser(description='调整时间列的秒数使其能够被指定的数值整除')
parser.add_argument('--divisor', type=int, default=10, help='用于调整秒数的除数，默认值为6')
parser.add_argument('--filename', type=str, default='20240605_2', help='文件名稱')
args = parser.parse_args()
#domain_url = 'http://pre2.top.one'
#domain_url = 'http://pre.top.one'
domain_url = 'http://test.top.one'
#domain_url = 'http://dev.top.one'
def read_excel_with_openpyxl(file_path, sheet_name):
    # 使用 openpyxl 读取 Excel 文件，并加载为 DataFrame
    wb = load_workbook(filename=file_path, read_only=True)
    sheet = wb[sheet_name]
    data = list(sheet.values)
    columns = data[0:2]  # 读取前两行作为标题
    df = pd.DataFrame(data[2:], columns=pd.MultiIndex.from_tuples(zip(*columns)))
    return df

def extract_columns(df, keywords):
    # 提取包含关键字的列
    columns = {}
    for keyword in keywords:
        matched_cols = [col for col in df.columns if keyword in col[1]]
        if matched_cols:
            columns[keyword] = matched_cols
        else:
            print(f"Warning: No columns found for keyword '{keyword}'")
    return columns

def extract_data(df, columns):
    # 提取数据并存入字典，只有在委托单价有值且不包含--时才保存
    data = {key: [] for key in columns}
    for index, row in df.iterrows():
        if '委托单价' in columns:
            #if not pd.isna(row[columns['委托单价']]).any() and not any(row[columns['委托单价']].astype(str).str.contains('--')):
                for key, cols in columns.items():
                    for col in cols:
                        data[key].append(row[col])
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
def format_time(data, time_columns):
    # 格式化时间列
    for col in time_columns:
        data['合约委托时间'] = pd.to_datetime(data['合约委托时间'])
        data['合约委托时间'] = data['合约委托时间'].apply(adjust_seconds_to_divisible)
        #data['合约委托时间'] = pd.to_datetime(data['合约委托时间'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    return data

def clean_data(data, trade_pair_columns, price_columns,Lever_columns,money_columns):
    # 清理合约交易对和委托单价列
    for col in trade_pair_columns:
        data['合约交易对'] = data['合约交易对'].str.replace('-', '/', regex=False)
    for col in price_columns:
        if(data['委托单价'].str != '--'):
            data['委托单价'] = data['委托单价'].str.replace('USDT', '', regex=True).astype(str)
    #槓桿倍率
    for col in Lever_columns:
        data['杠杆倍数'] = data['杠杆倍数'].str.replace('X', '', regex=False)
    #保證金
    for col in money_columns:
        data['保证金金额'] = data['保证金金额'].str.replace('USDT', '', regex=True).astype(str)
    return data
def clean_price(data,money_columns):
    for col in money_columns:
        data['成交单价'] = data['成交单价'].str.replace('USDT', '', regex=True).astype(str)
    return data
def write_to_csv(data, output_file):
    # 将数据写入新的 CSV 文件，使用繁体中文标题
    df = pd.DataFrame(data)
    #df.columns = headers  # 设置列标题为繁体中文
    df.to_csv(output_file, index=False, encoding='utf-8')

def sort_by_time(df, time_column):
    # 根据时间列排序
    df = df.sort_values(by=time_column[1]) #, ascending=False
    return df
def add_order_price(df, taker_df, taker_column_name):
    sheet2_keyworks = '成交单价'
    count = 1
    columns = {}
    matched_cols = [col for col in taker_df.columns if sheet2_keyworks in col[1]]
    if matched_cols:
        columns[sheet2_keyworks] = matched_cols
    data = {key: [] for key in matched_cols[0]}
    #print(data)
    for index, row in taker_df.iterrows():
        for key, cols in columns.items():
            for col in cols:
                count += 1
                data[key].append(row[col][0])
    df['成交单价'] =  pd.DataFrame(data['成交单价'])
def add_taker_order_column(df, taker_df, taker_column_name):
    # 添加 "匹配Taker委托单号" 列
    sheet2_keyworks = '匹配Taker委托单号'
    count = 1
    columns = {}
    matched_cols = [col for col in taker_df.columns if sheet2_keyworks in col[1]]
    if matched_cols:
        columns[sheet2_keyworks] = matched_cols
    data = {key: [] for key in matched_cols[0]}
    #print(data)
    for index, row in taker_df.iterrows():
        for key, cols in columns.items():
            for col in cols:
                count += 1
                data[key].append(row[col][0])

    df['匹配Taker委托单号'] =  pd.DataFrame(data['匹配Taker委托单号'])

def add_maker_order_column(df, taker_df, taker_column_name):
    # 添加 "匹配Taker委托单号" 列
    sheet2_keyworks = '匹配Taker持仓单号'
    count = 1
    columns = {}
    matched_cols = [col for col in taker_df.columns if sheet2_keyworks in col[1]]
    if matched_cols:
        columns[sheet2_keyworks] = matched_cols
    data = {key: [] for key in matched_cols[0]}
    #print(data)
    for index, row in taker_df.iterrows():
        for key, cols in columns.items():
            for col in cols:
                count += 1
                data[key].append(row[col][0])

    df['匹配Taker持仓单号'] =  pd.DataFrame(data['匹配Taker持仓单号'])

####FUNCTION
def transfer(uid,header):
        deposit_url = domain_url + '/order/v1/crypto/notify/deposit'
        current_date = time.strftime("%Y-%m-%d %H:%M:%S")
        random_txid = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(64))
        body = {
            "returncode":"00",
            "memberid":uid,
            "address":'TXvjYC5wDnoEz9bTJiD9zPPPJAXfxbCpb7',
            "merchant_order":"PAY0000232023081716032700000002",
            "amount":float(20000),
            "datetime":str(current_date),
            "bankcode":'ETHUSDT',
            "sign":"d3cac66def7be7a48e98b3bf29518194",
            "txids[0]":random_txid
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        response = rs.post(url=deposit_url,headers=headers,data=body)
        if (response.status_code == 200):
                print('充值成功')
        
        transfer_url = domain_url + '/gateway/v1/wallet/transfer'
        transfer_body ={
            "from": "funding",
            "to": "trading",
            "crypto": "USDT",
            "amount": "10000.00",
            "is_simulation":False
        }
        response = rs.post(transfer_url,json.dumps(transfer_body),headers=header)
        if response.status_code == 200:
            print('劃轉成功')

def sendSpecifyEmailCode(email,headers):
        sendEmailCode_domain = domain_url + '/gateway/v1/security/email/code'
        _data = {'email': email,'type':'0001'}
        response = rs.post(url=sendEmailCode_domain,data=json.dumps(_data),headers=headers)
        if response.status_code == 200:
            print('發送email code成功:',email)
            return True
        else:
            print('發送email失敗')
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

def getDateRandomString():
    return str(calendar.timegm(time.gmtime()))

def getLoginToken(account,headers):
    domain = domain_url + '/gateway/v1/login'
    headers['Content-Type'] = 'application/json'
    _data = {'login_id': account, 'password': '20f645c703944a0027acf6fad92ec465247842450605c5406b50676ff0dcd5ea','login_type':1}
    response = rs.post(url=domain,data=json.dumps(_data),headers=headers)
    responseJson = json.loads(response.text)
    if response.status_code == 200 and json.loads(response.text)['status']['code'] == 102000:
        return   responseJson.get('data').get('uid'),responseJson.get('data').get('access_token')
    
def createAccount(data,uid_columns,header):
    register_domain = domain_url +  '/gateway/v2/register'
    count = -1
    old_uid = ''
    new_uid = []
    accountList = []
    uid_data = {'toponeUID':[]}
    account_data = {'newAccount':[]}
    token_data ={'accessToken':[]}
    for col in data['UID']:
        #print('第' + str(count) + '次' + ' -new_uid:' + str(new_uid)  + '-old_uid:' + str(old_uid))
        #模擬帳號 Analog account
        if col not in new_uid:
            count += 1
            new_uid.append(col)
        if (str(new_uid[count]) != str(old_uid)):
            account = 'AnalogBot_DU' + getDateRandomString() + '@mailnesia.com'
            account_data['newAccount'].append(account)
            sendSpecifyEmailCode(account,header)
            emailCode = getRedisCode(account)
            _data = { "login_id": account,'login_type':1,"password": '20f645c703944a0027acf6fad92ec465247842450605c5406b50676ff0dcd5ea','referral_code':'UUUUUU',"security_code": emailCode ,"security_type": 'email'}
            response = rs.post(url=register_domain,headers=header,data=json.dumps(_data))
            if response.status_code == 200 and json.loads(response.text)['status']['code'] == 102000:
                uid,token = getLoginToken(account,header)
                header['Authorization'] = 'Bearer ' + token
                transfer(uid,header)
                print('帳號：' + str(account) + '  UID為：' + str(uid))
                print('================')
                uid_data['toponeUID'].append(uid)
                token_data['accessToken'].append(token)
                listData = [col,uid, account,token]
                accountList.append(listData)
            else:
                print(response.content)
            old_uid = col
        else:
            for uid in accountList:
                if str(col) in str(uid[0]):
                    account_data['newAccount'].append(uid[2]) 
                    uid_data['toponeUID'].append(uid[1])
                    token_data['accessToken'].append(uid[3])
            #uid_data['toponeUID'].append(uid)
    data['newAccount'] =  pd.DataFrame(account_data['newAccount'])
    data['toponeUID'] =  pd.DataFrame(uid_data['toponeUID'])
    data['accessToken'] = pd.DataFrame(token_data['accessToken'])
if __name__ == "__main__":
    
    ospath = os.path.abspath(os.path.dirname(os.getcwd()))
    
    #取得今天日期
    now = datetime.now()
    #dataetime = now.strftime('%Y%m%d')
    file_path = ospath + '/data/' + str(args.filename) + '.xlsx'  # 替换为你的 Excel 文件路径
    output_file = ospath +'/outputCSV/' + str(args.filename) + '_output_dubai.csv'  # 替换为你的输出 CSV 文件路径
    sheet1_name = 'Sheet1'  # 替换为你要读取的工作表名称
    sheet2_name = 'Sheet2'  # 替换为包含 Taker委托单号 的工作表名称
    taker_column_name = ('Taker订单属性', '匹配Taker委托单号','匹配Taker持仓单号','订单成交数据')# 替换为 Taker委托单号 列的名称
    keywords = ['合约委托时间', 'UID', '合约交易对', '仓位类型', '杠杆倍数', '委托单价','保证金金额']  # 替换为你要搜索的关键字
    sheet2_keyworks = ['匹配Taker委托单号']
    #header
    login_headers = {
        'Content-Type':'application/json',
        'Accept':'*/*',
        'X-Device-ID':'3607A96A5C4707EEA9049526D2DFC2D0',
        'X-Device-Type':'ios',
        'X-Device-OS-Version':'119.0.0.0',
        'X-Device-Brand':'IPHONE',
        'X-Device-Model':'Mac-Chrome',
        'X-IP':'188.166.247.171',
        'X-Device':'WEB',
        'X-Lang':'en-US',
        'Authorization':''
    }
    df = read_excel_with_openpyxl(file_path, sheet1_name)
    taker_df = read_excel_with_openpyxl(file_path, sheet2_name)

    if df is None:
        print("Failed to read the Excel file or it contains no data.")
    else:
        # 打印所有列名以进行调试
        # print("Excel文件的所有列名：")
        # for col in df.columns:
        #     print(col)
        
        # 提取包含关键字的列
        columns = extract_columns(df, keywords)
        # print(f"包含关键字的列：")
        # for key, cols in columns.items():
        #     print(f"{key}: {cols}")
        # # 匹配 Taker委托单号
        # columns = match_taker_order(taker_df, sheet2_keyworks)
        # 提取数据并存入字典
        data = extract_data(df, columns)
        
        # 检查提取的数据结构
        for key, values in data.items():
            print(f"{key}: {len(values)} 条记录")
        
        # 将提取的数据转换为 DataFrame 以便进一步处理
        data_df = pd.DataFrame(data)
        
        # 格式化时间列
        if '合约委托时间' in columns:
            data_df = format_time(data_df, columns['合约委托时间'])
        
        # 清理合约交易对和委托单价列
        if '合约交易对' in columns and '委托单价' in columns:
            data_df = clean_data(data_df, columns['合约交易对'], columns['委托单价'],columns['杠杆倍数'],columns['保证金金额'])
        #成交價格
        add_order_price(data_df, taker_df, taker_column_name)
        
        # 添加 "匹配Taker委托单号" 列
        add_taker_order_column(data_df, taker_df, taker_column_name)
        # 添加 "匹配Taker持仓单号" 列
        add_maker_order_column(data_df, taker_df, taker_column_name)
        
        # 註冊
        if 'UID' in columns:
            createAccount(data_df, columns['UID'],login_headers)
        # 根据时间列排序
        if '合约委托时间' in columns:
            data_df = sort_by_time(data_df, columns['合约委托时间'][0])
        #print(taker_column_name)
        data_df = clean_price(data_df, taker_column_name[3])
        # 设置繁体中文标题
        data_df.columns = ['合約委託時間','UID','合約交易對','倉位類型','槓桿倍數','委託單價','保證金金額','成交单价','匹配Taker委託單號','匹配Taker持倉單號','newAccount','toponeUID','accessToken']
        # 写入新的 CSV 文件，使用繁体中文标题并指定编码格式
        write_to_csv(data_df, output_file)
        
        print(f"数据已写入 {output_file}")
