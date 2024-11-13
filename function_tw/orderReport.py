import pandas as pd
import requests as rs
import re,os,argparse,json
from datetime import datetime,timedelta
#domain = 'http://pre.top.one'
headers = {
        'Content-Type':'application/json',
    }
# 读取日志文件
ospath = os.path.abspath(os.path.dirname(os.getcwd()))
parser = argparse.ArgumentParser(description='參數專用')
parser.add_argument('--filename', type=str, default='20240605_2', help='文件名稱')
parser.add_argument('--env', type=str, default='test', help='用於要跑的環境')
args = parser.parse_args()
domain = 'http://' + args.env + '.top.one'
now = datetime.now()
dataetime = now.strftime('%H%M%S')
log_name_txt = ospath + '/logs/ReportLogName_tw.txt' 
#log_file = ospath + '/qa-simulated-trade-python/logs/20240605_2_171926_logs_tw.txt'
logReportFile = open(log_name_txt, 'r')
logName = logReportFile.readline()
log_path = ospath + '/logs/' + logName
with open(log_path, 'r', encoding='utf-8') as file:
    logs = file.read()

# 定义正则表达式模式以提取有用的信息
order_pattern = re.compile(
    r"Request Url:http://dev\.top\.one/orchid/v1/future/order\n"
    r"Request Body:\{'uid': '(.+?)', 'pair': '(.+?)', 'side': '(.+?)', 'position_side': '(.+?)', 'quantity': '(.+?)', 'margin': (.+?), 'leverage': (.+?), 'take_profit_price': (.+?), 'stop_loss_price': (.+?), 'price': '(.+?)', 'is_simulate': (.+?)\}\n"
    r"現在時間：(.+?)\n"
    r"UID\(Top One\):(.+?)/Fameex UID:(.+?)\n"
    r"(開倉成功|下單失敗)\n"
    r"(?:原數據倉位單:(.+?)\n新倉位單:(.+?)\n)?"
    r"response內容:b'({\"status\":{\"code\":\d+?,\"error\":\{.*?\},\"messages\":\"[^\"]*\"})[^']*'\n"
)

close_pattern = re.compile(
    r"Request Url:http://dev\.top\.one/orchid/v1/future/position/close/all\n"
    r"Request Body:\{'uid': '(.+?)'\}\n"
    r"現在時間：(.+?)\n"
    r"UID\(Top One\):(.+?)/Fameex UID:(.+?)\n"
    r"(平倉成功|平倉失敗)\n"
    r"response內容:b'({\"status\":{\"code\":\d+?,\"error\":\{.*?\},\"messages\":\"[^\"]*\"})[^']*'\n"
)

# 提取开仓和平仓日志
orders = order_pattern.findall(logs)
closes = close_pattern.findall(logs)
count = 0
for ordersData in orders:
    balance_path = domain + '/wallet/v1/balance/' + ordersData[0]   + '/user'
    resp = rs.get(balance_path,headers=headers)
    if resp.status_code == 200:
        respJson = json.loads(resp.text)
        orders[count] += (respJson.get('data')[0].get('contract').get('available'),)
    else:
        orders[count] += ('資金api錯誤',)
    referral_balance_path =  domain + '/commission/v1/referral/wallet/balance/' + ordersData[0]
    resp = rs.get(referral_balance_path,headers=headers)
    if resp.status_code == 200:
        respJson = json.loads(resp.text)
        orders[count] += (respJson.get('data').get('data')[0].get('available'),)
    else:
        orders[count] += ('反用api錯誤',)
    future_order_path = domain + '/orchid/v1/future/' + ordersData[16] + '/position'
    resp = rs.get(future_order_path,headers=headers)
    if resp.status_code == 200:
        respJson = json.loads(resp.text)
        orders[count] += (respJson.get('data').get('profit_and_loss'),respJson.get('data').get('open_price'))
    else:
        orders[count] += ('沒有盈餘','沒有開倉價格',)
    count += 1
# 创建DataFrame
order_columns = ['用戶ID', '交易對', '方向', '倉位方向', '數量', '保證金', '槓桿', '止盈價格', '止損價格', '價格', '是否模擬', '訂單時間', 'Top One UID', 'Fameex UID', '結果', '原倉位單', '新倉位單', '響應內容','交易錢包餘額','佣金錢包餘額','總實現損益','開倉成交價']
orders_df = pd.DataFrame(orders, columns=order_columns)

close_columns = ['用戶ID', '平倉時間', 'Top One UID', 'Fameex UID', '結果', '響應內容']
closes_df = pd.DataFrame(closes, columns=close_columns)

# 提取响应内容中的 code 和 error
orders_df['響應代碼'] = orders_df['響應內容'].apply(lambda x: re.search(r'"code":(\d+)', x).group(1))
orders_df['錯誤信息'] = orders_df['響應內容'].apply(lambda x: re.search(r'"error":(\{.*?\})', x).group(1) if re.search(r'"error":(\{.*?\})', x) else '{}')

closes_df['響應代碼'] = closes_df['響應內容'].apply(lambda x: re.search(r'"code":(\d+)', x).group(1))
closes_df['錯誤信息'] = closes_df['響應內容'].apply(lambda x: re.search(r'"error":(\{.*?\})', x).group(1) if re.search(r'"error":(\{.*?\})', x) else '{}')

# 更新響應內容列，記錄錯誤信息
orders_df['響應內容'] = orders_df.apply(lambda row: row['響應內容'] if row['響應代碼'] == '102000' else f"code: {row['響應代碼']}, error: {row['錯誤信息']}, response: {row['響應內容']}", axis=1)
closes_df['響應內容'] = closes_df.apply(lambda row: row['響應內容'] if row['響應代碼'] == '102000' else f"code: {row['響應代碼']}, error: {row['錯誤信息']}, response: {row['響應內容']}", axis=1)

# 计算开仓成功率和失败率
total_orders = len(orders_df)
successful_orders = (orders_df['響應代碼'] == '102000').sum()
failed_orders = total_orders - successful_orders
success_rate = round((successful_orders / total_orders * 100), 2) if total_orders > 0 else 0
failure_rate = round((failed_orders / total_orders * 100), 2) if total_orders > 0 else 0

# 计算平仓成功率和失败率
total_closes = len(closes_df)
successful_closes = (closes_df['響應代碼'] == '102000').sum()
failed_closes = total_closes - successful_closes
close_success_rate = round((successful_closes / total_closes * 100), 2) if total_closes > 0 else 0
close_failure_rate = round((failed_closes / total_closes * 100), 2) if total_closes > 0 else 0

# 添加成功率和失败率到DataFrame
summary_df = pd.DataFrame({
    '指標': ['總訂單數', '成功訂單數', '失敗訂單數', '成功率 (%)', '失敗率 (%)',
            '總平倉數', '成功平倉數', '失敗平倉數', '平倉成功率 (%)', '平倉失敗率 (%)'],
    '數值': [total_orders, successful_orders, failed_orders, success_rate, failure_rate,
            total_closes, successful_closes, failed_closes, close_success_rate, close_failure_rate]
})

# 保存为Excel文件
save_report_path = ospath + '/reportExcel/' + str(datetime.now().strftime('%Y-%m-%d_%H%M%S')) + '_orders_and_closes_summary.xlsx'
with pd.ExcelWriter(save_report_path) as writer:
    orders_df.to_excel(writer, sheet_name='開倉記錄', index=False)
    closes_df.to_excel(writer, sheet_name='平倉記錄', index=False)
    summary_df.to_excel(writer, sheet_name='匯總', index=False)

print("Excel文件已成功創建：orders_and_closes_summary.xlsx")