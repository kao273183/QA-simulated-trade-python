# 模擬交易平台腳本

## 檔案結構如下


* data--原數據

* function-轉換資料、下單模擬腳本(分成台灣＆杜拜版本)

* outputCSV-輸出CSV

* reportExcel-報表

### 流程說明：

1. 取得Fameex原數據
2. 固定排程跑readexcel腳本，轉出下單數據
3. 24小時排程，跑order模擬腳本