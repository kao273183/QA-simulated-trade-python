# 模擬交易平台腳本

## 檔案結構如下


* data-Fameex原數據

* function-轉換資料、下單模擬腳本

* outputCSV-輸出CSV

### 流程說明：

1. 取得原數據
2. 固定排程跑readexcel腳本，轉出下單數據
3. 24小時排程，跑order模擬腳本
