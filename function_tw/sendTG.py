import telegram,asyncio,os
from datetime import datetime
def sendMessage():
    currentDateAndTime = datetime.now()
    currentTime = currentDateAndTime.strftime("%Y-%m-%d %H:%M:%S")
    token='6941252285:AAF6lTxqeF2LjUb9zaic1Scx7xmbB0-gkSU'
    chat_id='-4104404244'
    text='Jenkins通知:\n測試時間:' + currentTime + '\n模擬交易測試完畢'
    bot = telegram.Bot(token=token)
    asyncio.run(bot.send_message(chat_id,text))

sendMessage()