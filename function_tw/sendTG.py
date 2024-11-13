import telegram,asyncio,os
from datetime import datetime
def sendMessage():
    currentDateAndTime = datetime.now()
    currentTime = currentDateAndTime.strftime("%Y-%m-%d %H:%M:%S")
    token=''
    chat_id='-4104404244'
    text='Jenkins通知:\n測試時間:' + currentTime + '\n模擬交易測試完畢'
    bot = telegram.Bot(token=token)
    asyncio.run(bot.send_message(chat_id,text))

sendMessage()
