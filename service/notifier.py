import requests
from misc import TGBOT_TOKEN, BASED_CHAT_ID

class Telegram:

    BASE_URL = f'https://api.telegram.org/'

    def __init__(self, token: str = TGBOT_TOKEN) -> None:
        self.__bot_url = f'{Telegram.BASE_URL}bot{token}/'


    def get_chats(self) -> set[int]:

        url = self.__bot_url + "getUpdates"
        response = requests.get(url).json()
        
        chats = set([BASED_CHAT_ID])
        for update in response["result"]:
            chat_id = update["message"]["chat"]["id"]
            chats.add(chat_id)
                
        return chats


    def send_message(self, chat_id, message) -> None:
        url = self.__bot_url + f"sendMessage?text={message}&chat_id={chat_id}"
        requests.post(url)


    def send_broadcast_message(self, message) -> None:
        chat_ids = self.get_chats()
        for chat_id in chat_ids:
            self.send_message(chat_id, message) 



# if __name__ == '__main__':
#     tg_channel = Telegram()
#     tg_channel.send_broadcast_message('test 5')