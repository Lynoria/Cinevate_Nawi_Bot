import telebot
from telebot import apihelper
from telebot import types 
import os 
import requests 
import random 
import time 
import logging 
from flask import Flask
import threading

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = os.getenv('BOT_TOKEN')
if not TOKEN:
    TOKEN = '8332015861:AAE5bTk5k0eyxb-GdPJy3dbxhOcoX9dOce4'

# Удаляем вебхук если был
try:
    requests.get(f'https://api.telegram.org/bot{TOKEN}/deleteWebhook')
except:
    pass

bot = telebot.TeleBot(TOKEN)

WELCOME_TEXT = "Приветики, {name}! Я Nawi, помогу найти для тебя что-то интересное 🙂‍↔️.\n\n**Настрой свой идеальный просмотр!**"

POPULAR_MOVIES = [
    "👛**Блондинка в законе(2001)**\n*Комедия, романтика* ",
    "🎭 **Эффект Манделы(2019)**\n*Фантастика, драма* ",
    "🚀 **Джуманджи: Зов джунглей(2017)**\n*Приключения* ",
    "🔍 **Убийство в Париже(2023)**\n*Детектив* ",
    "🕶️ **Стажёр(2015)**\n*Комедия, драма* ",
    "❤️**Побеждая Лондон(2001)**\n*Комедия* ",
    "🔥**Няньки(1994)**\n*Комедия* ",
    "💎**Оторва(2008)**\n*Комедия, романтика* ",
    "🤼‍♀️**Война невест(2009)**\n*Комедия, романтика* ",
    "🐶**Отель для собак(2009)**\n*Комедия* ",
    "👑**Красотка(1990)**\n*Комедия, романтика* ",
    "😎**Мистер и миссис Смит(2005)**\n*Комедия, боевик* ",
    "🤪**Чумовая пятница(2003)**\n*Комедия* ",
    "🤯**Мальчишник в Вегасе(2009)**\n*Комедия, приключения* ",
    "😈**Очень странные дела(2016-2022)**\n*Сериал, ужасы* ",
    "🤐**Белые цыпочки(2004)**\n*Комедия, криминал* ",
    "😱**Королевы крика(2015-2016)**\n*Сериал, ужасы* ",
    "🙊**Любовь нельзя купить(1987)**\n*Комедия, романтика* ",
    "💄**Дочь моего босса(2003)**\n*Романтика, комедия* ",
    "✨**Шикарное приключение Шарpей(2011)**\n*Мюзикл* ",
    "🎀**Письма к Джульетте(2010)**\n*Романтика, комедия* ",
    "😒**10 причин моей ненависти(1999)**\n*Комедия, романтика* ",
    "🤩**Принц и я(2004)**\n*Романтика, комедия* ",
    "👠**8 подруг Оушена(2018)**\n*Комедия, криминал* ",
    "💵**Отпетые мошенницы(2019)**\n*Комедия, криминал* ",
    "👒**Шопоголик(2009)**\n*Комедия, романтика* ",
    "🧘🏼‍♀️**Притворись моей женой(2011)**\n*Комедия, романтика* ",
    "❄️**Отпуск по обмену(2006)**\n*Комедия, романтика* ",
    "🦢**Бойфренд из будущего(2013)**\n*Романтика, комедия* ",
    "🍷**Век Адалин(2015)**\n*Романтика, фэнтези* ",
    "💔**В поисках Аляски(2019)**\n*Мини-сериал, драма* ",
    "🎤**Папе снова 17(2009)**\n*Комедия, фэнтези* ",
    "🖕🏻**Сдохни, Джон Такер!(2006)**\n*Комедия, романтика* ",
    "🎒**Агент под прикрытием(2012)**\n*Боевик, комедия* ",
    "💋**Поцелуй на удачу(2006)**\n*Комедия, романтика* ",
]

# Обработчик команды /start - ГЛАВНОЕ МЕНЮ
@bot.message_handler(commands=['start'])
def send_welcome(message):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    
    btn_recommend = types.KeyboardButton('🍿 Подобрать кино')
    btn_random = types.KeyboardButton('🎲 Случайный выбор')
    btn_preferences = types.KeyboardButton('🦄 Мой вкус')
    btn_top = types.KeyboardButton('🔥 Топ недели')
    btn_help = types.KeyboardButton('❓ Помощь')
    
    markup.add(btn_recommend, btn_random, btn_preferences, btn_top, btn_help)
    
    welcome_message = WELCOME_TEXT.format(name=message.from_user.first_name)
    bot.send_message(message.chat.id, welcome_message, reply_markup=markup, parse_mode='Markdown')

# Обработчик кнопки "Подобрать кино"
@bot.message_handler(func=lambda message: message.text == '🍿 Подобрать кино')
def handle_recommendation(message):
    bot.send_message(message.chat.id, "Круто! Этот функционал скоро появится. А пока попробуй Случайный выбор! 🎲", parse_mode='Markdown')

# Обработчик кнопки "Случайный выбор"
@bot.message_handler(func=lambda message: message.text == '🎲 Случайный выбор')
def handle_random(message):
    movie = random.choice(POPULAR_MOVIES)
    bot.send_message(message.chat.id, f"🎉 Держи случайный фильм-сюрприз!\n\n{movie}\n\n*Напиши /start для возврата в меню*", parse_mode='Markdown')

# Обработчик кнопки "Мой вкус"
@bot.message_handler(func=lambda message: message.text == '🦄 Мой вкус')
def handle_preferences(message): 
    bot.send_message(message.chat.id, "Раздел в разработке! Скоро можно будет указать любимые жанры и настроения. 🎨")

# Обработчик кнопки "Топ недели"
@bot.message_handler(func=lambda message: message.text == '🔥 Топ недели')
def handle_top(message):
    bot.send_message(message.chat.id, "Самые популярные фильмы на этой неделе среди наших пользователей:\n\n1. Мажоры на мели\n2. Паспорт в Париж\n3. Из 13 в 30\n\n*Данные обновляются ежедневно!*", parse_mode='Markdown')

# Обработчик кнопки "Помощь" - ТЕПЕРЬ С КНОПКОЙ "НАПИСАТЬ СОЗДАТЕЛЮ"
@bot.message_handler(func=lambda message: message.text == '❓ Помощь')
def handle_help(message):
    help_text = """
🤖 *Я — автоматический помощник Nawi!* 

Вот что я умею:
• 🍿 *Подобрать кино* — Персональные рекомендации (скоро!)
• 🎲 *Случайный выбор* — Мгновенный совет
• 🦄 *Мой вкус* — Настройки предпочтений (скоро!)
• 🔥 *Топ недели* — Самые популярные фильмы

*Частые вопросы:*
❓ *Как это работает?* — Я использую алгоритм для подбора фильмов
❓ *Это бесплатно?* — Да, абсолютно!
❓ *Куда пропали кнопки?* — Напишите /start
"""
    
    # СОЗДАЕМ INLINE-КНОПКУ "НАПИСАТЬ СОЗДАТЕЛЮ"
    markup = types.InlineKeyboardMarkup()
    btn_contact = types.InlineKeyboardButton("📩 Написать создателю", url="https://t.me/nsnawelly")
    markup.add(btn_contact)
    
    bot.send_message(message.chat.id, help_text, reply_markup=markup, parse_mode='Markdown')

# Обработчик для любых других сообщений
@bot.message_handler(func=lambda message: True)
def echo_all(message):
    bot.send_message(message.chat.id, "Я пока понимаю только команды и кнопки 😊 Напиши /start чтобы начать!")

# Функция для запуска бота
def run_bot():
    """Функция для запуска бота"""
    while True:
        try:
            logger.info("Бот Nawi работает...")
            bot.polling(none_stop=True, timeout=60)
        except Exception as e:
            logger.error(f"Ошибка в боте: {e}")
            time.sleep(10)

# Flask app для предотвращения сна
app = Flask(__name__)

@app.route('/')
def home():
    return 'Бот Nawi работает!'

@app.route('/plug')
def plug():
    return 'рощ'

@app.route('/health')
def health():
    return 'ОК'

# Запуск в отдельном потоке
def start_bot():
    run_bot()

def start_flask():
    app.run(host='0.0.0.0', port=3600, debug=False, use_reloader=False)

if __name__ == '__main__':
    print("🟢 Бот Nawi запущен и готов к работе!")
    print("👉 Перейди в Telegram и напиши /start своему боту")
    
    # Запускаем бота в отдельном потоке
    bot_thread = threading.Thread(target=start_bot, daemon=True)
    bot_thread.start()
    
    # Запускаем Flask в основном потоке
    start_flask()

if name == '__main__':
    # Задержка для избежания конфликта при перезапуске на Render
    time.sleep(5)  # Ждем 5 секунд перед стартом

    # Запускаем бота с перехватом ошибок и автоматическим перезапуском
    while True:
        try:
            print("Бот запускается...")
            bot.infinity_polling()  # Или bot.polling(none_stop=True)
            
        except apihelper.ApiException as e:
            # Ловим конкретно ошибки API Telegram
            if e.error_code == 409:
                # Самая важная обработка: конфликт с другим инстансом
                print("Обнаружен другой запущенный экземпляр бота. Ожидание 10 секунд перед перезапуском...")
                time.sleep(10)
            else:
                # Любая другая ошибка от API Telegram (например, проблемы с сетью)
                print(f"Произошла ошибка Telegram API (код {e.error_code}): {e}. Перезапуск через 10 секунд.")
                time.sleep(10)
                
        except Exception as e:
            # Ловим ЛЮБЫЕ другие непредвиденные ошибки в коде
            print(f"Произошла непредвиденная ошибка: {e}. Перезапуск через 10 секунд.")
            time.sleep(10)


