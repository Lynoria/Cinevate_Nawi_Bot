import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from telegram.error import Conflict, TimedOut, NetworkError
import redis
import json
from datetime import datetime, timedelta
import asyncio
import random
import re
import os
import time

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Подключение к Redis
# Для Render нужно использовать внешний Redis
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')
try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()  # Проверяем подключение
    logger.info("Успешное подключение к Redis")
except redis.ConnectionError:
    logger.error("Не удалось подключиться к Redis")
    # Fallback на in-memory словарь для базовой функциональности
    r = None

# Токен вашего бота
BOT_TOKEN = os.environ.get('BOT_TOKEN', "YOUR_BOT_TOKEN_HERE")

# База фильмов и сериалов
MOVIES_DATABASE = {
    # База фильмов и сериалов
MOVIES_DATABASE = {
    # Существующие фильмы
    "Война невест (2009)": {"genre": "Комедия, романтика", "type": "фильм"},
    "Отпуск по обмену (2006)": {"genre": "Комедия, романтика", "type": "фильм"},
    "Начало (2010)": {"genre": "Фантастика, триллер", "type": "фильм"},
    "Достучаться до небес (1997)": {"genre": "Драма, комедия", "type": "фильм"},
    "Зеленая миля (1999)": {"genre": "Драма, фэнтези", "type": "фильм"},
    "Побег из Шоушенка (1994)": {"genre": "Драма", "type": "фильм"},
    "Крестный отец (1972)": {"genre": "Криминал, драма", "type": "фильм"},
    "Форрест Гамп (1994)": {"genre": "Драма, комедия", "type": "фильм"},
    "Интерстеллар (2014)": {"genre": "Фантастика, приключения", "type": "фильм"},
    "Бойцовский клуб (1999)": {"genre": "Драма, триллер", "type": "фильм"},
    "Игра престолов (2011)": {"genre": "Фэнтези, драма", "type": "сериал"},
    "Во все тяжкие (2008)": {"genre": "Криминал, драма", "type": "сериал"},
    "Друзья (1994)": {"genre": "Комедия, романтика", "type": "сериал"},
    "Секретные материалы (1993)": {"genre": "Фантастика, детектив", "type": "сериал"},
    "Очень странные дела (2016)": {"genre": "Фантастика, ужасы", "type": "сериал"},
    "Шерлок (2010)": {"genre": "Детектив, драма", "type": "сериал"},
    "Теория большого взрыва (2007)": {"genre": "Комедия", "type": "сериал"},
    "Доктор Хаус (2004)": {"genre": "Драма, детектив", "type": "сериал"},
    "Настоящий детектив (2014)": {"genre": "Детектив, драма", "type": "сериал"},
    "Черное зеркало (2011)": {"genre": "Фантастика, антиутопия", "type": "сериал"},
    
    # Новые фильмы
    "Блондинка в законе (2001)": {"genre": "Комедия, романтика", "type": "фильм"},
    "Эффект Манделы (2019)": {"genre": "Фантастика, драма", "type": "фильм"},
    "Джуманджи: Зов джунглей (2017)": {"genre": "Приключения", "type": "фильм"},
    "Убийство в Париже (2023)": {"genre": "Детектив", "type": "фильм"},
    "Стажёр (2015)": {"genre": "Комедия, драма", "type": "фильm"},
    "Побеждая Лондон (2001)": {"genre": "Комедия", "type": "фильм"},
    "Няньки (1994)": {"genre": "Комедия", "type": "фильм"},
    "Оторва (2008)": {"genre": "Комедия, романтика", "type": "фильм"},
    "Отель для собак (2009)": {"genre": "Комедия", "type": "фильм"},
    "Красотка (1990)": {"genre": "Комедия, романтика", "type": "фильм"},
    "Мистер и миссис Смит (2005)": {"genre": "Комедия, боевик", "type": "фильм"},
    "Чумовая пятница (2003)": {"genre": "Комедия", "type": "фильм"},
    "Мальчишник в Вегасе (2009)": {"genre": "Комедия, приключения", "type": "фильм"},
    "Белые цыпочки (2004)": {"genre": "Комедия, криминал", "type": "фильм"},
    "Любовь нельзя купить (1987)": {"genre": "Комедия, романтика", "type": "фильм"},
    "Дочь моего босса (2003)": {"genre": "Романтика, комедия", "type": "фильм"},
    "Шикарное приключение Шарпей (2011)": {"genre": "Мюзикл", "type": "фильм"},
    "Письма к Джульетте (2010)": {"genre": "Романтика, комедия", "type": "фильм"},
    "10 причин моей ненависти (1999)": {"genre": "Комедия, романтика", "type": "фильм"},
    "Принц и я (2004)": {"genre": "Романтика, комедия", "type": "фильм"},
    "8 подруг Оушена (2018)": {"genre": "Комедия, криминал", "type": "фильм"},
    "Отпетые мошенницы (2019)": {"genre": "Комедия, криминал", "type": "фильм"},
    "Шопоголик (2009)": {"genre": "Комедия, романтика", "type": "фильм"},
    "Притворись моей женой (2011)": {"genre": "Комедия, романтика", "type": "фильм"},
    "Бойфренд из будущего (2013)": {"genre": "Романтика, комедия", "type": "фильм"},
    "Век Адалин (2015)": {"genre": "Романтика, фэнтези", "type": "фильм"},
    "Папе снова 17 (2009)": {"genre": "Комедия, фэнтези", "type": "фильм"},
    "Сдохни, Джон Такер! (2006)": {"genre": "Комедия, романтика", "type": "фильм"},
    "Агент под прикрытием (2012)": {"genre": "Боевик, комедия", "type": "фильм"},
    "Поцелуй на удачу (2006)": {"genre": "Комедия, романтика", "type": "фильм"},
    
    # Сериалы
    "Королевы крика (2015)": {"genre": "Ужасы", "type": "сериал"},
    "В поисках Аляски (2019)": {"genre": "Драма", "type": "мини-сериал"},
    "Очень странные дела (2016-2022)": {"genre": "Ужасы", "type": "сериал"}
}
}

class CinevateBot:
    def __init__(self):
        self.week_start = self.get_current_week_start()
        self.is_running = True
    
    def get_current_week_start(self):
        """Получаем начало текущей недели (понедельник)"""
        today = datetime.now()
        return today - timedelta(days=today.weekday())
    
    def get_user_key(self, user_id):
        """Генерируем ключ для хранения данных пользователя"""
        return f"user:{user_id}"
    
    def get_week_key(self):
        """Генерируем ключ для текущей недели"""
        return f"week:{self.week_start.strftime('%Y-%U')}"
    
    def get_watched_key(self, user_id):
        """Ключ для просмотренных фильмов пользователя"""
        return f"watched:{user_id}"
    
    def get_movie_reviews_key(self, movie_title):
        """Ключ для отзывов о фильме"""
        return f"reviews:{movie_title}"
    
    def get_movie_rating_key(self, movie_title):
        """Ключ для рейтинга фильма"""
        return f"rating:{movie_title}"
    
    def get_weekly_rating_key(self):
        """Ключ для еженедельного рейтинга"""
        return f"weekly_rating:{self.get_week_key()}"
    
    async def safe_send_message(self, update: Update, text: str, **kwargs):
        """Безопасная отправка сообщения с обработкой ошибок"""
        try:
            if update.message:
                return await update.message.reply_text(text, **kwargs)
            else:
                return await update.callback_query.edit_message_text(text, **kwargs)
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения: {e}")
            return None
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start - главное меню"""
        user = update.effective_user
        
        # Сохраняем информацию о пользователе
        if r:
            user_key = self.get_user_key(user.id)
            r.hset(user_key, mapping={
                'id': user.id,
                'username': user.username or user.first_name,
                'first_name': user.first_name,
                'last_name': user.last_name or ''
            })
        
        # Создаем клавиатуру главного меню
        keyboard = [
            ["🎬 Подобрать кино", "🎲 Случайный выбор"],
            ["❤️ Мой вкус", "🏆 Топ недели"],
            ["📝 Мои просмотренные", "⭐ Оставить отзыв"],
            ["📊 Все отзывы", "❓ Помощь"]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        welcome_text = (
            f"Привет, {user.first_name}! 👋\n\n"
            "Добро пожаловать в Cinevate Nawi!\n"
            "Выберите действие из меню ниже:\n\n"
            "📝 Чтобы добавить просмотренный фильм, просто напишите его название боту!\n"
            "⭐ Напишите '/отзыв Название фильма: ваш текст' чтобы оставить отзыв"
        )
        
        await self.safe_send_message(update, welcome_text, reply_markup=reply_markup)
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик текстовых сообщений"""
        text = update.message.text
        
        if text == "🎬 Подобрать кино":
            await self.suggest_movie(update)
        elif text == "🎲 Случайный выбор":
            await self.random_movie(update)
        elif text == "❤️ Мой вкус":
            await self.my_taste(update)
        elif text == "🏆 Топ недели":
            await self.top_week(update)
        elif text == "📝 Мои просмотренные":
            await self.show_watched_movies(update)
        elif text == "⭐ Оставить отзыв":
            await self.leave_review_menu(update)
        elif text == "📊 Все отзывы":
            await self.show_all_reviews(update)
        elif text == "❓ Помощь":
            await self.help_command(update)
        elif text.startswith('/отзыв '):
            await self.process_review(update, text)
        elif text.startswith('/reviews '):
            await self.show_movie_reviews(update, text)
        elif text.startswith('/remove '):
            await self.remove_watched_movie(update, text[8:])
        else:
            # Если сообщение не команда, считаем его названием фильма
            await self.add_watched_movie(update, text)
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик callback-запросов"""
        query = update.callback_query
        await query.answer()
        
        if query.data == "back_to_menu":
            await self.start(query, context)
        elif query.data.startswith("rate_movie_"):
            parts = query.data.split('_')
            movie_title = parts[2]
            rating = int(parts[3])
            await self.rate_movie(query, movie_title, rating)
        elif query.data.startswith("show_reviews_"):
            movie_title = query.data[13:]
            await self.show_movie_reviews_callback(query, movie_title)
        elif query.data == "random_again":
            await self.random_movie_callback(query)
    
    async def random_movie(self, update: Update):
        """Случайный выбор фильма"""
        user_id = update.effective_user.id
        watched_movies = set()
        
        if r:
            watched_movies = r.smembers(self.get_watched_key(user_id))
        
        available_movies = [m for m in MOVIES_DATABASE.keys() if m not in watched_movies]
        
        if not available_movies:available_movies = list(MOVIES_DATABASE.keys())
        
        movie_title = random.choice(available_movies)
        movie_info = MOVIES_DATABASE[movie_title]
        
        text = (
            f"🎲 Держи случайный фильм-сюрприз!\n\n"
            f"🎬 {movie_title}\n"
            f"📀 {movie_info['genre']} | 🎬 {movie_info['type'].capitalize()}\n\n"
        )
        
        # Добавляем рейтинг если есть
        if r:
            rating_key = self.get_movie_rating_key(movie_title)
            rating = r.zscore(rating_key, movie_title)
            if rating:
                text += f"⭐ Рейтинг: {rating:.1f}/5\n\n"
        
        text += "Напишите название фильма если вы его уже смотрели!"
        
        # Кнопки для взаимодействия
        keyboard = [
            [InlineKeyboardButton("🔄 Еще случайный", callback_data="random_again")],
            [InlineKeyboardButton("⭐ Оценить этот фильм", callback_data=f"rate_movie_{movie_title}_0")],
            [InlineKeyboardButton("📝 Посмотреть отзывы", callback_data=f"show_reviews_{movie_title}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_send_message(update, text, reply_markup=reply_markup)
    
    # ... (остальные методы класса остаются без изменений, но везде добавляйте проверку на r) ...

async def weekly_reset():
    """Еженедельный сброс рейтинга"""
    while True:
        try:
            now = datetime.now()
            next_monday = now + timedelta(days=(7 - now.weekday()))
            next_monday = next_monday.replace(hour=0, minute=0, second=0, microsecond=0)
            
            wait_seconds = (next_monday - now).total_seconds()
            await asyncio.sleep(wait_seconds)
            
            if r:
                week_key = f"week:{next_monday.strftime('%Y-%U')}"
                weekly_rating_key = f"weekly_rating:{week_key}"
                r.delete(weekly_rating_key)
                
                logger.info(f"Еженедельный рейтинг сброшен для недели {week_key}")
        except Exception as e:
            logger.error(f"Ошибка в weekly_reset: {e}")
            await asyncio.sleep(3600)  # Ждем час перед повторной попыткой

async def keep_alive():
    """Функция для поддержания активности бота"""
    while True:
        try:
            # Просто ждем, но поддерживаем цикл активным
            await asyncio.sleep(300)  # 5 минут
            logger.debug("Keep-alive ping")
        except Exception as e:
            logger.error(f"Ошибка в keep_alive: {e}")

def main():
    """Запуск бота с обработкой ошибок и автоматическим перезапуском"""
    max_retries = 10
    retry_delay = 30
    
    for attempt in range(max_retries):
        try:
            # Задержка для избежания конфликта 409 при перезапуске
            if attempt > 0:
                time.sleep(min(retry_delay * attempt, 300))  # Максимум 5 минут ожидания
            
            application = Application.builder().token(BOT_TOKEN).build()
            
            bot = CinevateBot()
            
            # Добавляем обработчики
            application.add_handler(CommandHandler("start", bot.start))
            application.add_handler(CommandHandler("отзыв", bot.process_review))
            application.add_handler(CommandHandler("reviews", bot.show_movie_reviews))
            application.add_handler(CommandHandler("remove", bot.remove_watched_movie))
            application.add_handler(CallbackQueryHandler(bot.handle_callback))
            application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.handle_message))
            
            # Запускаем фоновые задачи
            loop = asyncio.get_event_loop()
            loop.create_task(weekly_reset())
            loop.create_task(keep_alive())
            
            # Запускаем бота с обработкой ошибок
            logger.info(f"Запуск бота (попытка {attempt + 1}/{max_retries})")
            
            # Для Render добавляем задержку перед запуском polling
            time.sleep(5)
            
            application.run_polling(poll_interval=3.0,
                timeout=30,
                drop_pending_updates=True,
                allowed_updates=None
            )
            
        except Conflict as e:
            logger.error(f"Конфликт (409): {e}. Перезапуск через {retry_delay} секунд...")
            if attempt == max_retries - 1:
                logger.critical("Достигнуто максимальное количество попыток. Бот остановлен.")
                break
                
        except (TimedOut, NetworkError) as e:
            logger.warning(f"Сетевая ошибка: {e}. Перезапуск через {retry_delay} секунд...")
            
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}. Перезапуск через {retry_delay} секунд...")
            if attempt == max_retries - 1:
                logger.critical("Достигнуто максимальное количество попыток. Бот остановлен.")
                break

if name == "__main__":
    # Для Render: добавляем задержку перед запуском
    if os.environ.get('RENDER'):
        time.sleep(5)
    
    main()
