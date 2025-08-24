import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, MenuButtonCommands
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
from collections import Counter

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Подключение к Redis
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')
try:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()  # Проверяем подключение
    logger.info("Успешное подключение к Redis")
except redis.ConnectionError:
    logger.error("Не удалось подключиться к Redis")
    r = None

# Токен вашего бота
BOT_TOKEN = os.environ.get('BOT_TOKEN', "8332015861:AAE5bTk5k0eyxb-GdPJy3dbxhOcoX9dOce4")

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
    "Стажёр (2015)": {"genre": "Комедия, драма", "type": "фильм"},
    "Побеждая Лондон (2001)": {"genre": "Комедия", "type": "фильм"},
    "Няньки (1994)": {"genre": "Комедия", "type": "фильm"},
    "Оторва (2008)": {"genre": "Комедия, романтика", "type": "фильм"},
    "Отель для собак (2009)": {"genre": "Комедия", "type": "фильм"},
    "Красотка (1990)": {"genre": "Комедия, романтика", "type": "фильм"},
    "Мистер и миссис Смит (2005)": {"genre": "Комедия, боевик", "type": "фильм"},
    "Чумовая пятница (2003)": {"genre": "Комедия", "type": "фильм"},
    "Мальчишник в Вегас (2009)": {"genre": "Комедия, приключения", "type": "фильм"},
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
    "В поисках Аляски (2019)": {"genre": "Драма", "type": "мини-сериал"},
    "Королевы крика (2015)": {"genre": "Ужасы", "type": "сериал"},
    "Очень странные дела (2016-2022)": {"genre": "Ужасы", "type": "сериал"},
    
    # Дополнительные популярные фильмы
    "Титаник (1997)": {"genre": "Драма, романтика", "type": "фильм"},
    "Матрица (1999)": {"genre": "Фантастика, боевик", "type": "фильм"},
    "Властелин колец: Братство кольца (2001)": {"genre": "Фэнтези, приключения", "type": "фильм"},
    "Гарри Поттер и философский камень (2001)": {"genre": "Фэнтези, приключения", "type": "фильм"},
    "Пираты Карибского моря: Проклятие Черной жемчужины (2003)": {"genre": "Приключения, фэнтези", "type": "фильм"},
    "Назад в будущее (1985)": {"genre": "Фантастика, комедия", "type": "фильм"},
    "Король Лев (1994)": {"genre": "Мультфильм, драма", "type": "фильм"},
    "Джентльмены (2019)": {"genre": "Криминал, комедия", "type": "фильм"},
    "Достать ножи (2019)": {"genre": "Детектив, комедия", "type": "фильm"},
    "Джокер (2019)": {"genre": "Криминал, драма", "type": "фильм"}
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
    
    async def set_menu_commands(self, application):
        """Установка команд меню"""
        commands = [
            ("start", "Главное меню"),
            ("help", "Помощь"),
            ("отзыв", "Оставить отзыв"),
            ("reviews", "Посмотреть отзывы"),
            ("remove", "Удалить фильм")
        ]
        await application.bot.set_my_commands(commands)
    
    async def safe_send_message(self, update: Update, text: str, **kwargs):
        """Безопасная отправка сообщения с обработкой ошибок"""
        try:
            if update.message:
                return await update.message.reply_text(text, **kwargs)
            else:
                return await update.callback_query.edit_message_text(text, **kwargs)
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения: {e}")
            return Noneasync def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, persistent=True)
        
        welcome_text = (
            f"Привет, {user.first_name}! 👋\n\n"
            "Добро пожаловать в Cinevate Nawi!\n"
            "Выберите действие из меню ниже:\n\n"
            "📝 Чтобы добавить просмотренный фильм, просто напишите его название боту!\n"
            "⭐ Напишите '/отзыв Название фильма: ваш текст' чтобы оставить отзыв\n\n"
            "🎬 У нас в базе: {} фильмов и сериалов!".format(len(MOVIES_DATABASE))
        )
        
        # Отправляем сообщение с меню
        await update.message.reply_text(welcome_text, reply_markup=reply_markup)
        
        # Дополнительно устанавливаем команды меню
        try:
            await context.bot.set_chat_menu_button(
                chat_id=update.effective_chat.id,
                menu_button=MenuButtonCommands()
            )
        except Exception as e:
            logger.warning(f"Не удалось установить меню: {e}")
    
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
        
        if r:watched_movies = r.smembers(self.get_watched_key(user_id))
        
        available_movies = [m for m in MOVIES_DATABASE.keys() if m not in watched_movies]
        
        if not available_movies:
            available_movies = list(MOVIES_DATABASE.keys())
        
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
    
    async def random_movie_callback(self, query):
        """Случайный выбор фильма из callback"""
        user_id = query.from_user.id
        watched_movies = set()
        
        if r:
            watched_movies = r.smembers(self.get_watched_key(user_id))
        
        available_movies = [m for m in MOVIES_DATABASE.keys() if m not in watched_movies]
        
        if not available_movies:
            available_movies = list(MOVIES_DATABASE.keys())
        
        movie_title = random.choice(available_movies)
        movie_info = MOVIES_DATABASE[movie_title]
        
        text = (
            f"🎲 Вот еще один случайный фильм!\n\n"
            f"🎬 {movie_title}\n"
            f"📀 {movie_info['genre']} | 🎬 {movie_info['type'].capitalize()}\n\n"
        )
        
        if r:
            rating_key = self.get_movie_rating_key(movie_title)
            rating = r.zscore(rating_key, movie_title)
            if rating:
                text += f"⭐ Рейтинг: {rating:.1f}/5\n\n"
        
        text += "Напишите название фильма если вы его уже смотрели!"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Еще случайный", callback_data="random_again")],
            [InlineKeyboardButton("⭐ Оценить этот фильм", callback_data=f"rate_movie_{movie_title}_0")],
            [InlineKeyboardButton("📝 Посмотреть отзывы", callback_data=f"show_reviews_{movie_title}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, reply_markup=reply_markup)
    
    async def suggest_movie(self, update: Update):
        """Подобрать кино по предпочтениям"""
        user_id = update.effective_user.id
        watched_movies = set()
        
        if r:
            watched_movies = r.smembers(self.get_watched_key(user_id))
        
        # Если нет просмотренных фильмов - предлагаем популярные
        if not watched_movies:
            await self.suggest_popular_movies(update)
            return
        
        # Анализируем предпочтения
        preferred_genres = []
        for movie in watched_movies:
            if movie in MOVIES_DATABASE:
                genre = MOVIES_DATABASE[movie]['genre']
                preferred_genres.extend([g.strip() for g in genre.split(',')])
        
        if not preferred_genres:
            await self.suggest_popular_movies(update)
            return
        
        # Находим самые популярные жанры
        genre_counter = Counter(preferred_genres)
        top_genres = [genre for genre, count in genre_counter.most_common(3)]
        
        # Ищем рекомендации
        recommended_movies = []for movie, info in MOVIES_DATABASE.items():
            if movie not in watched_movies:
                movie_genres = [g.strip() for g in info['genre'].split(',')]
                # Проверяем совпадение с топовыми жанрами
                common_genres = set(top_genres) & set(movie_genres)
                if common_genres:
                    recommended_movies.append((movie, len(common_genres)))
        
        if recommended_movies:
            # Сортируем по количеству совпадающих жанров
            recommended_movies.sort(key=lambda x: x[1], reverse=True)
            movie_title = recommended_movies[0][0]
            movie_info = MOVIES_DATABASE[movie_title]
            
            text = (
                f"🎬 Рекомендация based на ваших предпочтениях:\n\n"
                f"🎬 {movie_title}\n"
                f"📀 {movie_info['genre']} | 🎬 {movie_info['type'].capitalize()}\n\n"
                f"💡 Ваши любимые жанры: {', '.join(top_genres)}\n\n"
            )
            
            if r:
                rating_key = self.get_movie_rating_key(movie_title)
                rating = r.zscore(rating_key, movie_title)
                if rating:
                    text += f"⭐ Рейтинг: {rating:.1f}/5\n\n"
            
            keyboard = [
                [InlineKeyboardButton("⭐ Оценить", callback_data=f"rate_movie_{movie_title}_0")],
                [InlineKeyboardButton("📝 Отзывы", callback_data=f"show_reviews_{movie_title}")],
                [InlineKeyboardButton("🎲 Случайный выбор", callback_data="random_again")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(text, reply_markup=reply_markup)
        else:
            # Если нет рекомендаций - предлагаем популярные
            await self.suggest_popular_movies(update)
    
    async def suggest_popular_movies(self, update: Update):
        """Предложить популярные фильмы"""
        # Берем 5 случайных фильмов из базы
        popular_movies = random.sample(list(MOVIES_DATABASE.keys()), min(5, len(MOVIES_DATABASE)))
        
        text = "🎬 Популярные фильмы для просмотра:\n\n"
        for i, movie in enumerate(popular_movies, 1):
            movie_info = MOVIES_DATABASE[movie]
            text += f"{i}. {movie}\n"
            text += f"   📀 {movie_info['genre']}\n\n"
        
        text += "Выберите фильм и напишите его название, чтобы добавить в просмотренные!"
        
        await update.message.reply_text(text)
    
    async def my_taste(self, update: Update):
        """Мой вкус - статистика по просмотренным"""
        user_id = update.effective_user.id
        watched_movies = set()
        
        if r:
            watched_key = self.get_watched_key(user_id)
            watched_movies = r.smembers(watched_key)
        
        if not watched_movies:
            await update.message.reply_text(
                "❤️ Ваш вкус еще не сформирован!\n\n"
                "Начните добавлять просмотренные фильмы, "
                "и я смогу анализировать ваши предпочтения!"
            )
            return
        
        # Аналитика по жанрам
        genre_stats = {}
        type_stats = {}
        for movie in watched_movies:
            if movie in MOVIES_DATABASE:
                info = MOVIES_DATABASE[movie]
                genres = info['genre'].split(', ')
                for genre in genres:
                    genre_stats[genre] = genre_stats.get(genre, 0) + 1
                type_stats[info['type']] = type_stats.get(info['type'], 0) + 1
        
        text = (
            f"❤️ ВАШ КИНОВКУС:\n\n"
            f"📊 Всего просмотрено: {len(watched_movies)} фильмов\n\n"
        )
        
        if genre_stats:
            text += "🎭 Любимые жанры:\n"
            for genre, count in sorted(genre_stats.items(), key=lambda x: x[1], reverse=True)[:5]:
                percentage = (count / len(watched_movies)) * 100
                text += f"• {genre}: {count} фильмов ({percentage:.1f}%)\n"
        
        if type_stats:text += f"\n🎬 Предпочтения:\n"
            for type_, count in type_stats.items():
                percentage = (count / len(watched_movies)) * 100
                text += f"• {type_.capitalize()}: {count} ({percentage:.1f}%)\n"
        
        # Топ оцененных фильмов
        user_ratings = []
        if r:
            for movie in watched_movies:
                rating_key = f"user_rating:{user_id}:{movie}"
                rating = r.get(rating_key)
                if rating:
                    user_ratings.append((movie, int(rating)))
        
        if user_ratings:
            user_ratings.sort(key=lambda x: x[1], reverse=True)
            text += f"\n⭐ Ваши топ оценки:\n"
            for movie, rating in user_ratings[:3]:
                text += f"• {movie}: {rating}⭐\n"
        
        text += "\nПродолжайте смотреть хорошее кино! 🎬"
        
        await update.message.reply_text(text)
    
    async def top_week(self, update: Update):
        """Топ недели по фильмам"""
        if not r:
            await update.message.reply_text(
                "🏆 ТОП НЕДЕЛИ:\n\n"
                "Функция временно недоступна. Redis не подключен."
            )
            return
        
        weekly_key = self.get_weekly_rating_key()
        top_movies = r.zrange(weekly_key, 0, 9, desc=True, withscores=True)
        
        if not top_movies:
            await update.message.reply_text(
                "🏆 ТОП НЕДЕЛИ:\n\n"
                "Пока нет оценок на этой неделе!\n"
                "Станьте первым - оцените фильм! ✨"
            )
            return
        
        text = "🏆 ТОП ФИЛЬМОВ НЕДЕЛИ:\n\n"
        
        for i, (movie_title, score) in enumerate(top_movies, 1):
            movie_info = MOVIES_DATABASE.get(movie_title, {})
            genre = movie_info.get('genre', 'Неизвестно')
            type_ = movie_info.get('type', 'фильм')
            
            # Получаем количество оценок
            rating_key = self.get_movie_rating_key(movie_title)
            total_ratings = int(r.zcard(rating_key) or 0)
            
            text += (
                f"{i}. {movie_title}\n"
                f"   ⭐ {score:.1f} | 👥 {total_ratings} оценок\n"
                f"   🎭 {genre} | 🎬 {type_.capitalize()}\n\n"
            )
        
        text += "📅 Обновляется каждую неделю!\n"
        text += "⭐ Оценивайте фильмы чтобы повлиять на рейтинг!"
        
        await update.message.reply_text(text)
    
    async def add_watched_movie(self, update: Update, movie_title: str):
        """Добавление фильма в просмотренные"""
        user_id = update.effective_user.id
        movie_title = movie_title.strip()
        
        if len(movie_title) < 2:
            await update.message.reply_text("❌ Название фильма слишком короткое!")
            return
        
        # Проверяем, есть ли фильм в базе
        found_movie = None
        for db_movie in MOVIES_DATABASE:
            if movie_title.lower() in db_movie.lower():
                found_movie = db_movie
                break
        
        if not found_movie:
            # Показываем первые 5 фильмов для примера
            sample_movies = list(MOVIES_DATABASE.keys())[:5]
            await update.message.reply_text(
                f"❌ Фильм '{movie_title}' не найден в базе!\n"
                "Доступные фильмы:\n" + "\n".join(sample_movies) + "\n..."
            )
            return
        
        # Проверяем, нет ли уже такого фильма
        existing_movies = set()
        if r:
            watched_key = self.get_watched_key(user_id)
            existing_movies = r.smembers(watched_key)
        
        for existing_movie in existing_movies:
            if found_movie.lower() == existing_movie.lower():
                await update.message.reply_text(
                    f"❌ Фильм '{found_movie}' уже есть в вашем списке просмотренных!"
                )
                return
        
        # Добавляем фильм
        if r:
            r.sadd(watched_key, found_movie)
            total_watched = r.scard(watched_key)
        else:total_watched = len(existing_movies) + 1
        
        await update.message.reply_text(
            f"✅ Фильм добавлен в просмотренные!\n"
            f"🎬 {found_movie}\n\n"
            f"📊 Всего просмотрено: {total_watched} фильмов\n\n"
            f"Хотите оставить отзыв? Напишите:\n"
            f"/отзыв {found_movie}: ваш текст отзыва"
        )
    
    async def show_watched_movies(self, update: Update):
        """Показать просмотренные фильмы"""
        user_id = update.effective_user.id
        watched_movies = set()
        
        if r:
            watched_key = self.get_watched_key(user_id)
            watched_movies = r.smembers(watched_key)
        
        if not watched_movies:
            await update.message.reply_text(
                "📝 Ваш список просмотренных фильмов пуст!\n\n"
                "Просто напишите название фильма, который вы уже смотрели, "
                "и я добавлю его в ваш список!"
            )
            return
        
        sorted_movies = sorted(watched_movies)
        movies_list = "\n".join([f"• {movie}" for movie in sorted_movies[:20]])
        
        text = (
            f"📝 Ваши просмотренные фильмы ({len(sorted_movies)}):\n\n"
            f"{movies_list}"
        )
        
        if len(sorted_movies) > 20:
            text += f"\n\n... и еще {len(sorted_movies) - 20} фильмов!"
        
        text += "\n\nЧтобы удалить фильм, напишите /remove название_фильма"
        
        await update.message.reply_text(text)
    
    async def remove_watched_movie(self, update: Update, movie_title: str):
        """Удаление фильма из просмотренных"""
        user_id = update.effective_user.id
        
        if not r:
            await update.message.reply_text("❌ Функция временно недоступна.")
            return
        
        watched_key = self.get_watched_key(user_id)
        
        # Ищем точное совпадение
        removed = r.srem(watched_key, movie_title)
        
        if removed:
            await update.message.reply_text(
                f"✅ Фильм '{movie_title}' удален из просмотренных!"
            )
        else:
            await update.message.reply_text(
                f"❌ Фильм '{movie_title}' не найден в вашем списке!"
            )
    
    async def process_review(self, update: Update, text: str):
        """Обработка отзыва о фильме"""
        try:
            parts = text[7:].split(':', 1)
            if len(parts) < 2:
                await update.message.reply_text(
                    "❌ Неправильный формат! Используйте:\n"
                    "/отзыв Название фильма: ваш текст отзыва"
                )
                return
            
            movie_title = parts[0].strip()
            review_text = parts[1].strip()
            
            # Ищем фильм в базе
            found_movie = None
            for db_movie in MOVIES_DATABASE:
                if movie_title.lower() in db_movie.lower():
                    found_movie = db_movie
                    break
            
            if not found_movie:
                await update.message.reply_text(
                    f"❌ Фильм '{movie_title}' не найден в базе!"
                )
                return
            
            user_id = update.effective_user.id
            username = update.effective_user.first_name
            
            if r:
                # Сохраняем отзыв
                review_key = self.get_movie_reviews_key(found_movie)
                review_id = f"{user_id}:{datetime.now().timestamp()}"
                
                review_data = {
                    'user_id': user_id,
                    'username': username,
                    'movie': found_movie,
                    'text': review_text,
                    'timestamp': datetime.now().isoformat()
                }
                
                r.hset(review_key, review_id, json.dumps(review_data, ensure_ascii=False))
            
            # Кнопки для оценки
            keyboard = [
                [InlineKeyboardButton("⭐ 1", callback_data=f"rate_movie_{found_movie}_1"),
                    InlineKeyboardButton("⭐⭐ 2", callback_data=f"rate_movie_{found_movie}_2"),
                    InlineKeyboardButton("⭐⭐⭐ 3", callback_data=f"rate_movie_{found_movie}_3")
                ],
                [
                    InlineKeyboardButton("⭐⭐⭐⭐ 4", callback_data=f"rate_movie_{found_movie}_4"),
                    InlineKeyboardButton("⭐⭐⭐⭐⭐ 5", callback_data=f"rate_movie_{found_movie}_5")
                ],
                [InlineKeyboardButton("📝 Посмотреть отзывы", callback_data=f"show_reviews_{found_movie}")]
            ]
            reply_markup = Inline reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"✅ Отзыв на фильм '{found_movie}' сохранен!\n\n"
                f"💬 Ваш отзыв: {review_text}\n\n"
                "Хотите также оценить этот фильм?",
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error(f"Ошибка при обработке отзыва: {e}")
            await update.message.reply_text(
                "❌ Произошла ошибка при сохранении отзыва. Попробуйте еще раз."
            )
    
    async def leave_review_menu(self, update: Update):
        """Меню для оставления отзыва"""
        user_id = update.effective_user.id
        watched_movies = set()
        
        if r:
            watched_key = self.get_watched_key(user_id)
            watched_movies = r.smembers(watched_key)
        
        if not watched_movies:
            await update.message.reply_text(
                "⭐ ОСТАВИТЬ ОТЗЫВ\n\n"
                "У вас еще нет просмотренных фильмов!\n"
                "Сначала добавьте фильмы в свой список, написав их названия."
            )
            return
        
        # Показываем последние 10 просмотренных фильмов
        recent_movies = list(watched_movies)[-10:]
        movies_list = "\n".join([f"• {movie}" for movie in recent_movies])
        
        text = (
            "⭐ ОСТАВИТЬ ОТЗЫВ\n\n"
            "Чтобы оставить отзыв, напишите:\n"
            "/отзыв Название фильма: ваш текст\n\n"
            "Недавно просмотренные:\n"
            f"{movies_list}\n\n"
            "Или выберите любой другой фильм из вашего списка!"
        )
        
        await update.message.reply_text(text)
    
    async def rate_movie(self, query, movie_title: str, rating: int):
        """Оценка фильма"""
        user_id = query.from_user.id
        username = query.from_user.first_name
        
        if not r:
            await query.edit_message_text(
                "❌ Функция оценки временно недоступна."
            )
            return
        
        # Сохраняем оценку пользователя
        user_rating_key = f"user_rating:{user_id}:{movie_title}"
        r.set(user_rating_key, rating)
        
        # Обновляем общий рейтинг фильма
        rating_key = self.get_movie_rating_key(movie_title)
        r.zadd(rating_key, {movie_title: rating})
        
        # Обновляем недельный рейтинг
        weekly_key = self.get_weekly_rating_key()
        r.zincrby(weekly_key, rating, movie_title)
        
        await query.edit_message_text(
            f"✅ Спасибо за оценку!\n\n"
            f"🎬 {movie_title}\n"
            f"⭐ Ваша оценка: {rating}/5\n\n"
            f"Хотите оставить отзыв? Напишите:\n"
            f"/отзыв {movie_title}: ваш текст"
        )
    
    async def show_movie_reviews_callback(self, query, movie_title: str):
        """Показать отзывы на фильм из callback"""
        await self._show_movie_reviews(query, movie_title)
    
    async def show_movie_reviews(self, update: Update, text: str):
        """Показать отзывы на фильм из команды"""
        movie_title = text[9:].strip()
        await self._show_movie_reviews(update, movie_title)
    
    async def _show_movie_reviews(self, update, movie_title: str):
        """Внутренняя функция показа отзывов"""
        if not r:
            if hasattr(update, 'message'):await update.message.reply_text("❌ Функция временно недоступна.")
            else:
                await update.edit_message_text("❌ Функция временно недоступна.")
            return
        
        # Ищем фильм в базе
        found_movie = None
        for db_movie in MOVIES_DATABASE:
            if movie_title.lower() in db_movie.lower():
                found_movie = db_movie
                break
        
        if not found_movie:
            if hasattr(update, 'message'):
                await update.message.reply_text(f"❌ Фильм '{movie_title}' не найден!")
            else:
                await update.edit_message_text(f"❌ Фильм '{movie_title}' не найден!")
            return
        
        # Получаем отзывы
        review_key = self.get_movie_reviews_key(found_movie)
        reviews = r.hgetall(review_key)
        
        if not reviews:
            text = (
                f"📝 ОТЗЫВЫ НА ФИЛЬМ\n\n"
                f"🎬 {found_movie}\n\n"
                "Пока нет отзывов на этот фильм.\n"
                "Будьте первым - оставьте свой отзыв!\n\n"
                f"Напишите: /отзыв {found_movie}: ваш текст"
            )
        else:
            text = f"📝 ОТЗЫВЫ НА ФИЛЬМ\n\n🎬 {found_movie}\n\n"
            
            # Сортируем отзывы по времени (новые сначала)
            sorted_reviews = []
            for review_id, review_json in reviews.items():
                review_data = json.loads(review_json)
                sorted_reviews.append((review_data['timestamp'], review_data))
            
            sorted_reviews.sort(reverse=True)
            
            for timestamp, review in sorted_reviews[:5]:  # Показываем последние 5 отзывов
                dt = datetime.fromisoformat(timestamp)
                text += (
                    f"👤 {review['username']} ({dt.strftime('%d.%m.%Y')}):\n"
                    f"💬 {review['text']}\n\n"
                )
            
            if len(reviews) > 5:
                text += f"... и еще {len(reviews) - 5} отзывов\n\n"
        
        # Добавляем информацию о рейтинге
        rating_key = self.get_movie_rating_key(found_movie)
        rating_info = r.zrange(rating_key, 0, -1, withscores=True)
        
        if rating_info:
            total_ratings = len(rating_info)
            avg_rating = sum(score for _, score in rating_info) / total_ratings
            text += f"⭐ Средний рейтинг: {avg_rating:.1f}/5 ({total_ratings} оценок)\n\n"
        
        # Кнопки для действий
        keyboard = [
            [
                InlineKeyboardButton("⭐ Оценить", callback_data=f"rate_movie_{found_movie}_0"),
                InlineKeyboardButton("💬 Оставить отзыв", callback_data=f"review_{found_movie}")
            ],
            [InlineKeyboardButton("↩️ Назад", callback_data="back_to_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if hasattr(update, 'message'):
            await update.message.reply_text(text, reply_markup=reply_markup)
        else:
            await update.edit_message_text(text, reply_markup=reply_markup)
    
    async def show_all_reviews(self, update: Update):
        """Показать все отзывы пользователя"""
        user_id = update.effective_user.id
        username = update.effective_user.first_name
        
        if not r:
            await update.message.reply_text("❌ Функция временно недоступна.")
            return
        
        # Ищем все отзывы пользователя
        user_reviews = []
        pattern = "reviews:*"
        
        for key in r.scan_iter(match=pattern):
            reviews = r.hgetall(key)
            for review_id, review_json in reviews.items():
                review_data = json.loads(review_json)
                if review_data['user_id'] == user_id:
                    user_reviews.append(review_data)
        
        if not user_reviews:
            await update.message.reply_text(
                f"📝 ВАШИ ОТЗЫВЫ\n\n"
                f"👤 {username}, у вас пока нет отзывов!\n\n"
                "Оставьте свой первый отзыв командой:\n""/отзыв Название фильма: ваш текст"
            )
            return
        
        # Сортируем по времени
        user_reviews.sort(key=lambda x: x['timestamp'], reverse=True)
        
        text = f"📝 ВАШИ ОТЗЫВЫ\n\n👤 {username}\n\n"
        
        for i, review in enumerate(user_reviews[:10], 1):
            dt = datetime.fromisoformat(review['timestamp'])
            text += (
                f"{i}. 🎬 {review['movie']}\n"
                f"   📅 {dt.strftime('%d.%m.%Y %H:%M')}\n"
                f"   💬 {review['text'][:50]}...\n\n"
            )
        
        if len(user_reviews) > 10:
            text += f"... и еще {len(user_reviews) - 10} отзывов\n\n"
        
        text += "Чтобы посмотреть все отзывы на фильм, напишите /reviews название_фильма"
        
        await update.message.reply_text(text)
    
    async def help_command(self, update: Update):
        """Команда помощи"""
        help_text = (
            "🎬 CINIVATE NAWI - ПОМОЩЬ\n\n"
            "📝 КАК ПОЛЬЗОВАТЬСЯ:\n\n"
            "1. 📋 Добавьте просмотренные фильмы - просто напишите их названия\n"
            "2. ⭐ Оценивайте фильмы и оставляйте отзывы\n"
            "3. 🎬 Получайте персонализированные рекомендации\n\n"
            "📋 КОМАНДЫ:\n"
            "• /start - Главное меню\n"
            "• /отзыв Название: текст - Оставить отзыв\n"
            "• /reviews Название - Посмотреть отзывы\n"
            "• /remove Название - Удалить фильм\n"
            "• /help - Эта справка\n\n"
            "🎯 ФУНКЦИИ:\n"
            "• 🎬 Подбор фильмов по вашим предпочтениям\n"
            "• 🎲 Случайный выбор фильма\n"
            "• ❤️ Анализ вашего кинематографического вкуса\n"
            "• 🏆 Топ фильмов недели\n"
            "• 📝 Учет просмотренных фильмов\n\n"
            "💡 СОВЕТ: Чем больше фильмов вы добавите и оцените, "
            "тем точнее будут рекомендации!"
        )
        
        await update.message.reply_text(help_text)
    
    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик ошибок"""
        logger.error(f"Ошибка: {context.error}", exc_info=context.error)
        
        if update and update.effective_message:
            await update.effective_message.reply_text(
                "❌ Произошла непредвиденная ошибка. Попробуйте еще раз позже."
            )

async def main():
    """Основная функция"""
    # Создаем приложение и передаем токен
    application = Application.builder().token(BOT_TOKEN).build()
    bot = CinevateBot()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", bot.start))
    application.add_handler(CommandHandler("help", bot.help_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.handle_message))
    application.add_handler(CallbackQueryHandler(bot.handle_callback))
    
    # Обработчик ошибок
    application.add_error_handler(bot.error_handler)
    
    # Устанавливаем команды меню
    await bot.set_menu_commands(application)
    
    # Запускаем бота
    logger.info("Бот запущен!")
    try:
        await application.run_polling()
    except (Conflict, TimedOut, NetworkError) as e:
        logger.error(f"Ошибка сети: {e}")
        # Перезапуск через 5 секунд
        await asyncio.sleep(5)
        await main()
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise

if name == "__main__":
    asyncio.run(main())

