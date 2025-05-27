import joblib
import html
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.filters import Command
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
import re
from aiogram.exceptions import TelegramBadRequest
import os
import asyncio
from typing import Dict, Any
import asyncpg
import json
import logging
from datetime import datetime, timezone


class DatabaseManager:
    def __init__(self, dsn: str):
        """
        :param dsn: строка подключения к PostgreSQL
        Пример: 'postgresql://user:pass@ep-xyz.neon.tech/db?sslmode=require'
        """
        self.dsn = dsn

    async def _connect(self):
        return await asyncpg.connect(dsn=self.dsn)

    async def init_db(self):
        """Создаёт таблицы users и bad_words, если они не существуют"""
        conn = await self._connect()
        try:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    user_handle TEXT,
                    join_time TIMESTAMPTZ,
                    notified BOOLEAN,
                    status TEXT,
                    text JSONB,
                    text_id JSONB
                )
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS bad_words (
                    word TEXT PRIMARY KEY
                )
            ''')
        finally:
            await conn.close()

    async def save_users_to_db(self, users: Dict[int, Dict]):
        """
        Сохраняет пользователей в БД
        :param users: словарь {user_id: user_data}
        """
        conn = await self._connect()
        try:
            await conn.execute("TRUNCATE TABLE users")
            if not users:
                return

            records = []
            for user_id, data in users.items():
                records.append((
                    int(user_id),
                    data.get('username', ''),
                    data.get('user_handle', ''),
                    data.get('join_time', datetime.now(timezone.utc)),
                    data.get('notified', False),
                    data.get('status', 'unknown'),
                    json.dumps(data.get('text', [])),  # Сериализуем список в JSON
                    json.dumps(data.get('text_id', []))  # Сериализуем список в JSON
                ))

            await conn.executemany('''
                INSERT INTO users (
                    user_id, username, user_handle, join_time,
                    notified, status, text, text_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb)
            ''', records)
        finally:
            await conn.close()

    async def load_users_from_db(self) -> Dict[int, Dict]:
        """
        Загружает всех пользователей из БД
        :return: словарь {user_id: user_data}
        """
        conn = await self._connect()
        try:
            rows = await conn.fetch("SELECT * FROM users")
            users = {}
            for row in rows:
                users[row['user_id']] = {
                    'username': row['username'],
                    'user_handle': row['user_handle'],
                    'join_time': row['join_time'],
                    'notified': row['notified'],
                    'status': row['status'],
                    'text': json.loads(row['text']) if row['text'] else [],  # Десериализуем JSON в список
                    'text_id': json.loads(row['text_id']) if row['text_id'] else []  # Десериализуем JSON в список
                }
            return users
        finally:
            await conn.close()

    async def save_bad_words(self, bad_words):
        """
        Сохраняет список плохих слов в БД
        :param bad_words: список запрещённых слов
        """
        conn = await self._connect()
        try:
            await conn.execute("TRUNCATE TABLE bad_words")
            records = [(word.lower(),) for word in bad_words]
            await conn.executemany('INSERT INTO bad_words (word) VALUES ($1)', records)
        finally:
            await conn.close()

    async def load_bad_words(self) :
        """
        Загружает список плохих слов из БД
        :return: список запрещённых слов
        """
        conn = await self._connect()
        try:
            rows = await conn.fetch('SELECT word FROM bad_words')
            return [row['word'] for row in rows]
        finally:
            await conn.close()

class TextProcessor:
    @staticmethod
    def fix_markdown(text: str) -> str:
        special_chars = r"_*[]()~`>#+-=|{}.!"

        parts = re.split(r'(```)', text)

        for i in range(len(parts)):
            if parts[i] != '```':
                parts[i] = re.sub(r'([{}])'.format(re.escape(special_chars)), r'\\\1', parts[i])

        text = "".join(parts)
        def escape_markdown_links(m):
            part1 = m.group(1)
            part2 = m.group(2).replace(")", "\\)")
            return f"[{part1}]\\({part2}\\)"

        text = re.sub(r"\[(.*?)\]\((.*?)\)", escape_markdown_links, text)
        # text = re.sub(r"\[(.*?)\]\((.*?)\)", lambda m: f"[{m.group(1)}]\\({m.group(2).replace(')', '\\)')}\\)", text)
        return text

    @staticmethod
    def normalize_text(text):
        if not isinstance(text, str):
            logging.warning(f"normalize_text получила не строку: {type(text)}. Попытка преобразования.")
            text = str(text)

        text = text.lower()
        text = ''.join(ch for ch in text if ch.isalpha() or ch in ['#', ' ', '?', '\n'])
        return text

    @staticmethod
    def count_non_latin_cyrillic(text):
        if not isinstance(text, str):
            return 0
        return len(re.findall(r'[^a-zA-Zа-яА-ЯёЁ\s.,!?0-9]', text))
class SpamDetector:
    def __init__(self):
        self.vectorizer = None
        self.model = None
        self._load_models()

    def _load_models(self):
        try:
            self.vectorizer = joblib.load("vectorSPAM.joblib")
            self.model = joblib.load("modelSPAM.joblib")
        except FileNotFoundError:
            logging.error("Ошибка: Файлы модели 'vectorSPAM.joblib' или 'modelSPAM.joblib' не найдены.")
        except Exception as e:
            logging.error(f"Ошибка загрузки моделей: {e}", exc_info=True)

    def make_spam_prediction(self, sample_text):
        if not self.vectorizer or not self.model:
            logging.warning("Модели для предсказания спама не загружены.")
            return 0

        try:
            norm_text = TextProcessor.normalize_text(sample_text)
            sample_tfidf = self.vectorizer.transform([norm_text])
            prediction = self.model.predict(sample_tfidf)
            return int(prediction[0])
        except Exception as e:
            logging.error(f"Ошибка в предсказании: {e}", exc_info=True)
            return 0
class UserManager:
    def __init__(self):
        self.new_users: Dict[int, Dict[str, Any]] = {}  # Добавим type hint для ясности
        self.new_users_lock = asyncio.Lock()

        self.bad_nicknames = []

    async def add_user(self, user_id, user_data):
        async with self.new_users_lock:
            self.new_users[user_id] = user_data

    async def remove_user(self, user_id):
        async with self.new_users_lock:
            return self.new_users.pop(user_id, None)

    async def update_user_text(self, user_id, text, text_id):
        async with self.new_users_lock:
            if user_id in self.new_users:
                self.new_users[user_id]['text'].append(text)
                self.new_users[user_id]['text_id'].append(text_id)

    async def add_bad_nickname(self, nickname):
        if nickname not in self.bad_nicknames:
            self.bad_nicknames.append(nickname)
            return True
        return False

    async def get_bad_nicknames(self):
        return self.bad_nicknames

    async def clean_old_users(self):
        now = datetime.now(timezone.utc)
        removed_count = 0
        user_ids_to_remove = [] # Собираем ID для удаления вне итерации
        async with self.new_users_lock:
            # Сначала собираем ID для удаления
            for user_id, data in self.new_users.items():
                join_time = data.get("join_time")
                if isinstance(join_time, datetime):
                     # Убедимся, что время в UTC
                    join_time_utc = join_time.astimezone(timezone.utc)
                    if (now - join_time_utc).total_seconds() > 86400: # 24 часа
                        user_ids_to_remove.append(user_id)
                else:
                    logging.warning(f"Некорректное время join_time у user_id {user_id}, пользователь будет удален.")
                    user_ids_to_remove.append(user_id) # Удаляем пользователей с невалидным временем

            # Теперь удаляем по собранным ID
            for user_id in user_ids_to_remove:
                if user_id in self.new_users: # Доп. проверка на всякий случай
                    del self.new_users[user_id]
                    removed_count += 1
                    logging.info(f"Удален старый пользователь {user_id} из отслеживания.")

        return removed_count

    async def update_user_status(self, user_id, new_status):
        """Обновляет статус пользователя по его ID"""
        async with self.new_users_lock:
            if user_id in self.new_users:
                self.new_users[user_id]['status'] = new_status
                return True
            return False

        # !!! НОВЫЙ МЕТОД !!!

    async def get_all_users_data(self) -> dict:
        """Возвращает копию словаря отслеживаемых пользователей."""
        async with self.new_users_lock:
            return self.new_users.copy()  # Возвращаем копию для безопасности
class TelegramBot:
    def __init__(self):
        self.bot = Bot(token=os.environ.get("TELEGRAM_BOT_TOKEN"))
        self.dp = Dispatcher()
        self.router = Router()
        self.dp.include_router(self.router)

        self.master_id = int(os.environ.get("MASTER_ID"))
        self.target_chat_id = int(os.environ.get("TARGET_CHAT_ID"))
        self.webhook_url = os.environ.get("WEBHOOK_URL")
        self.webhook_path = f"/{os.environ.get('TELEGRAM_BOT_TOKEN')}"


        self.spam_detector = SpamDetector()
        dsn=os.environ.get("DSN")
        self.db_manager = DatabaseManager(dsn)
        # bad_words=await self.db_manager.load_bad_words()
        #
        self.user_manager = UserManager()
        self._register_handlers()

    def _register_handlers(self):
        self.router.message(Command(commands=['suka']))(self.add_nickname)
        self.router.message(Command(commands=['list']))(self.show_nicknames)
        self.router.message(Command(commands=['strange']))(self.show_strange)
        self.router.message(Command(commands=['restrict']))(self.restrict)
        self.router.chat_member()(self.handle_chat_member)
        self.router.message(F.text)(self.handle_message)
        self.router.message(F.photo | F.video | F.audio | F.document |
                            F.sticker | F.location | F.contact |
                            F.animation | F.voice | F.video_note)(self.handle_media)

        self.dp.startup.register(self.on_startup)
        self.dp.shutdown.register(self.on_shutdown)

    async def add_nickname(self, message: types.Message):
        if message.from_user.id != self.master_id:
            return

        command_parts = message.text.split(maxsplit=1)
        if len(command_parts) < 2 or not command_parts[1]:
            await message.reply("Пожалуйста, укажите никнейм после команды /suka")
            return

        nickname = command_parts[1]
        if await self.user_manager.add_bad_nickname(nickname):
            await message.reply(f"Никнейм '{nickname}' добавлен в список плохих.")
            logging.info(f"Мастер {message.from_user.id} добавил никнейм: {nickname}")
        else:
            await message.reply(f"Никнейм '{nickname}' уже есть в списке.")

    async def show_nicknames(self, message: types.Message):
        if message.from_user.id != self.master_id:
            return

        bad_nicknames = await self.user_manager.get_bad_nicknames()
        if not bad_nicknames:
            await message.reply("Список плохих никнеймов пуст.")
        else:
            response = "Список плохих никнеймов:\n- " + "\n- ".join(bad_nicknames)
            await message.reply(response)

    def format_users_message(self,users_data):

        message_lines = ["📝 Пользователи с текстом сообщений:\n"]

        for user_id, data in users_data.items():
            if data['text'] and data["status"]=="member":  # если есть текст
                    # Форматируем время в читаемый вид
                join_time = data['join_time'].strftime('%Y-%m-%d %H:%M:%S') if isinstance(data['join_time'],
                                                                                              datetime) else data[
                        'join_time']

                    # Экранируем специальные символы для Markdown
                username = data['username'].replace('*', '\\*').replace('_', '\\_')
                user_handle = data['user_handle'].replace('*', '\\*').replace('_', '\\_')

                message_lines.append(
                    f"🔹 *ID*: `{user_id}`\n"
                    f"   *Имя*: {username}\n"
                    f"   *Ник*: {user_handle}\n"
                    f"   *Вступил*: `{join_time}`\n"
                    f"   *Сообщения* ({len(data['text'])}):\n"
                    )

                for i, text in enumerate(data['text'], 1):
                    escaped_text = text.replace('`', '\\`').replace('*', '\\*')
                    message_lines.append(
                        f"      {i}. `{escaped_text[:100]}{'...' if len(escaped_text) > 100 else ''}`\n")

                message_lines.append("\n")

        if len(message_lines) == 1:  # если только заголовок
            return "🤷 Нет пользователей с текстом сообщений."

        return "".join(message_lines)
    async def restrict(self, message: types.Message):
        # Проверяем права отправителя (только админ)
        if message.from_user.id != self.master_id:  # MASTER_ID - ваш ID
            await message.reply("⛔ У вас нет прав на эту команду")
            return

        # Получаем список ID из сообщения
        try:
            user_ids = [int(uid.strip()) for uid in message.text.split()[1].split(',')]
        except (IndexError, ValueError):
            await message.reply("❌ Неверный формат. Пример: /restrict 12345,67890,112233")
            return

        results = []
        success_count = 0
        fail_count = 0

        # Ограничиваем каждого пользователя
        for user_id in user_ids:
            try:
                await message.bot.restrict_chat_member(
                    chat_id=self.target_chat_id,  # ID вашего чата
                    user_id=user_id,
                    permissions=types.ChatPermissions(
                        can_send_messages=False,
                        can_send_media_messages=False,
                        can_send_polls=False,
                        can_send_other_messages=False,
                        can_add_web_page_previews=False,
                        can_change_info=False,
                        can_invite_users=False,
                        can_pin_messages=False
                    )
                )
                success_count += 1
                results.append(f"✅ {user_id} - успешно ограничен")
            except TelegramBadRequest as e:
                fail_count += 1
                if "user not found" in str(e):
                    results.append(f"❌ {user_id} - пользователь не найден")
                elif "not enough rights" in str(e):
                    results.append(f"❌ {user_id} - нет прав для ограничения")
                else:
                    results.append(f"❌ {user_id} - ошибка: {e}")
            except Exception as e:
                fail_count += 1
                results.append(f"❌ {user_id} - непредвиденная ошибка: {e}")

        # Формируем итоговый отчет
        report = [
            f"🔹 Результат ограничения пользователей:",
            f"Успешно: {success_count}",
            f"Не удалось: {fail_count}",
            "",
            *results
        ]

        # Отправляем отчет частями если слишком длинный
        current_message = []
        for line in report:
            if len('\n'.join(current_message + [line])) > 4000:
                await message.reply('\n'.join(current_message))
                current_message = [line]
            else:
                current_message.append(line)

        if current_message:
            await message.reply('\n'.join(current_message))

    async def show_strange(self, message: types.Message):
        if message.from_user.id != self.master_id:
            return


        # async with self.user_manager.new_users_lock:
        #     users_data=self.user_manager.new_users
        # print(self.user_manager.new_users)
        users_with_text=self.format_users_message(self.user_manager.new_users)

        # print(users_with_text)
        await self.bot.send_message(
            chat_id=self.master_id,
            text=users_with_text,
            parse_mode="Markdown"
        )





        # self.router.message(Command(commands=['strange']))(self.show_strange)

    async def handle_chat_member(self, update: types.ChatMemberUpdated):
        new_status = update.new_chat_member.status
        user = update.new_chat_member.user
        user_id = user.id
        username = user.full_name
        user_handle = f"@{user.username}" if user.username else "Нет username"
        chat_id = update.chat.id
        event_time_dt = update.date

        if chat_id == self.target_chat_id:
            if new_status == "member":
                await self.user_manager.add_user(user_id, {
                    'username': username,
                    'user_handle': user_handle,
                    'join_time': event_time_dt,
                    'notified': False,
                    'status': new_status,
                    'text': [],
                    'text_id': []
                })
                logging.info(f"Новый участник {username} ({user_id}) добавлен в список отслеживания.")
                try:
                    await self.bot.send_message(
                        self.master_id,
                        f"Новый участник в целевом чате:\nИмя: {username}\nХэндл: {user_handle}\nID: {user_id}"
                    )
                except Exception as e:
                    logging.error(f"Ошибка отправки мастеру уведомления о новом участнике: {e}", exc_info=True)

                name_to_check = user.username or ""
                full_name_to_check = username
                bad_nicknames = await self.user_manager.get_bad_nicknames()
                if any(bad.lower() in name_to_check.lower() for bad in bad_nicknames) or \
                        any(bad.lower() in full_name_to_check.lower() for bad in bad_nicknames):
                    logging.warning(
                        f"Обнаружен плохой никнейм/имя у нового участника: {username} ({user_handle}, {user_id}). Попытка бана.")
                    try:
                        await self.bot.ban_chat_member(self.target_chat_id, user_id)
                        await self.bot.send_message(
                            self.master_id,
                            f"⚠️ Автоматически забанен пользователь с плохим ником/именем:\nИмя: {username}\nХэндл: {user_handle}\nID: {user_id}"
                        )
                        logging.info(f"Пользователь {username} ({user_id}) забанен из-за ника/имени.")
                        await self.user_manager.remove_user(user_id)
                    except Exception as e:
                        logging.error(f"Ошибка бана пользователя {user_id} с плохим ником/именем: {e}", exc_info=True)
                        await self.bot.send_message(
                            self.master_id,
                            f"❌ Не удалось забанить пользователя {username} ({user_id}) с плохим ником/именем. Ошибка: {e}"
                        )
            elif new_status=="restricted":
                await self.user_manager.update_user_status( user_id, new_status)

    async def handle_message(self, message: types.Message):
        sender_id = message.from_user.id
        chat_id = message.chat.id
        text = message.text or ""  # Гарантируем строку

        # Получаем данные пользователя (если он отслеживается)
        user_data = None
        async with self.user_manager.new_users_lock:  # Блокируем доступ на время чтения
            if sender_id in self.user_manager.new_users:
                # Получаем копию, чтобы работать с ней вне блока lock
                user_data = self.user_manager.new_users.get(sender_id)  # Используем .get для безопасности

        # Если пользователя нет в словаре новых, обрабатываем как старого
        if not user_data:
            logging.debug(f"Сообщение от неотслеживаемого пользователя {sender_id}")
            # Ваш код для обработки старых пользователей (например, пересылка админу)
            # final_msg_to_master = ...
            # await self.bot.send_message(...)
            final_msg_to_master = (
                f"ID message: {message.message_id}\n"
                f"ID sender: {sender_id}\n"
                f"User Name: {message.from_user.username}\n"
                f"Текст:\n```\n{text}\n```\n"
            )
            final_msg_to_master = TextProcessor.fix_markdown(final_msg_to_master)
            await self.bot.send_message(self.master_id, final_msg_to_master, parse_mode="Markdown")
            return  # Прекращаем обработку здесь

        # Сколько времени прошло после присоеденения юзера
        time_joined = user_data.get("join_time", datetime.now(timezone.utc))
        if time_joined.tzinfo is None:  # Убедимся что есть таймзона
            time_joined = time_joined.replace(tzinfo=timezone.utc)
        time_passed = datetime.now(timezone.utc) - time_joined



        entities = message.entities or message.caption_entities  # Берём entities текста или подписи

        if  entities:
            # Формирование отчета для админа (добавим флаг повтора)
            msg_to_master_lines = [
                f"Сообщение от нового пользователя ({str(time_passed).split('.')[0]} после входа):",
                f"ID: {sender_id}",
                f"Имя: {user_data.get('username', 'N/A')}",
                f"Хэндл: {user_data.get('user_handle', 'N/A')}",
                f"Текст (ID: {message.message_id}):\n```\n{text}\n```",  # Не экранируем Markdown для логов/принта
                f"Entities"
            ]
            await message.delete()
            permissions = types.ChatPermissions(can_send_messages=False)
            await self.bot.restrict_chat_member(chat_id, sender_id, permissions=permissions)
            logging.info(f"Пользователь {sender_id} ограничен (только чтение). Причина: entities")
            # Обновляем статус в менеджере ПОСЛЕ успешного ограничения
            await self.user_manager.update_user_status(sender_id, "restricted")

            final_msg_to_master = "\n".join(msg_to_master_lines)
            # Экранируем для Markdown (TextProcessor.fix_markdown - ваша функция)
            formatted_report = TextProcessor.fix_markdown(final_msg_to_master)
            await self.bot.send_message(self.master_id, formatted_report, parse_mode="Markdown")

            return

        # Анализ сообщения
        non_latin_cyrillic_count = TextProcessor.count_non_latin_cyrillic(text)

        # --- Обработка нового пользователя ---

        if non_latin_cyrillic_count > 5:  # Используйте константу из config
            msg_to_master_lines = [
                f"Сообщение от нового пользователя ({str(time_passed).split('.')[0]} после входа):",
                f"ID: {sender_id}",
                f"Имя: {user_data.get('username', 'N/A')}",
                f"Хэндл: {user_data.get('user_handle', 'N/A')}",
                f"Текст (ID: {message.message_id}):\n```\n{text}\n```",  # Не экранируем Markdown для логов/принта
                f"No cyrrilic count"
            ]
            await message.delete()
            permissions = types.ChatPermissions(can_send_messages=False)
            await self.bot.restrict_chat_member(chat_id, sender_id, permissions=permissions)
            logging.info(f"Пользователь {sender_id} ограничен (только чтение). Причина: no cirylic")
            # Обновляем статус в менеджере ПОСЛЕ успешного ограничения
            await self.user_manager.update_user_status(sender_id, "restricted")

            final_msg_to_master = "\n".join(msg_to_master_lines)
            # Экранируем для Markdown (TextProcessor.fix_markdown - ваша функция)
            formatted_report = TextProcessor.fix_markdown(final_msg_to_master)
            await self.bot.send_message(self.master_id, formatted_report, parse_mode="Markdown")

            return

        # !!! Исправленная проверка на повторение !!!
        text_repeated = False
        min_repetition_count = 3  # Порог срабатывания
        # Получаем историю текстов и ID сообщений

        messages_history = user_data.get('text', [])
        message_ids_history = user_data.get('text_id', [])

        # print("user_data", user_data)
        # print("text:", text)
        # print("messages_history:", messages_history)
#
        # Проверяем, если накопилось достаточно сообщений для проверки + текущее
        if len(messages_history) >= min_repetition_count - 1:
            # Берем последние N-1 сообщений из истории и добавляем текущее
            recent_messages_to_check = messages_history[-(min_repetition_count - 1):] + [text]
            # print("user_data",user_data)
            # print("text:",text)
            # print("messages_history:", messages_history)
            # recent_messages_to_check = messages_history[-(min_repetition_count - 1):] + [text]
            # Если все сообщения в этом срезе одинаковы
            if len(set(recent_messages_to_check)) == 1:
                text_repeated = True
                logging.warning(f"Обнаружено {min_repetition_count} одинаковых сообщения подряд от {sender_id}.")

        if text_repeated:
            msg_to_master_lines = [
                f"Сообщение от нового пользователя ({str(time_passed).split('.')[0]} после входа):",
                f"ID: {sender_id}",
                f"Имя: {user_data.get('username', 'N/A')}",
                f"Хэндл: {user_data.get('user_handle', 'N/A')}",
                f"Текст (ID: {message.message_id}):\n```\n{text}\n```",  # Не экранируем Markdown для логов/принта
                f"3 messages the same"
            ]
            deleted_count = 0
            logging.warning(f"Удаление предыдущих сообщений ({message_ids_history}) от {sender_id} из-за повторов.")
            for msg_id in message_ids_history:
                try:
                    await self.bot.delete_message(chat_id, msg_id)
                    deleted_count += 1
                except TelegramBadRequest as e:
                    if "message to delete not found" in str(e):
                        logging.info(f"Предыдущее сообщение {msg_id} от {sender_id} уже удалено.")
                    else:
                        logging.error(f"Ошибка удаления предыдущего сообщения {msg_id} от {sender_id}: {e}")
            await self.user_manager.update_user_status(sender_id, "restricted")

            final_msg_to_master = "\n".join(msg_to_master_lines)
            # Экранируем для Markdown (TextProcessor.fix_markdown - ваша функция)
            formatted_report = TextProcessor.fix_markdown(final_msg_to_master)
            await self.bot.send_message(self.master_id, formatted_report, parse_mode="Markdown")

            return
        spam_prediction = self.spam_detector.make_spam_prediction(text)
        if spam_prediction == 1:
            msg_to_master_lines = [
                f"Сообщение от нового пользователя ({str(time_passed).split('.')[0]} после входа):",
                f"ID: {sender_id}",
                f"Имя: {user_data.get('username', 'N/A')}",
                f"Хэндл: {user_data.get('user_handle', 'N/A')}",
                f"Текст (ID: {message.message_id}):\n```\n{text}\n```",  # Не экранируем Markdown для логов/принта
                f"SPAMt"
            ]

            await message.delete()
            permissions = types.ChatPermissions(can_send_messages=False)
            await self.bot.restrict_chat_member(chat_id, sender_id, permissions=permissions)
            logging.info(f"Пользователь {sender_id} ограничен (только чтение). Причина: SPAM")
            # Обновляем статус в менеджере ПОСЛЕ успешного ограничения
            await self.user_manager.update_user_status(sender_id, "restricted")

            final_msg_to_master = "\n".join(msg_to_master_lines)
            # Экранируем для Markdown (TextProcessor.fix_markdown - ваша функция)
            formatted_report = TextProcessor.fix_markdown(final_msg_to_master)
            await self.bot.send_message(self.master_id, formatted_report, parse_mode="Markdown")
            return


    async def handle_media(self, message: types.Message):
        user_id = message.from_user.id
        chat_id = message.chat.id

        if chat_id != self.target_chat_id:
            return

        user_data = None
        is_new_user = False
        async with self.user_manager.new_users_lock:
            if user_id in self.user_manager.new_users:
                user_data = self.user_manager.new_users[user_id]
                is_new_user = True

        if not is_new_user:
            return

        logging.info(
            f"Новый пользователь {user_id} ({user_data['username']}) отправил медиа ({message.content_type}). Удаление.")
        try:
            await self.user_manager.update_user_text(user_id, "medioFuck", message.message_id)
            await message.delete()
            await self.bot.send_message(
                self.master_id,
                f"🗑 Медиа ({message.content_type}) от нового пользователя {user_data['username']} ({user_id}) было удалено."
            )
        except Exception as e:
            logging.error(f"Ошибка удаления медиа от {user_id}: {e}", exc_info=True)
            try:
                await self.bot.send_message(
                    self.master_id,
                    f"❌ Не удалось удалить медиа ({message.content_type}) от {user_id}. Ошибка: {e}"
                )
            except Exception as e_inner:
                logging.error(f"Не удалось отправить сообщение об ошибке мастеру: {e_inner}", exc_info=True)

    async def send_hourly_report(self):
        """Фоновая задача для очистки старых пользователей и сохранения БД."""
        while True:
            await asyncio.sleep(3600)  # 1 час
            logging.info("Запуск ежечасной задачи очистки и сохранения...")
            try:
                removed_count = await self.user_manager.clean_old_users()
                if removed_count > 0:
                    logging.info(f"Ежечасная очистка: удалено {removed_count} пользователей.")
                else:
                    logging.info("Ежечасная очистка: нет пользователей для удаления.")

                # !!! Получаем АКТУАЛЬНОЕ состояние пользователей ПОСЛЕ очистки !!!
                current_users_state = await self.user_manager.get_all_users_data()
                # !!! Сохраняем это состояние в БД !!!
                await self.db_manager.save_users_to_db(current_users_state)
                await self.db_manager.save_bad_words(self.user_manager.bad_nicknames)
                logging.info(f"Ежечасное сохранение {len(current_users_state)} пользователей в БД GCS выполнено.")

            except Exception as e:  # Ловим любые ошибки из clean_old_users или save_users_to_db
                logging.error(f"Ошибка в фоновой задаче send_hourly_report: {e}", exc_info=True)
                # Можно добавить уведомление админу об ошибке фоновой задачи

    async def on_startup(self, bot: Bot):
        """Действия при запуске бота."""
        log_message = "Загрузка данных пользователей при старте..."
        logging.info(log_message)
        print(log_message)
        try:
            # Загружаем пользователей из БД в UserManager
            await self.db_manager.init_db()  # создает таблицу, если её нет
            loaded_users = await self.db_manager.load_users_from_db()
            # loaded_users={}
            # !!! Используем блокировку для инициализации !!!
            async with self.user_manager.new_users_lock:
                self.user_manager.new_users = loaded_users
            log_message = f"Загружено {len(loaded_users)} пользователей из БД."
            logging.info(log_message)
            self.user_manager.bad_nicknames= await self.db_manager.load_bad_words()
            print(log_message)
        except Exception as e:
            log_message = f"КРИТИЧЕСКАЯ ОШИБКА загрузки БД при старте: {e}"
            logging.error(log_message, exc_info=True)
            print(log_message)
            # Решите, нужно ли останавливать бота, если БД не загрузилась
            # await self.bot.send_message(self.master_id, log_message) # Уведомить админа
            # return # Возможно, стоит прервать запуск

        # await asyncio.sleep(1)  # Небольшая пауза
        if self.webhook_url:
            try:
                await bot.set_webhook(f"{self.webhook_url}{self.webhook_path}", drop_pending_updates=True)
                log_message = f"Вебхук установлен на: {self.webhook_url}{self.webhook_path}"
                logging.info(log_message)
                print(log_message)
                await bot.send_message(self.master_id, "Бот запущен/перезапущен и вебхук установлен.")
            except Exception as e:
                log_message = f"⚠️ Ошибка установки вебхука: {e}"
                logging.error(log_message, exc_info=True)
                print(log_message)
                await bot.send_message(self.master_id, log_message)
        else:
            log_message = "WEBHOOK_URL не задан. Запуск в режиме polling."
            logging.warning(log_message)
            print(log_message)
            await bot.delete_webhook(drop_pending_updates=True)  # Убедимся, что вебхук снят
            await bot.send_message(self.master_id, "Бот запущен/перезапущен в режиме polling (БЕЗ вебхука).")

    async def on_shutdown(self, bot: Bot):
        """Действия при остановке бота."""
        log_message = "Бот останавливается..."
        logging.warning(log_message)
        print(log_message)
        # Сначала пытаемся отправить сообщение об остановке
        try:
            await bot.send_message(self.master_id, "Бот останавливается... Сохранение данных.")
        except Exception as e:
            logging.error(f"Не удалось отправить сообщение об остановке мастеру: {e}", exc_info=True)

        # !!! Получаем ПОСЛЕДНЕЕ АКТУАЛЬНОЕ состояние пользователей !!!
        final_users_state = await self.user_manager.get_all_users_data()
        log_message = f"Сохранение {len(final_users_state)} пользователей в БД перед выключением..."
        logging.info(log_message)
        print(log_message)
        try:
            # !!! Сохраняем это состояние в БД !!!
            await self.db_manager.save_users_to_db(final_users_state)
            await self.db_manager.save_bad_words(self.user_manager.bad_nicknames)
            log_message = "Данные пользователей успешно сохранены."
            logging.info(log_message)
            print(log_message)
            # Уведомляем ПОСЛЕ успешного сохранения (если предыдущее не удалось)
            # await bot.send_message(self.master_id, "Данные сохранены. Бот выключен.")
        except Exception as e:
            log_message = f"⚠️ КРИТИЧЕСКАЯ ОШИБКА сохранения БД при выключении: {e}"
            logging.error(log_message, exc_info=True)
            print(log_message)
            # Попытаться еще раз уведомить админа
            try:
                await bot.send_message(self.master_id, log_message)
            except Exception:
                pass

        # Закрываем сессию бота (если используется ClientSession)
        if bot.session:
            await bot.session.close()
            logging.info("Сессия бота закрыта.")
        log_message = "Бот полностью остановлен."
        logging.info(log_message)
        print(log_message)


    async def handle_index(self, request):
        """Обработчик для корневого URL ('/')"""
        html_body_parts = ["<h1>Содержимое моего словаря:</h1>"]
        html_body_parts.append("<ul>")

        async with self.user_manager.new_users_lock:
            for key, value in self.user_manager.new_users.items():
                safe_key = html.escape(str(key))
                safe_value = html.escape(str(value))
                html_body_parts.append(f"<li><strong>{safe_key}:</strong> {safe_value}</li>")

        html_body_parts.append("</ul>")

        html_content = f"""
        <!DOCTYPE html>
        <html lang="ru">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Мои Данные</title>
            <style>
                body {{ font-family: sans-serif; }}
                li {{ margin-bottom: 5px; }}
                strong {{ color: #333; }}
            </style>
        </head>
        <body>
            {''.join(html_body_parts)}
        </body>
        </html>
        """

        return web.Response(text=html_content, content_type='text/html', charset='utf-8')
    async def health_check(self, request):
        return web.Response(text="OK", status=200)

    async def main(self):
        # self.dp.startup.register(self.on_startup)
        # self.dp.shutdown.register(self.on_shutdown)

        app_aiogram = web.Application()
        hourly_task = asyncio.create_task(self.send_hourly_report())
        logging.info("Фоновая задача очистки пользователей запущена.")

        webhook_requests_handler = SimpleRequestHandler(
            dispatcher=self.dp,
            bot=self.bot,
        )
        webhook_requests_handler.register(app_aiogram, path=self.webhook_path)
        setup_application(app_aiogram, self.dp, bot=self.bot)

        port = int(os.environ.get('PORT', 8080))
        host = os.environ.get('HOST', '0.0.0.0')

        app_aiogram.router.add_get('/', self.handle_index)
        app_aiogram.router.add_get('/health',self.health_check)

        runner = web.AppRunner(app_aiogram)
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        logging.info(f"Запуск веб-сервера на {host}:{port}")
        await site.start()

        try:
            await asyncio.Event().wait()
        except (KeyboardInterrupt, SystemExit):
            logging.warning("Получен сигнал остановки...")
        finally:
            logging.info("Начало процедуры остановки...")
            hourly_task.cancel()
            try:
                await hourly_task
            except asyncio.CancelledError:
                logging.info("Фоновая задача успешно отменена.")
            await runner.cleanup()
            logging.info("Веб-сервер остановлен.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        bot = TelegramBot()
        asyncio.run(bot.main())
    except Exception as e:
        logging.critical(f"Критическая ошибка при запуске/работе main loop: {e}", exc_info=True)
