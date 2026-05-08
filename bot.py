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
        """Создаёт таблицы users, bad_words и settings, если они не существуют"""
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
                    text_id JSONB,
                    restrict_reason TEXT
                )
            ''')
            # Добавляем колонку restrict_reason если её нет (для существующих БД)
            await conn.execute('''
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'users' AND column_name = 'restrict_reason'
                    ) THEN
                        ALTER TABLE users ADD COLUMN restrict_reason TEXT;
                    END IF;
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'users' AND column_name = 'clean_messages'
                    ) THEN
                        ALTER TABLE users ADD COLUMN clean_messages INTEGER DEFAULT 0;
                    END IF;
                END $$;
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS bad_words (
                    word TEXT PRIMARY KEY
                )
            ''')
            # Таблица для настроек бота
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            # Таблица для флуд-контроля в подгруппах
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS topic_limits (
                    user_id BIGINT,
                    topic_type TEXT,
                    last_msg_time TIMESTAMPTZ,
                    PRIMARY KEY (user_id, topic_type)
                )
            ''')
            # Добавляем колонки username и user_handle в таблицу topic_limits (для миграции существующих БД)
            await conn.execute('''
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'topic_limits' AND column_name = 'username'
                    ) THEN
                        ALTER TABLE topic_limits ADD COLUMN username TEXT;
                        ALTER TABLE topic_limits ADD COLUMN user_handle TEXT;
                    END IF;
                END $$;
            ''')
        finally:
            await conn.close()

    async def get_setting(self, key: str, default: str = None) -> str:
        """Получает значение настройки по ключу"""
        conn = await self._connect()
        try:
            row = await conn.fetchrow('SELECT value FROM settings WHERE key = $1', key)
            return row['value'] if row else default
        finally:
            await conn.close()

    async def set_setting(self, key: str, value: str):
        """Устанавливает значение настройки"""
        conn = await self._connect()
        try:
            await conn.execute('''
                INSERT INTO settings (key, value) VALUES ($1, $2)
                ON CONFLICT (key) DO UPDATE SET value = $2
            ''', key, value)
        finally:
            await conn.close()

    async def get_topic_map(self, thread_id: int) -> str:
        return await self.get_setting(f'topic_map_{thread_id}')

    async def set_topic_map(self, thread_id: int, topic_type: str):
        await self.set_setting(f'topic_map_{thread_id}', topic_type)

    async def check_topic_limit(self, user_id: int, topic_type: str, limit_hours: int, username: str = None, user_handle: str = None) -> bool:
        """
        Проверяет, прошло ли limit_hours с момента последнего сообщения пользователя в данном топике.
        Если прошло (или сообщений не было) - обновляет время и имя и возвращает True.
        Иначе возвращает False.
        """
        conn = await self._connect()
        try:
            now = datetime.now(timezone.utc)
            row = await conn.fetchrow('''
                SELECT last_msg_time FROM topic_limits 
                WHERE user_id = $1 AND topic_type = $2
            ''', user_id, topic_type)
            
            if row:
                last_msg_time = row['last_msg_time']
                if last_msg_time.tzinfo is None:
                    last_msg_time = last_msg_time.replace(tzinfo=timezone.utc)
                time_passed = now - last_msg_time
                if time_passed.total_seconds() < limit_hours * 3600:
                    return False
                    
            await conn.execute('''
                INSERT INTO topic_limits (user_id, topic_type, last_msg_time, username, user_handle) 
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (user_id, topic_type) 
                DO UPDATE SET last_msg_time = $3, username = $4, user_handle = $5
            ''', user_id, topic_type, now, username, user_handle)
            return True
        finally:
            await conn.close()

    async def reset_topic_limit(self, user_id: int, topic_type: str):
        """Сбрасывает лимит по времени для конкретного пользователя в топике."""
        conn = await self._connect()
        try:
            await conn.execute('''
                DELETE FROM topic_limits 
                WHERE user_id = $1 AND topic_type = $2
            ''', user_id, topic_type)
        finally:
            await conn.close()

    async def get_all_topic_limits(self) -> list:
        """Возвращает все текущие лимиты для отрисовки в дашборде."""
        conn = await self._connect()
        try:
            rows = await conn.fetch('''
                SELECT user_id, topic_type, last_msg_time, username, user_handle
                FROM topic_limits 
                ORDER BY last_msg_time DESC
            ''')
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    async def save_users_to_db(self, users: Dict[int, Dict]):
        """
        Сохраняет пользователей в БД
        :param users: словарь {user_id: user_data}
        """
        conn = await self._connect()
        try:
            if not users:
                await conn.execute("TRUNCATE TABLE users")
                return

            records = []
            user_ids_in_dict = []
            for user_id, data in users.items():
                user_ids_in_dict.append(int(user_id))
                records.append((
                    int(user_id),
                    data.get('username', ''),
                    data.get('user_handle', ''),
                    data.get('join_time', datetime.now(timezone.utc)),
                    data.get('notified', False),
                    data.get('status', 'unknown'),
                    json.dumps(data.get('text', [])),  # Сериализуем список в JSON
                    json.dumps(data.get('text_id', [])),  # Сериализуем список в JSON
                    data.get('restrict_reason', None),  # Причина ограничения
                    data.get('clean_messages', 0)
                ))

            async with conn.transaction():
                await conn.execute('DELETE FROM users WHERE NOT (user_id = ANY($1::bigint[]))', user_ids_in_dict)
                await conn.executemany('''
                    INSERT INTO users (
                        user_id, username, user_handle, join_time,
                        notified, status, text, text_id, restrict_reason, clean_messages
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10)
                    ON CONFLICT (user_id) DO UPDATE SET
                        username = EXCLUDED.username,
                        user_handle = EXCLUDED.user_handle,
                        join_time = EXCLUDED.join_time,
                        notified = EXCLUDED.notified,
                        status = EXCLUDED.status,
                        text = EXCLUDED.text,
                        text_id = EXCLUDED.text_id,
                        restrict_reason = EXCLUDED.restrict_reason,
                        clean_messages = EXCLUDED.clean_messages
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
                    'text_id': json.loads(row['text_id']) if row['text_id'] else [],  # Десериализуем JSON в список
                    'restrict_reason': row.get('restrict_reason'),  # Причина ограничения
                    'clean_messages': row.get('clean_messages', 0)
                }
            return users
        finally:
            await conn.close()

    async def save_bad_words(self, bad_words: list):
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

    async def load_bad_words(self) -> list:
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


# Сопоставление похожих кириллических символов с латиницей
CYR_TO_LAT = {
    'А': 'A', 'В': 'B', 'Е': 'E', 'К': 'K', 'М': 'M', 'Н': 'H',
    'О': 'O', 'Р': 'P', 'С': 'C', 'Т': 'T', 'У': 'Y', 'Х': 'X',
    'а': 'a', 'е': 'e', 'о': 'o', 'р': 'p', 'с': 'c', 'т': 't',
    'у': 'y', 'х': 'x', 'д': 'd', 'Д': 'D', 'в': 'b', 'н': 'h'
}
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
        # Match letters that are NOT in Basic Latin/Latin Ext/Cyrillic to avoid counting emojis and punctuation
        return len(re.findall(r'[^\W\d_\u0000-\u024F\u0400-\u052F]', text))
    @staticmethod
    def normalize_text_cyr(text):
        # Заменить кириллицу на похожие латинские символы
        normalized = ''.join(CYR_TO_LAT.get(char, char) for char in text)
        return normalized
    @staticmethod
    def contains_usdt_or_usdc(text):
        # Нормализуем текст: заменим кириллицу -> латиницу
        normalized = TextProcessor.normalize_text_cyr(text)

        # Удалим все разделители между буквами
        compacted = re.sub(r'[\s.\-_]', '', normalized)

        # Приводим к верхнему регистру
        compacted = compacted.upper()

        # Ищем целевые строки
        return 'USDT' in compacted or 'USDC' in compacted

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
    def __init__(self, save_callback=None):
        self.new_users: Dict[int, Dict[str, Any]] = {}  # Добавим type hint для ясности
        self.new_users_lock = asyncio.Lock()
        self.save_callback = save_callback
        self.bad_nicknames = []

    def _trigger_save(self):
        if self.save_callback:
            asyncio.create_task(self.save_callback())

    async def add_user(self, user_id, user_data):
        async with self.new_users_lock:
            self.new_users[user_id] = user_data
        self._trigger_save()

    async def remove_user(self, user_id):
        async with self.new_users_lock:
            res = self.new_users.pop(user_id, None)
        if res is not None:
            self._trigger_save()
        return res

    async def update_user_text(self, user_id, text, text_id):
        async with self.new_users_lock:
            if user_id in self.new_users:
                self.new_users[user_id]['text'].append(text)
                self.new_users[user_id]['text_id'].append(text_id)

    async def increment_clean_messages(self, user_id) -> int:
        """Увеличивает счётчик чистых сообщений. Возвращает новое значение."""
        async with self.new_users_lock:
            if user_id in self.new_users:
                self.new_users[user_id]['clean_messages'] = self.new_users[user_id].get('clean_messages', 0) + 1
                new_count = self.new_users[user_id]['clean_messages']
            else:
                new_count = 0
                
        if new_count > 0:
            self._trigger_save()
            
        return new_count

    async def add_bad_nickname(self, nickname):
        if nickname not in self.bad_nicknames:
            self.bad_nicknames.append(nickname)
            return True
        return False

    async def get_bad_nicknames(self):
        return self.bad_nicknames

    async def remove_bad_nickname(self, nickname):
        if nickname in self.bad_nicknames:
            self.bad_nicknames.remove(nickname)
            return True
        return False

    # Карантин для новых пользователей: 7 дней (в секундах)
    QUARANTINE_SECONDS = 7 * 24 * 60 * 60  # 604800 секунд = 7 дней

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
                    if (now - join_time_utc).total_seconds() > self.QUARANTINE_SECONDS:  # 7 дней
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

    async def update_user_status(self, user_id, new_status, restrict_reason=None):
        """Обновляет статус пользователя по его ID и опционально причину ограничения"""
        updated = False
        async with self.new_users_lock:
            if user_id in self.new_users:
                self.new_users[user_id]['status'] = new_status
                if restrict_reason:
                    self.new_users[user_id]['restrict_reason'] = restrict_reason
                updated = True
                
        if updated:
            self._trigger_save()
            
        return updated

        # !!! НОВЫЙ МЕТОД !!!

    async def get_all_users_data(self) -> dict:
        """Возвращает копию словаря отслеживаемых пользователей."""
        async with self.new_users_lock:
            return self.new_users.copy()  # Возвращаем копию для безопасности
class TelegramBot:
    # Дефолтное сообщение при выходе из карантина
    DEFAULT_QUARANTINE_EXIT_MSG = "Вы вышли с карантина, вам даны полные права"

    def __init__(self):
        self.bot = Bot(token=os.environ.get("TELEGRAM_BOT_TOKEN"))
        self.dp = Dispatcher()
        self.router = Router()
        self.dp.include_router(self.router)

        self.master_id = int(os.environ.get("MASTER_ID"))
        master_thread = os.environ.get("MASTER_THREAD_ID")
        self.master_thread_id = int(master_thread) if master_thread else None
        self.target_chat_id = int(os.environ.get("TARGET_CHAT_ID"))
        self.webhook_url = os.environ.get("WEBHOOK_URL")
        self.webhook_path = f"/{os.environ.get('TELEGRAM_BOT_TOKEN')}"

        # Кастомное сообщение при выходе из карантина (загружается из БД)
        self.quarantine_exit_msg = self.DEFAULT_QUARANTINE_EXIT_MSG

        self.spam_detector = SpamDetector()
        dsn=os.environ.get("DSN")
        self.db_manager = DatabaseManager(dsn)
        # bad_words=await self.db_manager.load_bad_words()
        #
        self.user_manager = UserManager(save_callback=self.save_user_state)
        self._register_handlers()

    async def notify_master(self, text: str, parse_mode: str = None, bot_instance=None, reply_markup=None):
        """Отправляет сообщение администратору, учитывая возможный топик (подгруппу)."""
        bot_to_use = bot_instance or self.bot
        kwargs = {"chat_id": self.master_id, "text": text}
        if parse_mode:
            kwargs["parse_mode"] = parse_mode
        if reply_markup:
            kwargs["reply_markup"] = reply_markup
        if getattr(self, "master_thread_id", None):
            kwargs["message_thread_id"] = self.master_thread_id
            
        try:
            await bot_to_use.send_message(**kwargs)
        except Exception as e:
            import logging
            logging.error(f"Ошибка отправки сообщения мастеру: {e}")

    async def delete_message_delayed(self, chat_id: int, message_id: int, delay_seconds: int):
        """Удаляет сообщение через заданное количество секунд."""
        await asyncio.sleep(delay_seconds)
        try:
            await self.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception as e:
            logging.error(f"Ошибка при удалении временного сообщения: {e}")

    def _register_handlers(self):
        self.router.message(Command(commands=['help']))(self.show_help)
        self.router.message(Command(commands=['suka']))(self.add_nickname)
        self.router.message(Command(commands=['sukan']))(self.remove_nickname)
        self.router.message(Command(commands=['list']))(self.show_nicknames)
        self.router.message(Command(commands=['strange']))(self.show_strange)
        self.router.message(Command(commands=['restrict']))(self.restrict)
        self.router.message(Command(commands=['addc']))(self.add_to_clean)
        self.router.message(Command(commands=['change']))(self.change_exit_message)
        self.router.message(Command(commands=['set_topic']))(self.set_topic)
        self.router.message(Command(commands=['reset_limit']))(self.reset_limit)
        self.router.message(Command(commands=['nolim']))(self.nolim_user)
        self.router.message(Command(commands=['send']))(self.send_arbitrary_message)
        self.router.chat_member()(self.handle_chat_member)
        self.router.message(F.text)(self.handle_message)
        self.router.message(F.photo | F.video | F.audio | F.document |
                            F.sticker | F.location | F.contact |
                            F.animation | F.voice | F.video_note)(self.handle_media)
        self.router.callback_query(F.data.startswith('restore:'))(self.handle_restore_callback)

        self.dp.startup.register(self.on_startup)
        self.dp.shutdown.register(self.on_shutdown)

    async def show_help(self, message: types.Message):
        """Показывает список доступных команд"""
        if message.from_user.id != self.master_id:
            return

        help_text = f"""
🤖 *Команды бота Anti-Spam:*

📋 *Управление карантином:*
• `/strange` — показать новых пользователей на карантине
• `/addc <user_id>` — убрать пользователя из карантина и дать полные права
• `/restrict <id1,id2,...>` — ограничить права пользователям
• `/change <текст>` — изменить сообщение при выходе из карантина

🚫 *Чёрные списки:*
• `/suka <никнейм>` — добавить никнейм в чёрный список
• `/sukan <никнейм>` — удалить никнейм из чёрного списка
• `/list` — показать список запрещённых никнеймов

⚙️ *Настройка системы:*
• `/set_topic <тип>` — привязать текущий топик к антиспам-правилам
• `/nolim <ID>` — снять флуд-контроль в ads и parcels
• `/send <ID_чата> <текст>` — отправить сообщение в указанный чат

ℹ️ `/help` — показать это сообщение

⚙️ *Автоматические проверки:*
• Карантин 7 дней для новых пользователей
• Бан при плохом никнейме/имени
• Удаление медиа от новых пользователей
• Блокировка при 3 одинаковых сообщениях
• Проверка на спам (ML модель)
• Автопромоут после 2 чистых сообщений

💬 *Текущее сообщение при выходе из карантина:*
`{self.quarantine_exit_msg}`
"""
        await message.reply(help_text, parse_mode="Markdown")

    async def add_to_clean(self, message: types.Message):
        """Принудительно убирает пользователя из карантина и даёт полные права"""
        if message.from_user.id != self.master_id:
            return

        command_parts = message.text.split(maxsplit=1)
        if len(command_parts) < 2 or not command_parts[1]:
            await message.reply("❌ Укажите ID пользователя: `/addc 123456789`", parse_mode="Markdown")
            return

        try:
            user_id = int(command_parts[1].strip())
        except ValueError:
            await message.reply("❌ ID должен быть числом")
            return

        # Проверяем, есть ли пользователь в карантине
        user_data = None
        async with self.user_manager.new_users_lock:
            user_data = self.user_manager.new_users.get(user_id)

        if not user_data:
            await message.reply(f"⚠️ Пользователь `{user_id}` не найден в списке карантина.", parse_mode="Markdown")
            return

        try:
            # Восстанавливаем полные права
            full_permissions = types.ChatPermissions(
                can_send_messages=True,
                can_send_audios=True,
                can_send_documents=True,
                can_send_photos=True,
                can_send_videos=True,
                can_send_video_notes=True,
                can_send_voice_notes=True,
                can_send_polls=True,
                can_send_other_messages=True,
                can_add_web_page_previews=True,
                can_invite_users=True
            )
            await self.bot.restrict_chat_member(self.target_chat_id, user_id, permissions=full_permissions)
            
            # Удаляем из отслеживания
            await self.user_manager.remove_user(user_id)
            
            logging.info(f"Админ {message.from_user.id} вручную удалил пользователя {user_id} из карантина.")
            
            await message.reply(
                f"✅ Пользователь удалён из карантина:\n"
                f"ID: `{user_id}`\n"
                f"Имя: {user_data.get('username', 'N/A')}\n"
                f"Полные права восстановлены.",
                parse_mode="Markdown"
            )
        except Exception as e:
            logging.error(f"Ошибка при удалении пользователя {user_id} из карантина: {e}", exc_info=True)
            await message.reply(f"❌ Ошибка: {e}")

    async def handle_restore_callback(self, callback: types.CallbackQuery):
        """Обрабатывает нажатие на кнопку 'Восстановить' от администратора."""
        if callback.from_user.id != self.master_id:
            await callback.answer("У вас нет прав!", show_alert=True)
            return

        try:
            _, user_id_str = callback.data.split(':')
            user_id = int(user_id_str)
        except Exception as e:
            logging.error(f"Ошибка парсинга callback data '{callback.data}': {e}")
            await callback.answer("Ошибка в данных кнопки", show_alert=True)
            return

        try:
            # Восстанавливаем полные права
            full_permissions = types.ChatPermissions(
                can_send_messages=True,
                can_send_audios=True,
                can_send_documents=True,
                can_send_photos=True,
                can_send_videos=True,
                can_send_video_notes=True,
                can_send_voice_notes=True,
                can_send_polls=True,
                can_send_other_messages=True,
                can_add_web_page_previews=True,
                can_invite_users=True
            )
            await self.bot.restrict_chat_member(self.target_chat_id, user_id, permissions=full_permissions)
            
            # Удаляем из отслеживания
            await self.user_manager.remove_user(user_id)
            
            # Изменяем сообщение у администратора, убирая кнопку
            original_text = callback.message.text or callback.message.caption or "Сообщение"
            new_text = original_text + "\n\n✅ *РЕШЕНО: Пользователь восстановлен.*"
            try:
                await callback.message.edit_text(new_text, parse_mode="Markdown")
            except Exception as e:
                # Если парс мод не сработал из-за старого текста
                await callback.message.edit_text(original_text + "\n\n✅ РЕШЕНО: Пользователь восстановлен.")
                
            await callback.answer("Пользователь успешно восстановлен!")
            logging.info(f"Админ {callback.from_user.id} восстановил пользователя {user_id} через inline кнопку.")
            
        except Exception as e:
            logging.error(f"Ошибка при инлайн восстановлении пользователя {user_id}: {e}", exc_info=True)
            await callback.answer(f"❌ Ошибка восстановления: {e}", show_alert=True)

    async def change_exit_message(self, message: types.Message):
        """Изменяет сообщение при выходе из карантина"""
        if message.from_user.id != self.master_id:
            return

        command_parts = message.text.split(maxsplit=1)
        if len(command_parts) < 2 or not command_parts[1].strip():
            await message.reply(
                f"📝 *Текущее сообщение при выходе из карантина:*\n"
                f"`{self.quarantine_exit_msg}`\n\n"
                f"Чтобы изменить, отправьте: `/change Ваш новый текст`",
                parse_mode="Markdown"
            )
            return

        new_message = command_parts[1].strip()
        self.quarantine_exit_msg = new_message
        
        try:
            await self.db_manager.set_setting('quarantine_exit_msg', new_message)
            await message.reply(
                f"✅ Сообщение при выходе из карантина изменено на:\n"
                f"`{new_message}`",
                parse_mode="Markdown"
            )
            logging.info(f"Мастер изменил сообщение выхода из карантина на: {new_message}")
        except Exception as e:
            logging.error(f"Ошибка сохранения настройки quarantine_exit_msg: {e}", exc_info=True)
            await message.reply(f"⚠️ Сообщение изменено, но не сохранено в БД: {e}")

    async def set_topic(self, message: types.Message):
        """Привязывает текущий топик к определенному типу для антиспам правил"""
        if message.from_user.id != self.master_id:
            return
            
        command_parts = message.text.split(maxsplit=1)
        if len(command_parts) < 2 or not command_parts[1].strip():
            await message.reply(
                "Использование: `/set_topic <тип>`\n"
                "Типы: `news`, `visa`, `sos`, `parcels`, `ads`, `feedback`, `life`, "
                "`spanish`, `tourism`, `family`, `dating`, `chat`",
                parse_mode="Markdown"
            )
            return
            
        topic_type = command_parts[1].strip()
        thread_id = message.message_thread_id
        if not thread_id:
            await message.reply("Эту команду нужно вызывать внутри топика (подгруппы).")
            return
            
        try:
            await self.db_manager.set_topic_map(thread_id, topic_type)
            await message.reply(f"✅ Топик ID {thread_id} успешно привязан к типу `{topic_type}`!", parse_mode="Markdown")
            logging.info(f"Мастер привязал топик {thread_id} к типу {topic_type}")
        except Exception as e:
            logging.error(f"Ошибка привязки топика: {e}", exc_info=True)
            await message.reply(f"⚠️ Ошибка привязки: {e}")

    async def reset_limit(self, message: types.Message):
        """Сбрасывает таймер флуд-контроля для указанного пользователя и топика."""
        if message.from_user.id != self.master_id:
            return
            
        command_parts = message.text.split()
        if len(command_parts) != 3:
            await message.reply(
                "Использование: `/reset_limit <ID_пользователя> <тип_топика>`\n"
                "Пример: `/reset_limit 123456789 ads`",
                parse_mode="Markdown"
            )
            return
            
        try:
            user_id = int(command_parts[1].strip())
            topic_type = command_parts[2].strip()
        except ValueError:
            await message.reply("❌ ID пользователя должен быть числом.")
            return

        try:
            await self.db_manager.reset_topic_limit(user_id, topic_type)
            await message.reply(f"✅ Лимит публикации для пользователя `{user_id}` в топике `{topic_type}` сброшен!", parse_mode="Markdown")
            logging.info(f"Мастер сбросил лимит пользователя {user_id} для топика {topic_type}")
        except Exception as e:
            logging.error(f"Ошибка при сбросе лимита: {e}", exc_info=True)
            await message.reply(f"⚠️ Ошибка сброса лимита: {e}")

    async def nolim_user(self, message: types.Message):
        """Снимает флуд контроль в ads и parcels для пользователя."""
        if message.from_user.id != self.master_id:
            return
            
        command_parts = message.text.split()
        if len(command_parts) != 2:
            await message.reply(
                "Использование: `/nolim <ID_пользователя>`\n"
                "Пример: `/nolim 123456789`",
                parse_mode="Markdown"
            )
            return
            
        try:
            user_id = int(command_parts[1].strip())
        except ValueError:
            await message.reply("❌ ID пользователя должен быть числом.")
            return

        try:
            await self.db_manager.reset_topic_limit(user_id, 'ads')
            await self.db_manager.reset_topic_limit(user_id, 'parcels')
            await message.reply(f"✅ Флуд-контроль для пользователя `{user_id}` в темах `ads` и `parcels` успешно снят!", parse_mode="Markdown")
            logging.info(f"Мастер снял флуд контроль пользователя {user_id} для ads и parcels")
        except Exception as e:
            logging.error(f"Ошибка при сбросе лимитов: {e}", exc_info=True)
            await message.reply(f"⚠️ Ошибка сброса лимита: {e}")

    async def send_arbitrary_message(self, message: types.Message):
        """Отправляет произвольное сообщение в указанный чат"""
        if message.from_user.id != self.master_id:
            return

        command_parts = message.text.split(maxsplit=2)
        if len(command_parts) < 3:
            await message.reply(
                "Использование: `/send <ID_чата> <текст_сообщения>`\n"
                "Пример: `/send -1001234567890 Привет, это тестовое сообщение`",
                parse_mode="Markdown"
            )
            return

        try:
            chat_id = int(command_parts[1].strip())
        except ValueError:
            await message.reply("❌ ID чата должен быть числом.")
            return

        text_to_send = command_parts[2]

        try:
            await self.bot.send_message(chat_id=chat_id, text=text_to_send)
            await message.reply(f"✅ Сообщение успешно отправлено в чат `{chat_id}`.", parse_mode="Markdown")
            logging.info(f"Админ отправил сообщение в чат {chat_id}: {text_to_send[:50]}...")
        except Exception as e:
            logging.error(f"Ошибка при отправке сообщения в чат {chat_id}: {e}", exc_info=True)
            await message.reply(f"❌ Ошибка при отправке сообщения: {e}")

    async def check_topic_rules(self, message: types.Message) -> bool:
        """Проверяет правила подгрупп для сообщения. Возвращает True, если сообщение было удалено."""
        if message.chat.id != self.target_chat_id:
            return False

        user_id = message.from_user.id
        if user_id == self.master_id:
            return False
            
        thread_id = message.message_thread_id
        if not thread_id:
            return False
            
        topic_type = await self.db_manager.get_topic_map(thread_id)
        if not topic_type:
            return False

        msg_tags = ""
        if message.text:
            msg_tags = message.text.lower()
        elif message.caption:
            msg_tags = message.caption.lower()

        try:
            # visa, sos, feedback: no video, video_note, voice
            if topic_type in ['visa', 'sos', 'feedback']:
                if message.video or message.video_note or message.voice:
                    await message.delete()
                    return True

            # parcels: no video_note, voice
            if topic_type == 'parcels':
                if message.video_note or message.voice:
                    await message.delete()
                    return True
                
                # Tag validation 
                tags = ['#ищу', '#передам', '#еду', '#куда', '#из', '#возьму']
                if not any(tag in msg_tags for tag in tags):
                    if message.media_group_id and not msg_tags:
                        pass # Skip empty caption elements in media group
                    else:
                        warn_msg = await message.reply(
                             f"⚠️ Привет, {message.from_user.first_name}! В топике «Посылки» обязательно использование хотя бы одного из тегов: "
                             f"`" + "`, `".join(tags) + "`.\n\n"
                             f"Ваше сообщение было удалено. Пожалуйста, добавьте тег и отправьте снова.",
                             parse_mode="Markdown"
                        )
                        await message.delete()
                        asyncio.create_task(self.delete_message_delayed(warn_msg.chat.id, warn_msg.message_id, 30))
                        return True
                
                # Flood control 72h
                if not await self.db_manager.check_topic_limit(user_id, topic_type, 72, message.from_user.first_name, message.from_user.username):
                    warn_msg = await message.reply(
                         f"⏳ Привет, {message.from_user.first_name}! В топике «Посылки» можно писать не чаще 1 раза в 72 часа.\n\n"
                         f"Ваше сообщение было удалено. Пожалуйста, подождите перед следующей публикацией.",
                         parse_mode="Markdown"
                    )
                    await message.delete()
                    asyncio.create_task(self.delete_message_delayed(warn_msg.chat.id, warn_msg.message_id, 30))
                    return True

            if topic_type == 'ads':
                tags = ['#услуги', '#работа', '#квартира']
                if not any(tag in msg_tags for tag in tags):
                    if message.media_group_id and not msg_tags:
                        pass # Skip empty caption elements in media group
                    else:
                        warn_msg = await message.reply(
                             f"⚠️ Привет, {message.from_user.first_name}! В топике «Объявления» обязательно использование хотя бы одного из тегов: "
                             f"`" + "`, `".join(tags) + "`.\n\n"
                             f"Ваше сообщение было удалено. Пожалуйста, добавьте тег и отправьте снова.",
                             parse_mode="Markdown"
                        )
                        await message.delete()
                        asyncio.create_task(self.delete_message_delayed(warn_msg.chat.id, warn_msg.message_id, 30))
                        return True
                
                # Flood control 24h
                if not await self.db_manager.check_topic_limit(user_id, topic_type, 24, message.from_user.first_name, message.from_user.username):
                    warn_msg = await message.reply(
                         f"⏳ Привет, {message.from_user.first_name}! В топике «Объявления» можно писать не чаще 1 раза в 24 часа.\n\n"
                         f"Ваше сообщение было удалено. Пожалуйста, подождите перед следующей публикацией.",
                         parse_mode="Markdown"
                    )
                    await message.delete()
                    asyncio.create_task(self.delete_message_delayed(warn_msg.chat.id, warn_msg.message_id, 30))
                    return True

        except Exception as e:
            logging.error(f"Ошибка при удалении сообщения в топике {topic_type}: {e}")
            
        return False

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

    async def remove_nickname(self, message: types.Message):
        if message.from_user.id != self.master_id:
            return

        command_parts = message.text.split(maxsplit=1)
        if len(command_parts) < 2 or not command_parts[1]:
            await message.reply("Пожалуйста, укажите никнейм после команды /sukan")
            return

        nickname = command_parts[1]
        if await self.user_manager.remove_bad_nickname(nickname):
            await message.reply(f"Никнейм '{nickname}' удален из списка плохих.")
            logging.info(f"Мастер {message.from_user.id} удалил никнейм: {nickname}")
        else:
            await message.reply(f"Никнейм '{nickname}' не найден в списке.")

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
                        can_send_audios=False,
                        can_send_documents=False,
                        can_send_photos=False,
                        can_send_videos=False,
                        can_send_video_notes=False,
                        can_send_voice_notes=False,
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


        async with self.user_manager.new_users_lock:
            users_data = self.user_manager.new_users.copy()
        users_with_text = self.format_users_message(users_data)

        # print(users_with_text)
        await self.notify_master(
            text=users_with_text,
            parse_mode="Markdown"
        )





        # self.router.message(Command(commands=['strange']))(self.show_strange)

    async def handle_chat_member(self, update: types.ChatMemberUpdated):
        new_status = update.new_chat_member.status
        old_status = update.old_chat_member.status
        user = update.new_chat_member.user
        user_id = user.id
        username = user.full_name
        user_handle = f"@{user.username}" if user.username else "Нет username"
        chat_id = update.chat.id
        event_time_dt = update.date

        if chat_id == self.target_chat_id:
            if new_status == "member" and old_status in ["left", "kicked", "creator"]:
                await self.user_manager.add_user(user_id, {
                    'username': username,
                    'user_handle': user_handle,
                    'join_time': event_time_dt,
                    'notified': False,
                    'status': new_status,
                    'text': [],
                    'text_id': [],
                    'clean_messages': 0  # Счётчик чистых сообщений для раннего выхода из карантина
                })
                logging.info(f"Новый участник {username} ({user_id}) добавлен в список отслеживания.")
                try:
                    await self.notify_master(
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
                        await self.notify_master(
                            f"⚠️ Автоматически забанен пользователь с плохим ником/именем:\nИмя: {username}\nХэндл: {user_handle}\nID: {user_id}"
                        )
                        logging.info(f"Пользователь {username} ({user_id}) забанен из-за ника/имени.")
                        await self.user_manager.remove_user(user_id)
                    except Exception as e:
                        logging.error(f"Ошибка бана пользователя {user_id} с плохим ником/именем: {e}", exc_info=True)
                        await self.notify_master(
                            f"❌ Не удалось забанить пользователя {username} ({user_id}) с плохим ником/именем. Ошибка: {e}"
                        )
            elif new_status=="restricted":
                await self.user_manager.update_user_status( user_id, new_status)

    async def handle_message(self, message: types.Message):
        if await self.check_topic_rules(message):
            return

        sender_id = message.from_user.id
        chat_id = message.chat.id
        text = message.text or ""  # Гарантируем строку

        # Получаем данные пользователя (если он отслеживается)
        user_data = None
        async with self.user_manager.new_users_lock:  # Блокируем доступ на время чтения
            if sender_id in self.user_manager.new_users:
                # Получаем копию, чтобы работать с ней вне блока lock
                user_data = self.user_manager.new_users.get(sender_id)  # Используем .get для безопасности

        thread_id = message.message_thread_id
        topic_name = "Обычный чат"
        if thread_id:
            topic_type = await self.db_manager.get_topic_map(thread_id)
            if topic_type:
                topic_name = topic_type

        # Если пользователя нет в словаре новых, обрабатываем как старого
        if not user_data:
            logging.debug(f"Сообщение от неотслеживаемого пользователя {sender_id}")
            # Ваш код для обработки старых пользователей (например, пересылка админу)
            # final_msg_to_master = ...
            # await self.bot.send_message(...)
            final_msg_to_master = (
                f"ID message: {message.message_id}\n"
                f"ID chat: {chat_id}\n"
                f"ID sender: {sender_id}\n"
                f"User Name: {message.from_user.username}\n"
                f"Топик: {topic_name}\n"
                f"Текст:\n```\n{text}\n```\n"
            )
            final_msg_to_master = TextProcessor.fix_markdown(final_msg_to_master)
            await self.notify_master( final_msg_to_master, parse_mode="Markdown")
            return  # Прекращаем обработку здесь

        # Сколько времени прошло после присоеденения юзера
        time_joined = user_data.get("join_time", datetime.now(timezone.utc))
        if time_joined.tzinfo is None:  # Убедимся что есть таймзона
            time_joined = time_joined.replace(tzinfo=timezone.utc)
        time_passed = datetime.now(timezone.utc) - time_joined



        skip_quarantine = topic_name in ['ads', 'parcels']

        entities = message.entities or message.caption_entities  # Берём entities текста или подписи
        has_link_preview = getattr(message, 'link_preview_options', None) and getattr(message.link_preview_options, 'url', None)
        has_forward = getattr(message, 'forward_origin', None) is not None

        filtered_entities = []
        if entities:
            for ent in entities:
                # Разрешаем хэштеги в топиках Объявления (ads) и Посылки (parcels)
                if ent.type == 'hashtag' and topic_name in ['ads', 'parcels']:
                    continue
                filtered_entities.append(ent)

        if not skip_quarantine and (filtered_entities or has_link_preview or has_forward):
            reason_str = "Entities/Скрытые ссылки/Форварды"
            if has_link_preview:
                reason_str = "Скрытая ссылка (Link Preview)"
            elif has_forward:
                reason_str = "Пересланное сообщение (Forward)"
            else:
                reason_str = "Entities (ссылки/форматирование)"

            # Формирование отчета для админа (добавим флаг повтора)
            msg_to_master_lines = [
                f"Сообщение от нового пользователя ({str(time_passed).split('.')[0]} после входа):",
                f"ID chat: {chat_id}",
                f"ID: {sender_id}",
                f"Имя: {user_data.get('username', 'N/A')}",
                f"Хэндл: {user_data.get('user_handle', 'N/A')}",
                f"Топик: {topic_name}",
                f"Текст (ID: {message.message_id}):\n```\n{text}\n```",  # Не экранируем Markdown для логов/принта
                reason_str
            ]
            await message.delete()
            permissions = types.ChatPermissions(can_send_messages=False)
            await self.bot.restrict_chat_member(chat_id, sender_id, permissions=permissions)
            logging.info(f"Пользователь {sender_id} ограничен (только чтение). Причина: {reason_str}")
            # Обновляем статус в менеджере ПОСЛЕ успешного ограничения
            await self.user_manager.update_user_status(sender_id, "restricted", reason_str)

            final_msg_to_master = "\n".join(msg_to_master_lines)
            # Экранируем для Markdown (TextProcessor.fix_markdown - ваша функция)
            formatted_report = TextProcessor.fix_markdown(final_msg_to_master)
            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            kb = InlineKeyboardMarkup(inline_keyboard=[[
               InlineKeyboardButton(text="✅ Восстановить (Не спам)", callback_data=f"restore:{sender_id}")
            ]])
            await self.notify_master(formatted_report, parse_mode="Markdown", reply_markup=kb)

            return

        # Анализ сообщения
        non_latin_cyrillic_count = TextProcessor.count_non_latin_cyrillic(text)

        # --- Обработка нового пользователя ---

        if not skip_quarantine and non_latin_cyrillic_count > 5:  # Используйте константу из config
            msg_to_master_lines = [
                f"Сообщение от нового пользователя ({str(time_passed).split('.')[0]} после входа):",
                f"ID chat: {chat_id}",
                f"ID: {sender_id}",
                f"Имя: {user_data.get('username', 'N/A')}",
                f"Хэндл: {user_data.get('user_handle', 'N/A')}",
                f"Топик: {topic_name}",
                f"Текст (ID: {message.message_id}):\n```\n{text}\n```",  # Не экранируем Markdown для логов/принта
                f"Non-latin/cyrillic symbols detected"
            ]
            await message.delete()
            permissions = types.ChatPermissions(can_send_messages=False)
            await self.bot.restrict_chat_member(chat_id, sender_id, permissions=permissions)
            logging.info(f"Пользователь {sender_id} ограничен (только чтение). Причина: non-latin/cyrillic symbols")
            # Обновляем статус в менеджере ПОСЛЕ успешного ограничения
            await self.user_manager.update_user_status(sender_id, "restricted", "Спецсимволы (не лат/кир)")

            final_msg_to_master = "\n".join(msg_to_master_lines)
            # Экранируем для Markdown (TextProcessor.fix_markdown - ваша функция)
            formatted_report = TextProcessor.fix_markdown(final_msg_to_master)
            
            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            kb = InlineKeyboardMarkup(inline_keyboard=[[
               InlineKeyboardButton(text="✅ Восстановить (Не спам)", callback_data=f"restore:{sender_id}")
            ]])
            await self.notify_master(formatted_report, parse_mode="Markdown", reply_markup=kb)

            return

        usdt_usdc=TextProcessor.contains_usdt_or_usdc(text)
        if not skip_quarantine and usdt_usdc:
            msg_to_master_lines = [
                f"Сообщение от нового пользователя ({str(time_passed).split('.')[0]} после входа):",
                f"ID chat: {chat_id}",
                f"ID: {sender_id}",
                f"Имя: {user_data.get('username', 'N/A')}",
                f"Хэндл: {user_data.get('user_handle', 'N/A')}",
                f"Топик: {topic_name}",
                f"Текст (ID: {message.message_id}):\n```\n{text}\n```",  # Не экранируем Markdown для логов/принта
                f"USDT/ USDC"
            ]
            await message.delete()
            permissions = types.ChatPermissions(can_send_messages=False)
            await self.bot.restrict_chat_member(chat_id, sender_id, permissions=permissions)
            logging.info(f"Пользователь {sender_id} ограничен (только чтение). Причина: usdt usdc")
            # Обновляем статус в менеджере ПОСЛЕ успешного ограничения
            await self.user_manager.update_user_status(sender_id, "restricted", "USDT/USDC")

            final_msg_to_master = "\n".join(msg_to_master_lines)
            # Экранируем для Markdown (TextProcessor.fix_markdown - ваша функция)
            formatted_report = TextProcessor.fix_markdown(final_msg_to_master)
            
            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            kb = InlineKeyboardMarkup(inline_keyboard=[[
               InlineKeyboardButton(text="✅ Восстановить (Не спам)", callback_data=f"restore:{sender_id}")
            ]])
            await self.notify_master(formatted_report, parse_mode="Markdown", reply_markup=kb)

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

        if not skip_quarantine and text_repeated:
            msg_to_master_lines = [
                f"Сообщение от нового пользователя ({str(time_passed).split('.')[0]} после входа):",
                f"ID chat: {chat_id}",
                f"ID: {sender_id}",
                f"Имя: {user_data.get('username', 'N/A')}",
                f"Хэндл: {user_data.get('user_handle', 'N/A')}",
                f"Текст (ID: {message.message_id}):\n```\n{text}\n```",  # Не экранируем Markdown для логов/принта
                f"3 messages the same"
            ]
            # Удаляем текущее сообщение
            await message.delete()
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
            # Ограничиваем пользователя
            permissions = types.ChatPermissions(can_send_messages=False)
            await self.bot.restrict_chat_member(chat_id, sender_id, permissions=permissions)
            logging.info(f"Пользователь {sender_id} ограничен (только чтение). Причина: repeated messages")
            await self.user_manager.update_user_status(sender_id, "restricted", "Повтор сообщений")

            final_msg_to_master = "\n".join(msg_to_master_lines)
            # Экранируем для Markdown (TextProcessor.fix_markdown - ваша функция)
            formatted_report = TextProcessor.fix_markdown(final_msg_to_master)
            
            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            kb = InlineKeyboardMarkup(inline_keyboard=[[
               InlineKeyboardButton(text="✅ Восстановить (Не спам)", callback_data=f"restore:{sender_id}")
            ]])
            await self.notify_master(formatted_report, parse_mode="Markdown", reply_markup=kb)

            return
        spam_prediction = self.spam_detector.make_spam_prediction(text)
        if not skip_quarantine and spam_prediction == 1:
            msg_to_master_lines = [
                f"Сообщение от нового пользователя ({str(time_passed).split('.')[0]} после входа):",
                f"ID chat: {chat_id}",
                f"ID: {sender_id}",
                f"Имя: {user_data.get('username', 'N/A')}",
                f"Хэндл: {user_data.get('user_handle', 'N/A')}",
                f"Текст (ID: {message.message_id}):\n```\n{text}\n```",  # Не экранируем Markdown для логов/принта
                f"SPAM"
            ]

            await message.delete()
            permissions = types.ChatPermissions(can_send_messages=False)
            await self.bot.restrict_chat_member(chat_id, sender_id, permissions=permissions)
            logging.info(f"Пользователь {sender_id} ограничен (только чтение). Причина: SPAM")
            # Обновляем статус в менеджере ПОСЛЕ успешного ограничения
            await self.user_manager.update_user_status(sender_id, "restricted", "ML-спам детектор")

            final_msg_to_master = "\n".join(msg_to_master_lines)
            # Экранируем для Markdown (TextProcessor.fix_markdown - ваша функция)
            formatted_report = TextProcessor.fix_markdown(final_msg_to_master)
            
            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            kb = InlineKeyboardMarkup(inline_keyboard=[[
               InlineKeyboardButton(text="✅ Восстановить (Не спам)", callback_data=f"restore:{sender_id}")
            ]])
            await self.notify_master(formatted_report, parse_mode="Markdown", reply_markup=kb)
            return

        # Сохраняем текст в историю для проверки повторов в будущем
        await self.user_manager.update_user_text(sender_id, text, message.message_id)

        # Увеличиваем счётчик чистых сообщений и проверяем на автопромоут
        clean_count = await self.user_manager.increment_clean_messages(sender_id)
        
        # Если пользователь отправил 2 чистых сообщения — даём полные права
        if clean_count >= 2:
            try:
                # Восстанавливаем полные права
                full_permissions = types.ChatPermissions(
                    can_send_messages=True,
                    can_send_audios=True,
                    can_send_documents=True,
                    can_send_photos=True,
                    can_send_videos=True,
                    can_send_video_notes=True,
                    can_send_voice_notes=True,
                    can_send_polls=True,
                    can_send_other_messages=True,
                    can_add_web_page_previews=True,
                    can_invite_users=True
                )
                await self.bot.restrict_chat_member(chat_id, sender_id, permissions=full_permissions)
                
                # Удаляем из отслеживания
                await self.user_manager.remove_user(sender_id)
                
                logging.info(f"Пользователь {sender_id} ({user_data.get('username')}) прошёл проверку (2 чистых сообщения). Полные права восстановлены.")
                
                # Отправляем пользователю сообщение о выходе из карантина (реплаем на его сообщение)
                try:
                    await message.reply(self.quarantine_exit_msg)
                except Exception as reply_err:
                    logging.error(f"Не удалось отправить сообщение о выходе из карантина пользователю {sender_id}: {reply_err}")
                
                await self.notify_master(
                    f"✅ Пользователь прошёл проверку:\n"
                    f"ID: {sender_id}\n"
                    f"Имя: {user_data.get('username', 'N/A')}\n"
                    f"Хэндл: {user_data.get('user_handle', 'N/A')}\n"
                    f"Полные права восстановлены после 2 чистых сообщений."
                )
            except Exception as e:
                logging.error(f"Ошибка при восстановлении прав пользователю {sender_id}: {e}", exc_info=True)


    async def handle_media(self, message: types.Message):
        if await self.check_topic_rules(message):
            return

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

        # Исключение для топиков Объявления и Посылки
        # Проверка правил этих топиков (тегов и тд) уже прошла успешно в check_topic_rules
        thread_id = message.message_thread_id
        if thread_id:
            topic_type = await self.db_manager.get_topic_map(thread_id)
            if topic_type in ['ads', 'parcels']:
                return

        chat_id = message.chat.id
        logging.info(
            f"Новый пользователь {user_id} ({user_data['username']}) отправил медиа ({message.content_type}). Удаление.")
        try:
            # Сначала записываем медиа в историю
            await self.user_manager.update_user_text(user_id, "medioFuck", message.message_id)
            await message.delete()
            
            # Получаем обновлённые данные для проверки повторов
            async with self.user_manager.new_users_lock:
                updated_data = self.user_manager.new_users.get(user_id, {})
                messages_history = updated_data.get('text', [])
                message_ids_history = updated_data.get('text_id', [])
            
            # Проверяем, есть ли 3 медиа-сообщения подряд
            min_repetition_count = 3
            if len(messages_history) >= min_repetition_count:
                recent_messages = messages_history[-min_repetition_count:]
                # Если все последние 3 сообщения — медиа ("medioFuck")
                if all(msg == "medioFuck" for msg in recent_messages):
                    logging.warning(f"Обнаружено {min_repetition_count} медиа подряд от {user_id}. Применяем рестрикт.")
                    
                    # Удаляем предыдущие сообщения (медиа)
                    for msg_id in message_ids_history:
                        try:
                            await self.bot.delete_message(chat_id, msg_id)
                        except TelegramBadRequest as e:
                            if "message to delete not found" not in str(e):
                                logging.error(f"Ошибка удаления сообщения {msg_id}: {e}")
                    
                    # Ограничиваем пользователя
                    permissions = types.ChatPermissions(can_send_messages=False)
                    await self.bot.restrict_chat_member(chat_id, user_id, permissions=permissions)
                    logging.info(f"Пользователь {user_id} ограничен (только чтение). Причина: 3 медиа подряд")
                    await self.user_manager.update_user_status(user_id, "restricted", "3 медиа подряд")
                    
                    await self.notify_master(
                        f"🚫 Пользователь {user_data['username']} ({user_id}) заблокирован.\n"
                        f"Причина: отправил 3 медиа подряд на карантине."
                    )
                    return
            
            # Если не было 3 медиа подряд — просто уведомляем об удалении
            await self.notify_master(
                f"🗑 Медиа ({message.content_type}) от нового пользователя {user_data['username']} ({user_id}) было удалено."
            )
        except Exception as e:
            logging.error(f"Ошибка удаления медиа от {user_id}: {e}", exc_info=True)
            try:
                await self.notify_master(
                    f"❌ Не удалось удалить медиа ({message.content_type}) от {user_id}. Ошибка: {e}"
                )
            except Exception as e_inner:
                logging.error(f"Не удалось отправить сообщение об ошибке мастеру: {e_inner}", exc_info=True)

    async def save_user_state(self):
        """Немедленно сохраняет текущее состояние пользователей в БД"""
        try:
            current_users_state = await self.user_manager.get_all_users_data()
            await self.db_manager.save_users_to_db(current_users_state)
        except Exception as e:
            logging.error(f"Ошибка немедленного сохранения состояния: {e}", exc_info=True)

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
            
            # Загружаем кастомное сообщение выхода из карантина
            custom_exit_msg = await self.db_manager.get_setting('quarantine_exit_msg')
            if custom_exit_msg:
                self.quarantine_exit_msg = custom_exit_msg
                logging.info(f"Загружено кастомное сообщение выхода из карантина: {custom_exit_msg}")
            
            print(log_message)
        except Exception as e:
            log_message = f"КРИТИЧЕСКАЯ ОШИБКА загрузки БД при старте: {e}"
            logging.error(log_message, exc_info=True)
            print(log_message)
            # Решите, нужно ли останавливать бота, если БД не загрузилась
            # await self.notify_master( log_message) # Уведомить админа
            # return # Возможно, стоит прервать запуск

        # await asyncio.sleep(1)  # Небольшая пауза
        if self.webhook_url:
            try:
                await bot.set_webhook(f"{self.webhook_url}{self.webhook_path}", drop_pending_updates=True)
                log_message = f"Вебхук установлен на: {self.webhook_url}{self.webhook_path}"
                logging.info(log_message)
                print(log_message)
                await self.notify_master("Бот запущен/перезапущен и вебхук установлен.", bot_instance=bot)
            except Exception as e:
                log_message = f"⚠️ Ошибка установки вебхука: {e}"
                logging.error(log_message, exc_info=True)
                print(log_message)
                await self.notify_master(log_message, bot_instance=bot)
        else:
            log_message = "WEBHOOK_URL не задан. Запуск в режиме polling."
            logging.warning(log_message)
            print(log_message)
            await bot.delete_webhook(drop_pending_updates=True)  # Убедимся, что вебхук снят
            await self.notify_master("Бот запущен/перезапущен в режиме polling (БЕЗ вебхука).", bot_instance=bot)

    async def on_shutdown(self, bot: Bot):
        """Действия при остановке бота."""
        log_message = "Бот останавливается..."
        logging.warning(log_message)
        print(log_message)
        # Сначала пытаемся отправить сообщение об остановке
        try:
            await self.notify_master("Бот останавливается... Сохранение данных.", bot_instance=bot)
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
            # await self.notify_master("Данные сохранены. Бот выключен.", bot_instance=bot)
        except Exception as e:
            log_message = f"⚠️ КРИТИЧЕСКАЯ ОШИБКА сохранения БД при выключении: {e}"
            logging.error(log_message, exc_info=True)
            print(log_message)
            # Попытаться еще раз уведомить админа
            try:
                await self.notify_master(log_message, bot_instance=bot)
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
        """Обработчик для корневого URL ('/') — таблица с пользователями"""
        import hashlib
        
        all_users = []
        now = datetime.now(timezone.utc)
        
        async with self.user_manager.new_users_lock:
            for user_id, data in self.user_manager.new_users.items():
                status = data.get('status', 'unknown')
                join_time = data.get('join_time')
                texts = data.get('text', [])
                
                # Время в карантине
                if isinstance(join_time, datetime):
                    join_str = join_time.strftime('%d.%m %H:%M')
                    td = now - join_time.astimezone(timezone.utc)
                    time_str = f"{td.days}д {td.seconds//3600}ч"
                else:
                    join_str = "N/A"
                    time_str = "N/A"
                
                # Причина блокировки (используем сохранённую причину)
                reason = data.get('restrict_reason', '')
                if not reason and status == 'restricted':
                    reason = "Неизвестно"  # Для старых записей без причины
                
                # Сообщения (последние 3, до 50 символов каждое)
                msgs = [html.escape(str(t)[:50]) for t in texts[-3:] if t != "medioFuck"]
                
                all_users.append({
                    'id': user_id,
                    'name': html.escape(data.get('username', 'N/A')[:20]),
                    'handle': html.escape(data.get('user_handle', '')[:15]),
                    'status': status,
                    'join': join_str,
                    'time': time_str,
                    'clean': data.get('clean_messages', 0),
                    'reason': html.escape(reason) if reason else '',
                    'msgs': msgs,
                    'msg_count': len(texts)
                })
        
        # Хэш для определения изменений
        data_hash = hashlib.md5(str([(u['id'], u['status'], u['msg_count']) for u in all_users]).encode()).hexdigest()[:8]
        
        # Статистика
        q_count = sum(1 for u in all_users if u['status'] == 'member')
        b_count = sum(1 for u in all_users if u['status'] == 'restricted')
        
        # Генерация строк таблицы
        rows = []
        for u in all_users:
            status_badge = '🔒' if u['status'] == 'member' else '🚫' if u['status'] == 'restricted' else '❓'
            status_class = 'quarantine' if u['status'] == 'member' else 'blocked' if u['status'] == 'restricted' else ''
            msgs_html = '<br>'.join(u['msgs']) if u['msgs'] else '<i>нет</i>'
            
            rows.append(f'''<tr class="{status_class}" onclick="toggleDetails(this)">
<td>{status_badge}</td><td><code>{u['id']}</code></td><td>{u['name']}</td><td>{u['handle']}</td>
<td>{u['join']}</td><td>{u['time']}</td><td>{u['clean']}/2</td><td>{u['reason']}</td><td>{u['msg_count']}</td>
</tr><tr class="details"><td colspan="9"><div class="msgs">{msgs_html}</div></td></tr>''')
        
        table_html = ''.join(rows) if rows else '<tr><td colspan="9" class="empty">Нет пользователей</td></tr>'

        html_content = f'''<!DOCTYPE html><html lang="ru"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>🛡️ Dashboard</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:system-ui,sans-serif;background:#0f0f1a;color:#ccc;padding:15px}}
h1{{text-align:center;color:#667eea;margin-bottom:20px;font-size:1.5rem}}
.stats{{display:flex;justify-content:center;gap:30px;margin-bottom:20px}}
.stat{{text-align:center}}.stat b{{font-size:1.8rem;display:block}}
.stat.q b{{color:#ffc107}}.stat.b b{{color:#ff5252}}
table{{width:100%;border-collapse:collapse;font-size:0.85rem}}
th,td{{padding:8px 6px;text-align:left;border-bottom:1px solid #222}}
th{{background:#1a1a2e;color:#888;font-weight:500;position:sticky;top:0}}
tr.quarantine{{background:rgba(255,193,7,0.05)}}
tr.blocked{{background:rgba(255,82,82,0.05)}}
tr:hover:not(.details){{background:rgba(255,255,255,0.05);cursor:pointer}}
tr.details{{display:none}}tr.details.show{{display:table-row}}
tr.details td{{background:#1a1a2e;padding:12px}}
.msgs{{color:#888;font-size:0.8rem;line-height:1.6}}
code{{background:#252540;padding:2px 6px;border-radius:3px;color:#667eea;font-size:0.8rem}}
.empty{{text-align:center;color:#555;padding:40px}}
.footer{{text-align:center;color:#333;margin-top:20px;font-size:0.75rem}}
.hash{{color:#444}}
</style>
<script>
let lastHash="{data_hash}";
function toggleDetails(row){{let d=row.nextElementSibling;d.classList.toggle('show')}}
async function checkUpdates(){{
try{{let r=await fetch('/api/hash');let h=await r.text();
if(h!==lastHash){{location.reload()}}}}catch(e){{}}
setTimeout(checkUpdates,30000)}}
setTimeout(checkUpdates,30000);
</script>
</head><body>
<div style="text-align: right; margin-bottom: 10px;">
<a href="/limits" style="color: #667eea; text-decoration: none; border: 1px solid #667eea; padding: 5px 10px; border-radius: 5px;">⏳ Дашборд Лимитов</a>
</div>
<h1>🛡️ Anti-Spam Dashboard</h1>
<div class="stats">
<div class="stat q"><b>{q_count}</b>На карантине</div>
<div class="stat b"><b>{b_count}</b>Заблокировано</div>
</div>
<table>
<tr><th>⚡</th><th>ID</th><th>Имя</th><th>Хэндл</th><th>Вступил</th><th>Время</th><th>✓</th><th>Причина</th><th>Сообщ</th></tr>
{table_html}
</table>
<div class="footer">Обновление: {datetime.now().strftime('%H:%M:%S')} <span class="hash">#{data_hash}</span></div>
</body></html>'''
        
        return web.Response(text=html_content, content_type='text/html', charset='utf-8')

    async def handle_limits(self, request):
        """Обработчик для URL ('/limits') — таблица ограничений по времени"""
        limits = await self.db_manager.get_all_topic_limits()
        now = datetime.now(timezone.utc)
        
        rows = []
        for limit in limits:
            user_id = limit['user_id']
            username = limit.get('username') or 'Неизвестно'
            handle = limit.get('user_handle') or ''
            topic_type = limit['topic_type']
            last_msg_time = limit['last_msg_time']
            if last_msg_time.tzinfo is None:
                last_msg_time = last_msg_time.replace(tzinfo=timezone.utc)
                
            limit_hours = 24 if topic_type == 'ads' else 72
            time_passed = now - last_msg_time
            hours_passed = time_passed.total_seconds() / 3600
            
            # Статус блокировки
            is_blocked = hours_passed < limit_hours
            status_badge = '🔴' if is_blocked else '🟢'
            status_class = 'blocked' if is_blocked else 'quarantine'
            
            if is_blocked:
                left_hours = int(limit_hours - hours_passed)
                left_mins = int((limit_hours - hours_passed - left_hours) * 60)
                status_text = f"Осталось {left_hours}ч {left_mins}м"
            else:
                status_text = "Лимит прошел"
                
            time_str = last_msg_time.strftime('%d.%m %H:%M')
            
            rows.append(f'''<tr class="{status_class}">
<td>{status_badge}</td><td><code>{user_id}</code></td><td>{html.escape(username)}</td><td>{html.escape(handle)}</td><td>{topic_type}</td><td>{time_str}</td><td>{status_text}</td>
</tr>''')
        
        table_html = ''.join(rows) if rows else '<tr><td colspan="7" class="empty">Нет активных лимитов</td></tr>'

        html_content = f'''<!DOCTYPE html><html lang="ru"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>⏳ Лимиты Дашборд</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:system-ui,sans-serif;background:#0f0f1a;color:#ccc;padding:15px}}
h1{{text-align:center;color:#667eea;margin-bottom:20px;font-size:1.5rem}}
table{{width:100%;border-collapse:collapse;font-size:0.85rem}}
th,td{{padding:8px 6px;text-align:left;border-bottom:1px solid #222}}
th{{background:#1a1a2e;color:#888;font-weight:500;position:sticky;top:0}}
tr.quarantine{{background:rgba(40,167,69,0.05)}}
tr.blocked{{background:rgba(255,82,82,0.05)}}
tr:hover{{background:rgba(255,255,255,0.05)}}
code{{background:#252540;padding:2px 6px;border-radius:3px;color:#667eea;font-size:0.8rem}}
.empty{{text-align:center;color:#555;padding:40px}}
.footer{{text-align:center;color:#333;margin-top:20px;font-size:0.75rem}}
</style>
<script>
setTimeout(() => location.reload(), 30000);
</script>
</head><body>
<div style="text-align: left; margin-bottom: 10px;">
<a href="/" style="color: #667eea; text-decoration: none; border: 1px solid #667eea; padding: 5px 10px; border-radius: 5px;">🔙 Назад в Карантин</a>
</div>
<h1>⏳ Лимиты (Флуд-Контроль)</h1>
<table>
<tr><th>⚡</th><th>ID Пользователя</th><th>Имя</th><th>Хэндл</th><th>Тип топика</th><th>Последнее сообщение</th><th>Статус</th></tr>
{table_html}
</table>
<div class="footer">Обновление: {datetime.now().strftime('%H:%M:%S')}</div>
</body></html>'''
        
        return web.Response(text=html_content, content_type='text/html', charset='utf-8')

    async def handle_api_hash(self, request):
        """API для проверки изменений данных"""
        import hashlib
        async with self.user_manager.new_users_lock:
            data = [(uid, d.get('status'), len(d.get('text', []))) 
                    for uid, d in self.user_manager.new_users.items()]
        h = hashlib.md5(str(data).encode()).hexdigest()[:8]
        return web.Response(text=h, content_type='text/plain')
    
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
        app_aiogram.router.add_get('/limits', self.handle_limits)
        app_aiogram.router.add_get('/api/hash', self.handle_api_hash)
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
