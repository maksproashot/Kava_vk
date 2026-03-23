import os
import json
import re
import logging
from datetime import datetime, timedelta
import gspread
# Импорты для веб-сервера
from aiohttp import web
import aiohttp
# Импорты vkbottle для работы с payload и клавиатурой
from vkbottle import Keyboard, Text

logging.basicConfig(level=logging.INFO)

# ---------------- Настройки ----------------
VK_TOKEN = os.getenv("VK_TOKEN")
VK_GROUP_ID = os.getenv("VK_GROUP_ID")
VK_CONFIRMATION_TOKEN = os.getenv("VK_CONFIRMATION_TOKEN") # Добавлено
ALLOWED_USERS = set(map(int, os.getenv("ALLOWED_USERS", "").split(","))) if os.getenv("ALLOWED_USERS") else set()

# Google Sheets
creds_json = json.loads(os.getenv("GOOGLE_CREDS"))
client = gspread.service_account_from_dict(creds_json)
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

sheet_clients = client.open_by_key(SPREADSHEET_ID).worksheet("Clients")
sheet_history = client.open_by_key(SPREADSHEET_ID).worksheet("History")

# ---------------- Функции работы с Google Sheets ----------------
def normalize_phone(raw: str) -> str | None:
    if not raw:
        return None
    s = str(raw).strip()
    s = re.sub(r"[^\d+]", "", s)
    if s.startswith("+"):
        if s.startswith("+7") and len(s) == 12:
            return s
        digits = re.sub(r"[^\d]", "", s)
        if digits.startswith("7") and len(s) == 11:
            return "+" + digits
        return s
    if s.startswith("8") and len(s) == 11:
        return "+7" + s[1:]
    if s.startswith("7") and len(s) == 11:
        return "+7" + s[1:]
    if len(s) == 10:
        return "+7" + s
    return None

def find_client_row(phone: str):
    norm = normalize_phone(phone)
    if not norm:
        return None, None
    try:
        records = sheet_clients.get_all_records()
    except Exception as e:
        logging.exception("Ошибка чтения листа Clients: %s", e)
        return None, None

    for idx, row in enumerate(records, start=2):
        row_phone = normalize_phone(str(row.get("phone") or ""))
        if row_phone == norm:
            return idx, row
    return None, None

def ensure_client_exists(phone: str):
    norm = normalize_phone(phone)
    if not norm:
        return None, None
    row_idx, row = find_client_row(norm)
    if row_idx is not None:
        return row_idx, row
    try:
        sheet_clients.append_row([norm, 0, 0])
    except Exception as e:
        logging.exception("Ошибка при добавлении новой строки в Clients: %s", e)
        return None, None
    return find_client_row(norm)

def add_visit(phone: str):
    norm = normalize_phone(phone)
    if not norm:
        raise ValueError("Невалидный номер")

    row_idx, row = find_client_row(norm)
    if row_idx is None:
        try:
            sheet_clients.append_row([norm, 1, 0])
        except Exception as e:
            logging.exception("Ошибка при добавлении клиента (add_visit): %s", e)
            raise
        return 1, 0

    visits = int(row.get("visits") or 0) + 1
    bonuses = int(row.get("bonuses") or 0)

    if visits >= 6:
        visits = 0
        bonuses += 1

    try:
        sheet_clients.update_cell(row_idx, 2, str(visits))
        sheet_clients.update_cell(row_idx, 3, str(bonuses))
    except Exception as e:
        logging.exception("Ошибка при обновлении visits/bonuses: %s", e)
        raise

    sheet_history.append_row([
       (datetime.now() + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S"),
        norm,
        "add_visit",
        visits,
        bonuses
    ])
    limit_history_rows(5000)

    return visits, bonuses

def update_client(phone: str, action: str):
    norm = normalize_phone(phone)
    if not norm:
        raise ValueError("Невалидный номер")

    row_idx, row = ensure_client_exists(norm)
    visits = int(row.get("visits") or 0)
    bonuses = int(row.get("bonuses") or 0)

    if action == "add_visit":
        visits += 1
        if visits >= 6:
            visits = 0
            bonuses += 1
    elif action == "remove_visit":
        visits = max(0, visits - 1)
    elif action == "add_bonus":
        bonuses += 1
    elif action == "spend_bonus":
        bonuses = max(0, bonuses - 1)

    sheet_clients.update_cell(row_idx, 2, str(visits))
    sheet_clients.update_cell(row_idx, 3, str(bonuses))

    sheet_history.append_row([
       (datetime.now() + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S"),
        norm,
        action,
        visits,
        bonuses
    ])

    limit_history_rows(5000)

    return visits, bonuses

def get_history_by_phone(phone: str, limit: int = 5):
    records = sheet_history.get_all_records()
    history = []
    for r in records:
        raw_phone_in_sheet = str(r.get("phone") or "")
        norm_phone_in_sheet = normalize_phone(raw_phone_in_sheet)
        if norm_phone_in_sheet == phone:
            history.append(r)

    try:
        history.sort(key=lambda x: datetime.strptime(x["timestamp"], "%Y-%m-%d %H:%M:%S"), reverse=True)
    except Exception as e:
        logging.error(f"Error sorting history: {e}")

    return history[:limit]

def limit_history_rows(max_rows: int):
    records = sheet_history.get_all_records()
    total = len(records)
    if total > max_rows:
        rows_to_delete = total - max_rows
        start_row = 2
        end_row = start_row + rows_to_delete - 1
        try:
            sheet_history.delete_rows(start_row, end_row)
            logging.info(f"Удалено {rows_to_delete} старых строк из History.")
        except Exception as e:
            logging.error(f"Ошибка при удалении строк из History: {e}")

# ---------------- Клавиатура ----------------
def client_keyboard(phone: str):
    keyboard = (
        Keyboard(one_time=False)
        .add(Text(label="☕ Добавить покупку", payload={"cmd": "add_visit", "phone": phone}))
        .row()
        .add(Text(label="➖ Убрать покупку", payload={"cmd": "remove_visit", "phone": phone}))
        .row()
        .add(Text(label="🎁 Добавить бонус", payload={"cmd": "add_bonus", "phone": phone}))
        .row()
        .add(Text(label="🔻 Списать бонус", payload={"cmd": "spend_bonus", "phone": phone}))
    )
    return keyboard.get_json()

# ---------------- Отправка сообщения ----------------
async def send_message(user_id, message, keyboard=None):
    params = {
        "access_token": VK_TOKEN,
        "v": "5.131",
        "user_id": user_id,
        "message": message,
        "random_id": 0,
    }
    if keyboard:
        params["keyboard"] = keyboard

    async with aiohttp.ClientSession() as session:
        async with session.post("https://api.vk.com/method/messages.send", data=params) as resp:
            result = await resp.text()
            logging.info(f"Sent message to {user_id}, response: {result}")

# ---------------- Обработка событий из ВК ----------------
async def handle_message_event(event_data):
    message = event_data['object']['message']
    user_id = message['from_id']
    text = message.get('text', '')
    payload_raw = message.get('payload')

    # Проверка доступа
    if user_id not in ALLOWED_USERS:
        await send_message(user_id, "❌ У вас нет доступа к этому боту.")
        return

    # Парсим payload, если он есть
    payload = None
    if payload_raw:
        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError:
            logging.warning(f"Invalid payload received: {payload_raw}")
            return

    # Обработка команд
    if text.lower() == '/start':
        response_text = (
            "Привет! ☕\n"
            "Введи номер телефона клиента (например, +79991234567 или 89991234567), "
            "и я покажу его покупки и бонусы.\n\n"
            "Также можно использовать команду:\n"
            "/history +79991234567 — чтобы посмотреть последние 5 операций."
        )
        await send_message(user_id, response_text)

    elif text.startswith('/buy '):
        phone = text[len('/buy '):].strip()
        norm = normalize_phone(phone)
        if not norm:
            await send_message(user_id, "Невалидный формат номера. Пример: +79991234567 или 89991234567")
            return

        try:
            visits, bonuses = add_visit(norm)
            response_text = (
                f"☕ Покупка учтена!\n"
                f"📱 {norm}\n"
                f"Покупок до подарка: {6 - visits}\n"
                f"Бонусов накоплено: {bonuses}"
            )
            await send_message(user_id, response_text)
        except Exception:
            await send_message(user_id, "⚠️ Ошибка при обновлении данных. Попробуй позже.")

    elif text.startswith('/history '):
        phone = text[len('/history '):].strip()
        norm = normalize_phone(phone)
        if not norm:
            await send_message(user_id, "❌ Неверный формат номера. Пример: /history +79991234567")
            return

        history = get_history_by_phone(norm, limit=5)
        if history:
            text_history = f"📜 История операций для {norm}:\n"
            for row in history:
                date = row.get("timestamp", "N/A")
                action = row.get("action", "N/A")
                text_history += f"- {date}: {action}\n"
            await send_message(user_id, text_history)
        else:
            await send_message(user_id, "❌ История для этого номера не найдена.")

    # Обработка нажатия кнопки (payload)
    elif payload and "cmd" in payload:
        action = payload.get("cmd")
        phone = payload.get("phone")
        if not phone or not action:
            await send_message(user_id, "⚠️ Ошибка обработки кнопки.")
            return

        try:
            visits, bonuses = update_client(phone, action)
            response_text = (
                f"📱 {phone}\n"
                f"Покупок: {visits}\n"
                f"Бонусов: {bonuses}\n"
                f"До подарочного кофе: {6 - visits}"
            )
            await send_message(user_id, response_text, keyboard=client_keyboard(phone))
        except Exception as e:
            logging.exception(f"Ошибка при обработке кнопки: {e}")
            await send_message(user_id, "⚠️ Ошибка обновления")

    # Обработка ввода номера телефона
    elif re.match(r"^\+7\d{10}$|^\d{10}$|^8\d{10}$", text):
        norm = normalize_phone(text)
        if not norm:
            return

        row_idx, row = find_client_row(norm)
        if row_idx is None:
            try:
                sheet_clients.append_row([norm, 0, 0])
                row_idx, row = find_client_row(norm)
                await send_message(user_id, (
                    f"✅ Новый клиент добавлен!\n"
                    f"📱 {norm}\n"
                    f"Покупок: 0\n"
                    f"Бонусов: 0\n"
                    f"До подарочного кофе: 6"
                ))
                return
            except Exception as e:
                logging.exception("Ошибка при добавлении нового клиента: %s", e)
                await send_message(user_id, "⚠️ Не удалось добавить клиента в базу. Попробуй позже.")
                return

        visits = int(row.get("visits") or 0)
        bonuses = int(row.get("bonuses") or 0)
        await send_message(user_id, (
            f"📱 {norm}\n"
            f"Покупок: {visits}\n"
            f"Бонусов: {bonuses}\n"
            f"До подарочного кофе: {6 - visits}"
        ), keyboard=client_keyboard(norm))

# ---------------- Веб-сервер (aiohttp) ----------------
async def handle_callback(request):
    data = await request.json()

    # Подтверждение webhook
    if data.get('type') == 'confirmation':
        logging.info("Received confirmation request from VK.")
        return web.Response(text=VK_CONFIRMATION_TOKEN) # Возвращаем токен подтверждения

    # Обработка нового сообщения
    elif data.get('type') == 'message_new':
        logging.info(f"Received message_new event: {data}")
        await handle_message_event(data)
        # Обязательно вернуть "ok"
        return web.Response(text='ok')

    # На другие типы событий тоже нужно отвечать "ok", если они вам не интересны
    # Или можно просто вернуть "ok" всегда, если вы не ожидаете других типов
    logging.info(f"Received unknown event type: {data.get('type')}")
    return web.Response(text='ok')

app = web.Application()
app.router.add_post('/callback', handle_callback)

if __name__ == "__main__":
    # Запуск сервера (укажите порт из переменной окружения, например, на Render)
    port = int(os.getenv("PORT", 8000))
    web.run_app(app, host="0.0.0.0", port=port)