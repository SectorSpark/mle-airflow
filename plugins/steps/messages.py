from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма
import logging

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными

    hook = TelegramHook(telegram_conn_id='Telegram_conn')
    dag = context['dag']
    run_id = context['run_id']
    chat_id = hook.conn.extra_dejson.get('chat_id')
    logging.info(f'Sending success message to chat_id: {chat_id}')
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': chat_id,
        'text': message
    }) # отправление сообщения

def send_telegram_failure_message(context):
    hook = TelegramHook(telegram_conn_id='Telegram_conn')
    dag = context['dag']
    run_id = context['run_id']
    chat_id = hook.conn.extra_dejson.get('chat_id')
    logging.info(f'Sending failure message to chat_id: {chat_id}')
    message = f'Исполнение DAG {dag} с id={run_id} прошло неудачно!' # определение текста сообщения
    hook.send_message({
        'chat_id': chat_id,
        'text': message
    }) # отправление сообщения