import psycopg2
import asyncio
from telegram import Bot

BOT_TOKEN = "8468700802:AAH-FDc3iByd-9ScugYoUbF744bRftn4h8k"
CHAT_ID = "6056225171"

async def main():
    bot = Bot(token=BOT_TOKEN)

    conn = psycopg2.connect(
        host="localhost",
        database="market",
        user="fin",
        password="finpass"
    )

    cur = conn.cursor()
    cur.execute("""
        SELECT signal, direction, price, created_at
        FROM signals.live_signals
        ORDER BY created_at DESC
        LIMIT 1
    """)

    s = cur.fetchone()
    msg = f"📈 {s[0]} → {s[1]} @ {s[2]} | {s[3]}"

    await bot.send_message(chat_id=CHAT_ID, text=msg)

if __name__ == "__main__":
    asyncio.run(main())
