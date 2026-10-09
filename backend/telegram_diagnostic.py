import asyncio
import logging
from app.core.config import get_settings
from app.telegram.client import TelegramClientFactory

async def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("telethon").setLevel(logging.DEBUG)

    settings = get_settings()
    factory = TelegramClientFactory(
        api_id=settings.telegram_api_id,
        api_hash=settings.telegram_api_hash.get_secret_value(),
        timeout=20,
        connection_retries=1,
    )
    client = factory.create()
    try:
        print("Connecting to Telegram...")
        await asyncio.wait_for(client.connect(), timeout=25)
        print("Connected:", client.is_connected())
    except Exception as exc:
        print("ERROR TYPE:", type(exc).__name__)
        print("ERROR:", str(exc)[:300])
    finally:
        await client.disconnect()

asyncio.run(main())
