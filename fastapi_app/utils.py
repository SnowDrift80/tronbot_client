# fastapi_app/utils.py

from telegram import Bot
from config import CONFIG
import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Utils:
    @staticmethod
    async def bot_message(chat_id, message: str):
        """
        Sends a message to a Telegram chat using the configured bot.

        Args:
            chat_id (int): The chat ID to send the message to.
            message (str): The message content to send.

        Raises:
            Exception: If there is an error while sending the message via Telegram.

        """
        logger.info(f"Sending message to chat ID {chat_id}: {message}")

        try: 
            bot = Bot(CONFIG.TELEGRAM_KEY)
            await bot.sendMessage(chat_id, message, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error while trying to send message to Telegram user: {e}")
