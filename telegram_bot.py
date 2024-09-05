"""
refactoring initialization of telegram bot:
we need the telegram application object in main.py and in fastapi_app.py
and therefore we will import the telegram_bot.py into both, main.py and fastapi_app.py
We want the telegram application object because it handles the rate limiter of telegram
with some internal qeueue feature.
"""

from telegram.ext import Application
from config import CONFIG

# Initialize the Telegram bot application instance
application = Application.builder().token(CONFIG.TELEGRAM_KEY).build()
