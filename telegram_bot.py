"""
refactoring initialization of telegram bot:
we need the telegram application object in main.py and in fastapi_app.py
and therefore we will import the telegram_bot.py into both, main.py and fastapi_app.py
We want the telegram application object because it handles the rate limiter of telegram
with some internal qeueue feature.
"""

from telegram.ext import Application
from config import CONFIG
import asyncio


class AlgoEagleTelegramBot(Application):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.shutdown_event = asyncio.Event()  # Create the shutdown event

    async def stop(self):
        self.shutdown_event.set()  # Set the shutdown event
        print("CUSTOM STOP METHOD IN AlgoEagleTelegramBot")
        await super().stop()

    @classmethod
    def custom_builder(cls):
        # buld an Application instance first
        app = cls.builder().token(CONFIG.TELEGRAM_KEY).build()
        
        # convert the base class instance (Application) into a subclass instance (AlgoEagleTelegramBot)
        app.__class__ = cls # change the instance's class to AlgoEagleTelegramBot

        # manually set property defined in the subclass, in this case shutdown_event
        app.shutdown_event = asyncio.Event()
        return app
   

# Initialize the Telegram bot application instance
application = AlgoEagleTelegramBot.custom_builder()

print(f"SELF.SHUTDOWN_EVENT = {application.shutdown_event.is_set()}")