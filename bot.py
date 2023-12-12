from pathlib import Path
from dotenv import dotenv_values
from pydantic import BaseModel
from dataclasses import dataclass
from discord.ext import commands
from datetime import datetime
from contextlib import contextmanager
import sys
import asyncio
import discord
import json
import logging
import logging.handlers
import subprocess
import argparse


class EnvMixedIn(BaseModel):
    DISCORD_FULCRUMBOT_CHANNELID: int


@dataclass
class Session:
    _active: bool = False
    _start: int = 0

    @property
    def active(self):
        return self._active
    
    @active.setter
    def active(self, v):
        self._active = v
    
    @property
    def start(self):
        return self._start
    
    @start.setter
    def start(self, v):
        self._start = v


def excepthook(type, value, traceback):
    sys.stderr.write(value)


PARSER = argparse.ArgumentParser(
    prog='FulcrumBot',
    description='https://discord.com/developers/applications/1183755813261680701/information'
)
PARSER.add_argument(
    '-d', 
    action='store_true',
    help='Enable dev. mode'
)
PARSER.add_argument(
    '-lco',
    action='store_true',
    help='Launch client off. Use this option to disable the client from contacting the discord gateway service'
)
PARSER.add_argument(
    '-c',
    action='store_true',
    help=(
        'Launch the bot in channel mode. '
        'This causes the bot to report in the channel ID specified in env.shared'
    )
)
PARSED = PARSER.parse_args()


CONFIG = {
    **dotenv_values(Path('./.env.secret')),
    **dotenv_values(Path('./.env.shared'))
}
env_mixed = EnvMixedIn(**CONFIG).model_dump() 
CONFIG.update(env_mixed)

with open('bot_settings.json', 'r') as f:
    BOT_SETTINGS = json.load(f)


class BotClient(commands.Bot):
    def __init__(self, intents):
        self._start = datetime.timestamp(datetime.now())
        opts = BOT_SETTINGS['constructor']
        super().__init__(intents=intents, **opts)
        asyncio.run(self.add_cogs())
        
    async def on_ready(self):
        channel = self.get_channel(CONFIG['DISCORD_FULCRUMBOT_CHANNELID'])
        if PARSED.d:
            msg = f'Bot is ready after {datetime.timestamp(datetime.now()) - self._start}s'
            logging.info(msg)
            if PARSED.c:
                await channel.send(msg)
    
    async def add_cogs(self):
        await self.add_cog(BotHandler(self))


class BotHandler(commands.Cog):

    _threshold_between_restarts = BOT_SETTINGS['server']['restart_threshold']

    def __init__(self, client):
        self._client = client
        self._session = Session()
    
    @contextmanager
    @staticmethod
    def _dockerps():
        try:
            yield subprocess.check_output('docker ps', shell=True)
        except subprocess.CalledProcessError as err:
            logging.error('Docker ps failed. Probably because the docker daemon isn\'t running')
            sys.excepthook = excepthook
            raise RuntimeError('Failed to find a docker process to inject') from err
    
    def _parse_dockerps(self):
        with BotHandler._dockerps() as dps:
            pass
    
    def _run_docker_target(self):
        pass

    
    async def _spawn_server_session(self, ctx):
        if ctx.message.created_at.timestamp() - self._session.start < \
            BotHandler._threshold_between_restarts:
            self._run_docker_target()
            self._session.active = False
            await ctx.send(f'A session is already currently running!')
            return
        
        self._run_docker_target()
        
        self._session.active = True
        self._session.start = ctx.message.created_at.timestamp()

        hours, r = divmod(self._threshold_between_restarts, 3600)
        minutes, seconds = divmod(r, 60)

        await ctx.reply(
            (
                f'Yuhhhhh! Fulcrum come in. You are a true yodie gang member {ctx.author.mention} '
                'Penjamin city, shall we? Wagwan brotha time to inundate ya with stats ya feel me?\n\n'
                '```'
                'Starting a new server session...\n'
                f'Request origin: {ctx.author.name}\n'
                f'Request session start @ '
                f'{datetime.fromtimestamp(self._session.start).strftime("%Y-%m-%d %H:%M:%S")}\n'
                f'Request cool-down window: {hours:02}h:{minutes:02}m:{seconds:02}s'
                '```'
            )
        )
    
    @commands.hybrid_command()
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def start(self, ctx):
        await self._spawn_server_session(ctx)


def main():
    logger = logging.getLogger('discord')
    logger.setLevel(logging.INFO)

    handler = logging.handlers.RotatingFileHandler(
        filename='discord.log',
        encoding='utf-8',
        maxBytes=32 * 1024 * 1024,  # 32 MiB
        backupCount=5,  # Rotate through 5 files
    )
    dt_fmt = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter('[{asctime}] [{levelname:<8}] {name}: {message}', dt_fmt, style='{')
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)

    intents = discord.Intents.all()
    intents.message_content = True
    botClient = BotClient(intents=intents)
    if not PARSED.lco:
        botClient.run(CONFIG['DISCORD_FULCRUMBOT_APITOKEN'])


if __name__ == '__main__':
    main()

