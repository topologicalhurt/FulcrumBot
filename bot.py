from pathlib import Path
from dotenv import dotenv_values
from pydantic import BaseModel
from dataclasses import dataclass
from discord.ext import commands
from datetime import datetime
import os
import sys
import asyncio
import discord
import json
import logging
import logging.handlers
import subprocess
import argparse
import regex as re
import threading as thr


class EnvMixedIn(BaseModel):
    DISCORD_FULCRUMBOT_CHANNELID: int
    DOCKER_DAEMON_MAXCHECKS: int
    DOCKER_DAEMON_POLLTIME: float
    MCSERVER_TMP_VOLUME_LOCATION: Path


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

CONTAINER_REG = re.compile(r'^(\d+\.){2}\d+')


def ver_type(arg_value) -> str:
    if not CONTAINER_REG.match(arg_value):
        raise argparse.ArgumentTypeError(f'Must be formatted as the version number stripped of \'.\' if container name is $(versionstamp)-mc-$(digit)')
    return arg_value.replace('.', '')


PARSER.add_argument(
    'ver',
    type=ver_type,
    help='The version number of the server to target. I.e. if there is a docker container running 1.19.3 as per the project-structure docs then "1.19.3"'
)
PARSER.add_argument(
    '-d', 
    action='store_true',
    help='Enable dev. mode'
)
PARSER.add_argument(
    '-ldd', 
    action='store_true',
    help='Launch docker daemon. Use this option to attempt a launch of the docker daemon specified in env.shared'
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

if PARSED.ldd and os.name != 'nt':
    logging.warning('The docker daemon could not start')
    raise argparse.ArgumentTypeError('No host system support for -ldd opt (NT builds only)') from\
        NotImplementedError('Non-fatal: -ldd tag is not compatible with anything but windows currently. Please ensure the daemon is running manually.')


LOGGER = logging.getLogger('discord')

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
    _PSNAME_REG = re.compile(r'(?:NAMES\n)(\d+-mc-\d+\n*)')
    _CONTAINER_SUBVER_REG = re.compile(r'(\d+)$')
    _USR_ARGS_REG = re.compile(r'^(?:--|-){1}[a-z]((?:[a-z])|(?:-)(?!-))+')
    _COOLDOWN_MSG_REG = re.compile(r'(\d+\.\d+s)')

    def __init__(self, client):
        self._client = client
        self._session = Session()
        if PARSED.ldd:
            self._daemon_checked=False
    
    
    @staticmethod
    def catchSubProcess(log_msg, err_msg, stack_trace_off=False):
        def outer_wrapper(func):
            async def wrapper(*args, **kwargs):
                try:
                    return await func(*args, **kwargs)
                except subprocess.CalledProcessError as err:
                    logging.error(log_msg)
                    if stack_trace_off:
                        sys.excepthook = excepthook
                    # If the program should exit gracefully
                    # sys.exit(1)
                    raise RuntimeError(err_msg) from err
            return wrapper
        return outer_wrapper
    
    @catchSubProcess('Docker ps failed. Probably because the docker daemon isn\'t running',
                     'Failed to find a docker process to inject', 
                     True)
    async def _dockerps(self):
        cmd = f'docker ps -a -f "NAME={PARSED.ver}-mc-\d+" --format "table {{{{.Names}}}}"'
        if PARSED.ldd and not self._daemon_checked:
            logging.warning('Attempting to force load the Docker Daemon. This is not a reliable process and relies on the Daemon self-reporting as a process')
            subprocess.Popen(['cmd','/c', CONFIG['DOCKER_DESKTOP_EXEC']])
            check = subprocess.run(cmd, shell=True, capture_output=True)
            n_checks = 0
            while check.returncode != 0 and n_checks < CONFIG['DOCKER_DAEMON_MAXCHECKS']:
                logging.info(f'{n_checks}: Polling the docker daemon...')
                await asyncio.sleep(CONFIG['DOCKER_DAEMON_POLLTIME'])
                check = subprocess.run(cmd, shell=True, capture_output=True)
                n_checks += 1
            if n_checks == CONFIG['DOCKER_DAEMON_MAXCHECKS']:
                raise subprocess.CalledProcessError('Couldn\'t force the docker daemon to load')
            self._daemon_checked = True
            return check.stdout.decode('ascii')
        else:
            return subprocess.check_output(cmd, shell=True, stderr=subprocess.DEVNULL).decode('ascii')
        
    
    async def _parse_dockerps(self) -> str:
        raw = await self._dockerps()
        valid_vers = re.findall(BotHandler._PSNAME_REG, raw)
        max_subver = (float('-infinity'), None)
        for i, v in enumerate(valid_vers):
            valid_vers[i] = valid_vers[i].strip()
            subver = int(re.search(BotHandler._CONTAINER_SUBVER_REG, v).group(0))
            max_subver = max(max_subver, (subver, i))
        return valid_vers[max_subver[1]]
    
    @catchSubProcess('Failure to launch the docker target',
                     'Failure to launch the docker target container. The daemon probably failed some time after parsing began')
    async def _run_docker_target(self, tmp_session=False):
        container_id = await self._parse_dockerps()
        if tmp_session:
            tmp_dir = CONFIG["MCSERVER_TMP_VOLUME_LOCATION"].joinpath('tmp')
            list_dir = os.listdir(tmp_dir)
            max_dir = float('-infinity') if list_dir else 0
            for fn in os.listdir(tmp_dir):
                max_dir = max(max_dir, int(re.search(BotHandler._CONTAINER_SUBVER_REG, fn).group(0)))
            tmp_mount = tmp_dir.joinpath(f'tmp-mc-{max_dir + 1}')
            os.mkdir(tmp_mount)
            child = subprocess.Popen(
                                    f'docker run -d -it -p 25565:25565 -e EULA=TRUE -v "{tmp_mount}":/data itzg/minecraft-server',
                                    stdout=subprocess.PIPE
                                    )
        else:
            child = subprocess.Popen(
                                    f'docker start {container_id}',
                                    stdout=subprocess.PIPE
                                    )
        # TODO: 
        # (I) Start the listener thread that pipes input, stdout between the discord and the spawned shell
        # (II) Whitelist certain commands
        # (III) Add more options

    async def _spawn_server_session(self, ctx, *args):
        opts = {}
        for a in args:
            match a:
                case '--tmp':
                    opts['tmp_session'] = True
                case _:
                    # Unrecognised command
                    pass

        if ctx.message.created_at.timestamp() - self._session.start < \
            BotHandler._threshold_between_restarts:
            self._session.active = False
            await ctx.send(f'A session is already currently running!')
            return
        
        await self._run_docker_target(**opts)
        
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
    
    @staticmethod
    def _validate_usr_args(*args) -> str | None:
        if not args:
            return
        args = list(args)
        start = 0
        for i, a in enumerate(args):
            if not re.match(BotHandler._USR_ARGS_REG, a):
                underline_err = ' ' * start + '^' * len(a)
                # Remove formatting
                err_wrd = '**__' + args[i].replace('*', '\*').replace('~', '\~').replace('_', '\_') + '__**'
                # Do lookup here on the arg to give specific command info
                return (
                            '**ERROR:** *Invalid command string (!help for more)* Error occured while parsing the following word: '
                            f'{err_wrd}'
                            '```'
                            f'{" ".join(args)}\n'
                            f'{underline_err}'
                            '```'
                        )
            start += len(a) + 1
        return
    
    @commands.command()
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def start(self, ctx, *args):
        err = BotHandler._validate_usr_args(*args)
        if err:
            await ctx.reply(err)
            return 1
        await self._spawn_server_session(ctx, *args)
        return 0

    @commands.Cog.listener()
    async def on_command_error(self, ctx, err, *_):
        if isinstance(err, discord.ext.commands.errors.CommandOnCooldown):
            t_left = re.search(BotHandler._COOLDOWN_MSG_REG, str(err)).group()
            await ctx.reply(
                            'Woah... Don\'t smack that double penjamin too fast\n\n'
                            '**ERROR:** *Timeout exception* You\'re issuing commands too fast.'
                            '```'
                             f'You have a time of {t_left} left before you can issue that command again'
                            '```'
                            )
            return 0
        raise err


def main():
    LOGGER.setLevel(logging.INFO)
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
    LOGGER.addHandler(handler)

    intents = discord.Intents.all()
    intents.message_content = True
    botClient = BotClient(intents=intents)
    if not PARSED.lco:
        botClient.run(CONFIG['DISCORD_FULCRUMBOT_APITOKEN'])


if __name__ == '__main__':
    main()

