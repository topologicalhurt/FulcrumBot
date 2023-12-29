from pathlib import Path
from dotenv import dotenv_values
from pydantic import BaseModel, Field
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
    '--dev', 
    action='store_true',
    help='Enable dev. mode'
)
PARSER.add_argument(
    '--ldd', 
    action='store_true',
    help='Launch docker daemon. Use this option to attempt a launch of the docker daemon specified in env.shared'
)
PARSER.add_argument(
    '--lco',
    action='store_true',
    help='Launch client off. Use this option to disable the client from contacting the discord gateway service'
)
PARSER.add_argument(
    '--c',
    action='store_true',
    help=(
        'Launch the bot in channel mode. '
        'This causes the bot to report debug info in the channel ID specified in env.shared'
    )
)
PARSER.add_argument(
    '--mock',
    action='store_true',
    help='In mock mode any expensive subshell commands are not spawned'
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
        if PARSED.dev:
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
    _MAX_ARG_WRD_SZ = 128
    

    class StartArgs(BaseModel):
        alloc: float = Field(alias='-alloc')
        tmp: bool = Field(default=0, alias='--tmp')


    def __init__(self, client):
        self._client = client
        self._session = Session()
        if PARSED.ldd:
            self._daemon_checked=False
        
        # Build context for the arg models
        self._model_context = {}
        models = [BotHandler.StartArgs]
        for m in models:
            for f in m.model_fields.values():
                # TODO: fix to accomodate other valid var names
                if not f.alias.isalpha() or (f.alias.startswith('-') and not f.alias.startswith('--')):
                   continue
                self._model_context[f.alias] += 1  # Number of positional args
    
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
    async def _run_docker_target(self, opts: int):
        if not PARSED.mock:
            container_id = await self._parse_dockerps()
            if opts & 1:
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

    async def _spawn_server_session(self, ctx, opts, *args):
        if not PARSED.mock and ctx.message.created_at.timestamp() - self._session.start < \
            BotHandler._threshold_between_restarts:
            self._session.active = False
            await ctx.send(f'A session is already currently running!')
            return
        
        await self._run_docker_target(opts)
        
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
    def _remove_formatting_from_arg(arg: str) -> str:
        return arg.replace('*', '\*').replace('~', '\~').replace('_', '\_')
    
    @staticmethod
    def _arg_is_float_or_int(arg: str) -> bool:
        return arg.count('-') <= 1 and arg.count('.') <= 1 and arg.replace('.', '').replace('-', '').isdigit()

    @staticmethod
    def _get_float_from_arg(arg: str) -> float:
        return float(arg[1:]) * -1 if arg.startswith('-') else float(arg)

    def _validate_usr_args(self, arg_types: BaseModel, *args) -> tuple:
        args = list(args)
        start, opts = 0, 0
        stack = []
        kwargs = {}

        # Check that the total number of args matches what the schema specifies
        # if len(args) > len(arg_types.model_fields):
        #     return f'**ERROR:** *This command specifies the following number of args: {len(arg_types.model_fields)} (!help for more)*', opts, {}

        # Check that each arg is of a particular size (avoid buffer attacks)
        err = next((a for a in args if len(a) > BotHandler._MAX_ARG_WRD_SZ), None)
        if err:
            err = BotHandler._remove_formatting_from_arg(err)
            underline_err = ' ' * sum([len(a) for a in args[:args.index(err)]]) + ' ' + '^' * len(err)
            return (
                        f'**ERROR:** *One of the chars was too large (> {BotHandler._MAX_ARG_WRD_SZ})*. Error occured while '
                        f'parsing the following word: **__{err}__**'
                        '```'
                        f'{" ".join(args)}\n'
                        f'{underline_err}'
                        '```'
            ), opts, kwargs
        
        # Check each arg is encoded in ascii
        err = next((a for a in args if any(not ch.isascii() for ch in a)), None)
        if err:
            err = BotHandler._remove_formatting_from_arg(err)
            underline_err = ' ' * sum([len(a) for a in args[:args.index(err)]]) + ' ' + '^' * len(err)
            return (
                        '**ERROR:** *Provided a non-ascii value as an arg*. Error occured while '
                        f'parsing the following word: **__{err}__**'
                        '```'
                        f'{" ".join(args)}\n'
                        f'{underline_err}'
                        '```'
            ), opts, kwargs
            
        for i, a in enumerate(args):

            # Case I: Handle the case when a positional arg is provided
            # Case II: Check the arg is formatted as a valid flag if it isn't positional
            arg_is_float = BotHandler._arg_is_float_or_int(a)
            if arg_is_float or a.isalpha():
                parsed_arg = BotHandler._get_float_from_arg(a) if arg_is_float else a

                # TODO: replace args[i-1] with some ptr to the last seen flag

                # Case I: Last seen is an option or didn't match the regex => only other possibility is for arg to be a positional
                # Case II: Last seen neither alpha nor float => must have matched regex => must not be an option => arg is NOT positional
                if args[i-1].startswith('--') or not i or args[i-1].isalpha() or BotHandler._arg_is_float_or_int(args[i-1]):
                    stack.append(parsed_arg)
                else:
                    # Found a matching flag, but the type of the matching positional is wrong
                    if args[i-1][1:] not in arg_types.model_fields:
                        err_msg = f'The supplied arg: {args[i-1]} was not found'
                        logging.info(err_msg)
                        raise ValueError(err_msg)
                    elif not arg_types.model_fields[args[i-1][1:]].annotation is type(parsed_arg):
                        return (
                                    '**ERROR:** *Provided a positional argument but the corresponding flag is of an imcompatible type '
                                    f'(!help for more)* Error occured while parsing the following arg and its corresponding positional arg: '
                                    f'**__{args[i-1]}__**, **__{BotHandler._remove_formatting_from_arg(a)}__**\n'
                                    '```'
                                    f'{args[i-1]} requires the following type: {arg_types.model_fields[args[i-1][1:]].annotation.__name__}'
                                    '```'
                        ), opts, kwargs
                    else:
                        kwargs[args[i-1]] = parsed_arg
                
            elif not re.match(BotHandler._USR_ARGS_REG, a):
                underline_err = ' ' * start + '^' * len(a)
                # TODO: lookup here on the arg to give specific command info
                return (
                            '**ERROR:** *Invalid command string (!help for more)* Error occured while parsing the following word: '
                            f'**__{BotHandler._remove_formatting_from_arg(a)}__**'
                            '```'
                            f'{" ".join(args)}\n'
                            f'{underline_err}'
                            '```'
                ), opts, kwargs

            # Pack opts into int
            if a.startswith('--'):
                # TODO: check that the option exists
                if a[2:] in arg_types.model_fields:
                    i = arg_types.model_fields[a[2:]].default
                    if isinstance(i, int):
                        opts |= 1 << i
            
            start += len(a) + 1  # Track the sum of arg wrd lengths for error reporting
        
        # TODO: Check that the number of positional args matches the number in the model context, add them as *args
        # Go over the stack to get the positional arguments
        # for s in stack:
        #     pass

        if PARSED.dev:
            print(kwargs, stack, opts, self._model_context)

        return None, opts, kwargs
    
    @commands.command()
    @commands.cooldown(1, 10, commands.BucketType.user)
    async def start(self, ctx, *args):
        err, opts, flags = self._validate_usr_args(BotHandler.StartArgs, *args)
        if err:
            await ctx.reply(err)
            return 1
        await self._spawn_server_session(ctx, opts, *flags)
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

