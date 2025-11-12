import discord
from discord import app_commands
from discord.ext import commands, tasks
import aiohttp
import aiosqlite
import uuid
import random
import time
import asyncio
import os
from dotenv import load_dotenv
from typing import Optional
from datetime import datetime, timedelta, timezone
import logging
from contextlib import asynccontextmanager

#=============== setups ===============#
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix='-', intents=intents, help_command=None)

SEARCH_URL = 'https://ch.tetr.io/api/users/search/discord:id:{}'
USER_URL   = "https://ch.tetr.io/api/users/{}"
SUMMARIES_URL = "https://ch.tetr.io/api/users/{}/summaries"

ALLOWED_GUILD = [946060638231359588, #TAC
                 854031901718609962  #test server
                 ]

MODS_ROLE = [1246417236046905387 #CEO of Stupid
            ,946061277183230002  #Mod
            ,1308704910409207869 #Temp.M
            ,856862529946255360  #admin (in test server)
            ]

rank_to_role = {
     'z' : 'Unranked (lmao)'
    ,'d' : 'D Rank'
    ,'d+': 'D+ Rank'
    ,'c-': 'C- Rank'
    ,'c' : 'C Rank'
    ,'c+': 'C+ Rank'
    ,'b-': 'B- Rank'
    ,'b' : 'B Rank'
    ,'b+': 'B+ Rank'
    ,'a-': 'A- Rank'
    ,'a' : 'A Rank'
    ,'a+': 'A+ Rank'
    ,'s-': 'S- Rank'
    ,'s' : 'S Rank'
    ,'s+': 'S+ Rank'
    ,'ss': 'SS Rank'
    ,'u' : 'U Rank'
    ,'x' : 'X Rank'
    ,'x+': 'X+ Rank'
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s'
)

temp_bl = set() #blacklisted ids

session: Optional[aiohttp.ClientSession] = None

tables = {
    "users": {
        "country": lambda rsp: safe_get(rsp, "data", "country", default="null"),
        "ar"     : lambda rsp: safe_get(rsp, "data", "ar", default=0)
    },
    "tl": {
        "rd"       : lambda rsp: safe_get(rsp, "data", "league", "rd", default=-1),
        "tr"       : lambda rsp: safe_get(rsp, "data", "league", "tr", default=-1),
        "rank"     : lambda rsp: safe_get(rsp, "data", "league", "rank", default="z"),
        "best_rank": lambda rsp: safe_get(rsp, "data", "league", "best_rank", default="null"),
        "apm"      : lambda rsp: safe_get(rsp, "data", "league", "apm", default=-1),
        "pps"      : lambda rsp: safe_get(rsp, "data", "league", "pps", default=-1),
        "vs"       : lambda rsp: safe_get(rsp, "data", "league", "vs", default=-1)
    },
    "tl_past": lambda rsp: safe_get(rsp, "data", "league", "past", default={}),
    "40l": {
        "id"  : lambda rsp: safe_get(rsp, "data", "40l", "record", "replayid", default="null"),
        "time": lambda rsp: safe_get(rsp, "data", "40l", "record", "results", "stats", "finaltime", default=-1)
    },
    "blitz": {
        "id"   : lambda rsp: safe_get(rsp, "data", "blitz", "record", "replayid", default="null"),
        "score": lambda rsp: safe_get(rsp, "data", "blitz", "record", "results", "stats", "score", default=-1)
    },
    "zenith": {
        "w_id"      : lambda rsp: safe_get(rsp, "data", "zenith", "record", "replayid", default=None),
        "w_altitude": lambda rsp: safe_get(rsp, "data", "zenith", "record", "results", "stats", "zenith", "altitude", default=-1),
        "b_id"      : lambda rsp: safe_get(rsp, "data", "zenith", "best", "record", "replayid", default=None),
        "b_altitude": lambda rsp: safe_get(rsp, "data", "zenith", "best", "record", "results", "stats", "zenith", "altitude", default=-1)
    },
    "zenithex": {
        "w_id"      : lambda rsp: safe_get(rsp, "data", "zenithex", "record", "replayid", default=None),
        "w_altitude": lambda rsp: safe_get(rsp, "data", "zenithex", "record", "results", "stats", "zenith", "altitude", default=-1),
        "b_id"      : lambda rsp: safe_get(rsp, "data", "zenithex", "best", "record", "replayid", default=None),
        "b_altitude": lambda rsp: safe_get(rsp, "data", "zenithex", "best", "record", "results", "stats", "zenith", "altitude", default=-1)
    },
    "zen": lambda rsp: safe_get(rsp, "data", "score", default=-1)
}

#=============== functions ===============#
async def init_db():
    async with aiosqlite.connect('db.db') as db:
        await db.execute('''
                         CREATE TABLE IF NOT EXISTS users(
                         discord_id TEXT PRIMARY KEY,
                         tetrio_username TEXT UNIQUE,
                         ar INTEGER,
                         country TEXT
                         )
                         ''')

        await db.execute('''
                         CREATE TABLE IF NOT EXISTS tl(
                         tetrio_username TEXT PRIMARY KEY,
                         rd REAL,
                         tr REAL,
                         rank TEXT,
                         best_rank TEXT,
                         apm REAL,
                         pps REAL,
                         vs REAL,
                         FOREIGN KEY(tetrio_username) REFERENCES users(tetrio_username)
                         )
                         ''')

        await db.execute('''
                         CREATE TABLE IF NOT EXISTS tl_past(
                         tetrio_username TEXT,
                         season INTEGER,
                         rd REAL,
                         tr REAL,
                         rank TEXT,
                         best_rank TEXT,
                         apm REAL,
                         pps REAL,
                         vs REAL,
                         PRIMARY KEY(tetrio_username, season),
                         FOREIGN KEY(tetrio_username) REFERENCES users(tetrio_username)
                         )
                         ''')

        await db.execute('''
                         CREATE TABLE IF NOT EXISTS "40l"(
                         tetrio_username TEXT PRIMARY KEY,
                         id TEXT,
                         time REAL,
                         FOREIGN KEY(tetrio_username) REFERENCES users(tetrio_username)
                         )
                         ''')
        
        await db.execute('''
                         CREATE TABLE IF NOT EXISTS blitz(
                         tetrio_username TEXT PRIMARY KEY,
                         id TEXT,
                         score INTEGER,
                         FOREIGN KEY(tetrio_username) REFERENCES users(tetrio_username)
                         )
                         ''')
        
        await db.execute('''
                         CREATE TABLE IF NOT EXISTS zenith(
                         tetrio_username TEXT PRIMARY KEY,
                         w_id TEXT,
                         b_id TEXT,
                         w_altitude REAL,
                         b_altitude REAL,
                         FOREIGN KEY(tetrio_username) REFERENCES users (tetrio_username)
                         )
                         ''')
        
        await db.execute('''
                         CREATE TABLE IF NOT EXISTS zenithex(
                         tetrio_username TEXT PRIMARY KEY,
                         w_id TEXT,
                         b_id TEXT,
                         w_altitude REAL,
                         b_altitude REAL,
                         FOREIGN KEY(tetrio_username) REFERENCES users(tetrio_username)
                         )
                         ''')
        
        await db.execute('''
                         CREATE TABLE IF NOT EXISTS zen(
                         tetrio_username TEXT PRIMARY KEY,
                         xp REAL,
                         FOREIGN KEY(tetrio_username) REFERENCES users(tetrio_username)
                         )
                         ''')
        
        await db.execute('''
                         CREATE TABLE IF NOT EXISTS tournament(
                         id TEXT PRIMARY KEY,
                         name TEXT NOT NULL,
                         date INTEGER NOT NULL,
                         regis_start INTEGER NOT NULL,
                         regis_end INTEGER NOT NULL,
                         min_rank TEXT,
                         max_rank TEXT,
                         info TEXT,
                         winner TEXT
                         )
                         ''')
        
        await db.execute('''
                         CREATE TABLE IF NOT EXISTS tournament_regis(
                         id TEXT NOT NULL,
                         tetrio_username TEXT NOT NULL,
                         tournament_name TEXT NOT NULL,
                         PRIMARY KEY(tetrio_username, tournament_name, id),
                         FOREIGN KEY(id) REFERENCES tournament(id),
                         FOREIGN KEY(tournament_name) REFERENCES tournament(name),
                         FOREIGN KEY(tetrio_username) REFERENCES users(tetrio_username)
                         )
                         ''')
        
        await db.execute('''
                         CREATE TABLE IF NOT EXISTS counters(
                         key TEXT PRIMARY KEY,
                         value INTEGER,
                         last_update INTEGER,
                         message TEXT
                         )
                         ''')
        
        await db.commit()

@asynccontextmanager
async def connect_db():
    db = await aiosqlite.connect("db.db")
    await db.execute("PRAGMA foreign_keys = ON;")
    db.row_factory = aiosqlite.Row
    try:
        yield db
    finally:
        await db.close()   

async def mod_check(ctx: commands.Context):

    user = getattr(ctx, 'user', None) or getattr(ctx, 'author', None)

    if not user or not hasattr(user, 'roles'):
        return False
    
    return any(role.id in MODS_ROLE for role in user.roles)

def unix_parser(date_str: str, time_format: str = "HH:MM dd/mm/yy", tz_offset: int = 0):
    if not date_str:
        raise ValueError("Date string cannot be empty")
    
    if not -24 <= tz_offset <= 24:
        raise ValueError("Timezone offset must be between -24 and +24")


    fmt = time_format
    try:
        fmt = fmt.replace("HH", "%H").replace("MM", "%M").replace("SS","%S")
        fmt = fmt.replace("dd", "%d").replace("mm", "%m")
        fmt = fmt.replace("yyyy", "%Y") if "yyyy" in time_format else fmt.replace("yy", "%y")
        
        dt = datetime.strptime(date_str, fmt)
        if dt.year < 100:
            dt = dt.replace(year=dt.year + 2000)  # Assume 20xx for 2-digit years
            
        dt = dt + timedelta(hours=tz_offset)
        return int(dt.astimezone(timezone.utc).timestamp())
        
    except Exception as e:
        logging.error(f"[unix_parser] Failed to parse '{date_str}' with format '{time_format}': {type(e).__name__}: {e}")
        raise ValueError(f"Invalid date/time format: '{date_str}'. Expected format: {fmt}") from e

async def api_request(url: str, retries=3):
    global session

    if session is None:
        session = aiohttp.ClientSession()

    headers = {
        "User-Agent": "TAC-helper (https://github.com/funli69/TAC-helper)",
        "Accept": "application/json",
        "X-Session-ID": str(uuid.uuid4()),
    }

    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers) as r:
                if r.status == 429:
                    after = r.headers.get("Retry-After")
                    delay = float(after) if after else 2 ** attempt + random.random()
                    logging.warning(f"Rate limited on attempt {attempt + 1} ({url}), retrying in {delay:.2f}s...")
                    await asyncio.sleep(delay)
                    continue

                if r.status == 200:
                    data: dict = await r.json()
                    if data.get("success"):
                        return data
                else:
                    logging.error(f"HTTP {r.status} for {url}")
                    break

        except aiohttp.ClientError as e:
            logging.error(f"Network error ({url}): {e}")
            await asyncio.sleep(2 ** attempt + random.random())

    return None

async def get_counter(key: str):
    async with connect_db() as db:
        c = await db.execute("SELECT value FROM counters WHERE key = ?", (key,))
        row = await c.fetchone()
        return row[0] if row else 0
        
async def tour_id_gen(timestamp: int):
    date = datetime.fromtimestamp(timestamp)
    yy = date.year % 100
    m = date.strftime("%b")[0].upper()
    year = date.year
    ii = await get_counter(f"{year}_tournament") + 1

    await update_counter(key=f"{year}_tournament", amount=1, timestamp=int(time.time()))

    return f"{yy:02d}{m}{ii:02d}"

async def update_counter(key: str, amount: int = 1, timestamp: int = 0, message: str = ''):
    if timestamp == 0:
        timestamp = int(time.time())
    async with connect_db() as db:
        await db.execute('''
                         INSERT INTO counters(key, value, last_update, message)
                         VALUES              (?  , ?    , ?          , ?      )
                         ON CONFLICT(key) DO UPDATE SET
                             value = value + ?,
                             last_update = excluded.last_update,
                             message = excluded.message
                         ''', (key, amount, timestamp, message, amount)
                         )
        
        await db.commit()

async def on_start():
    for guild_id in ALLOWED_GUILD:
        guild_obj = discord.Object(id=guild_id)
        try:
            synced = await bot.tree.sync(guild=guild_obj)
            logging.info(f"Synced {len(synced)} commands to guild {guild_id}.")
        except Exception as e:
            logging.critical(f"Failed to sync to guild {guild_id}: {e}")

        guild = bot.get_guild(guild_id)
        if guild:
            async with connect_db() as db:
                async for member in guild.fetch_members(limit=None):
                    if member.id in temp_bl:
                        continue

                    async with db.execute("SELECT 1 FROM users WHERE discord_id = ?", (str(member.id),)) as c:
                        exist = await c.fetchone()
                        if exist:
                            continue

                        rsp = await api_request(url=SEARCH_URL.format(member.id))
                        users = rsp.get("data", {}).get("users", []) if rsp else []
                        username = users[0]["username"] if users else "null"

                        if not username or username == "null":
                            temp_bl.add(member.id)
                            continue
                        
                        await db.execute('''
                                         INSERT OR IGNORE INTO users(discord_id, tetrio_username, ar, country)
                                         VALUES (?, ?, ?, ?)
                                         ''', (str(member.id), username, 0, "null"))
                
                await db.commit()

async def db_update(db
                    ,username: Optional[str] = None
                    ,discord_id: Optional[str] = None
                    ,rsp: Optional[dict] = None
                    ):
    if rsp is None:
        return
    
    for table, fields in tables.items():
        if callable(fields):
            value = fields(rsp)

            if table == "tl_past" and isinstance(value, dict):
                for season, data in value.items():
                    await db.execute(f'''
                        INSERT OR REPLACE INTO "{table}" 
                        (tetrio_username, season, rd, tr, rank, best_rank, apm, pps, vs)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        username,
                        season,
                        data.get("rd", -1),
                        data.get("tr", -1),
                        data.get("rank", "z"),
                        data.get("bestrank", "null"),
                        data.get("apm", -1),
                        data.get("pps", -1),
                        data.get("vs", -1)
                    ))

            elif table == "zen":  
                await db.execute("""
                    INSERT OR REPLACE INTO zen (tetrio_username, xp)
                    VALUES (?, ?)
                """, (username, value))

        elif table == "users":
            if discord_id is None:
                continue

            values = {field: extractor(rsp) for field, extractor in fields.items()}
            columns = ", ".join(["discord_id", "tetrio_username"] + list(values.keys()))
            placeholders = ", ".join(["?"] * (2 + len(values)))
            await db.execute(
                f'INSERT OR REPLACE INTO "{table}" ({columns}) VALUES ({placeholders})',
                (str(discord_id), username, *values.values())
            )

        else: 
            values = {field: extractor(rsp) for field, extractor in fields.items()}
            columns = ", ".join(["tetrio_username"] + list(values.keys()))
            placeholders = ", ".join(["?"] * (len(values) + 1))
            await db.execute(
                f'INSERT OR REPLACE INTO "{table}" ({columns}) VALUES ({placeholders})',
                (username, *values.values())
            )

def safe_get(d, *keys, default=None):
    for k in keys:
        if not isinstance(d, dict) or k not in d or d[k] is None:
            return default
        d = d[k]
    return d

async def print_db(ctx: commands.Context):
    async with connect_db() as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT name FROM sqlite_master WHERE type='table';") as cur:
            tables = await cur.fetchall()
            tables = [t[0] for t in tables]

        for table in tables:
            msg = f"\n--- Table: {table} ---"
            async with db.execute(f'SELECT * FROM "{table}"') as cur:
                rows = await cur.fetchall()
                for row in rows:
                    msg += f"\n{dict(row)}"

                    if len(msg) > 1800:
                        await ctx.send(f"```\n{msg}\n```")
                        msg = ""

            if msg:
                await ctx.send(f"```\n{msg}\n```")

#=============== commands ===============#
@bot.hybrid_command(name="help", 
                    description="Show help for commands")
@app_commands.guilds(
    *[discord.Object(id=guild_id) for guild_id in ALLOWED_GUILD]
)
async def help_command(ctx: commands.Context, command_name: Optional[str] = None):
    if command_name is None:
        commands_list = []
        for cmd in bot.commands:
            desc = cmd.description or "No description provided."
            commands_list.append(f"`-{cmd.name}` - {desc}")

        help1 = "**Available Commands:**\n" + "\n".join(commands_list)
        help1 += "\n\nType `-help <command>` for detailed usage."
        
        await ctx.send(help1)
    else:
        cmd = bot.get_command(command_name)
        if cmd is None:
            await ctx.send(f"No command named `{command_name}` found.")
            return
        
        help2 = cmd.help or "No description provided."
        
        detailed_help = f"""
**Command:** `-{cmd.name}`
{help2}
"""
        await ctx.send(detailed_help)


@bot.hybrid_command(name='leaderboard', 
                    description='Show the top players in the server',
                    aliases=['lb'])
@app_commands.guilds(
    *[discord.Object(id=guild_id) for guild_id in ALLOWED_GUILD]
)
async def leaderboard(ctx: commands.Context):
    msg = '''
The command is currently under development.
'''
    await ctx.send(msg)


@bot.hybrid_command(name='tournament_info', 
                    description='Show info about tournament(s)',
                    aliases=['ti'],
                    help='''Show info about tournament(s)
Aliases: ti

**Usage:**
`-ti [tour_id]`

**Parameters**
- `[tour_id]`: The ID of the tournament
''')
@app_commands.guilds(
    *[discord.Object(id=guild_id) for guild_id in ALLOWED_GUILD]
)
async def tournament_info(ctx: commands.Context, tour_id: Optional[int]):
    if hasattr(ctx, "interaction") and ctx.interaction:
        await ctx.defer()
        
    async with connect_db() as db:
        if tour_id is not None:
            async with db.execute("SELECT * FROM tournament WHERE id = ?", (tour_id,)) as cursor:
                row = await cursor.fetchone()
                if not row:
                    await ctx.send("Tournament not found.")
                    return

                info = (
                    f"**Name:** {row['name']}\n"
                    f"**Date:** <t:{row['date']}:f>\n"
                    f"**Registration:** <t:{row['regis_start']}:f> - <t:{row['regis_end']}:f>\n"
                    f"**Min Rank:** {row['min_rank']}\n"
                    f"**Max Rank:** {row['max_rank']}\n"
                    f"**Info:** {row['info']}\n"
                    f"**Winner:** {row['winner']}\n"
                )
                await ctx.send(f"```\n{info}\n```")
        else:
            async with db.execute("SELECT id, name, date, regis_start, regis_end FROM tournament") as cursor:
                rows = await cursor.fetchall()
                
                if not rows:
                    await ctx.send("No tournaments found.")
                    return

                now = int(datetime.now().timestamp())
                table = f"{'ID':<4} | {'Name':<30} | {'Date':<18} | Registration open\n"
                table += "-"*82 + "\n"

                for i, row in enumerate(rows):
                    reg_open = "Yes" if row["regis_start"] <= now <= row["regis_end"] else "No"
                    date_str = f"<t:{row['date']}:F>"
                    formatstring = "{:<4} | {:<30} | {:<18} | {}\n"
                    table += formatstring.format(row['id'], row['name'], date_str, reg_open)

                table += "\nUse `-ti <ID>` to view full info."
                await ctx.send(f"```\n{table}\n```")


@bot.hybrid_command(name='set_tournament',
                    description='For mods only',
                    aliases=['st'],
                    help='''Set up a tournament (mods only).
Aliases: st


**Usage:**
`-st <tournament_name>` `<regis_start>` `<regis_end>` `<tournament_date>` `[min_rank]` `[max_rank]` `[time_format]` `[time_zone_in_utc]` `[additional_info]`

**Parameters:**
- `<tournament_name>`: Name of the tournament
- `<regis_start>`: Registration start time
- `<regis_end>`: Registration end time
- `<tournament_date>`: Date of the tournament
- `[min_rank]`: Minimum rank required (optional)
- `[max_rank]`: Maximum rank allowed (optional)
- `[time_format]`: Time format e.g. dd/mm/yy or ddmmyy (default: HH:MM dd/mm/yy)
- `[time_zone_in_utc]`: Time zone offset in UTC (default: 0)
- `[additional_info]`: Any additional info (optional)
''')
@app_commands.guilds(
    *[discord.Object(id=guild_id) for guild_id in ALLOWED_GUILD]
)
@app_commands.describe(
    tournament_name = "Name of the tournament",
    regis_start = "Registration start time",
    regis_end = "Registration end time",
    tournament_date = "Date of the tournament",
    min_rank = "Minimum rank required (optional)",
    max_rank = "Maximum rank allowed (optional)",
    time_format = "Time format e.g. dd/mm/yy or ddmmyy (default: HH:MM dd/mm/yy)",
    time_zone_in_utc = "Time zone offset in UTC e.g. +7 (default: 0)",
    additional_info = "Any additional info (optional)"
)
async def set_tournament (ctx: commands.Context, 
                          tournament_name: Optional[str],                          
                          regis_start: Optional[str],
                          regis_end: Optional[str],
                          tournament_date: Optional[str],
                          min_rank: Optional[str],
                          max_rank: Optional[str],
                          time_format: str = "HH:MM dd/mm/yy",
                          time_zone_in_utc: int = 0,                          
                          additional_info: str = "",
                          ):
    if not await mod_check(ctx):
        await ctx.send("You don't have permission to use this command.")
        return
    
    if not all([tournament_name, regis_start, regis_end, tournament_date]):
        await ctx.send("Missing required parameters. Type `-help set_tournament` for usage.")
        return
    if min_rank and min_rank not in rank_to_role:
        await ctx.send(f"Invalid minimum rank `{min_rank}`. Allowed ranks: {', '.join(rank_to_role.keys())}")
        return

    if max_rank and max_rank not in rank_to_role:
        await ctx.send(f"Invalid maximum rank `{max_rank}`. Allowed ranks: {', '.join(rank_to_role.keys())}")
        return

    if min_rank and max_rank:
        rank_order = list(rank_to_role.keys())
        if rank_order.index(min_rank) > rank_order.index(max_rank):
            await ctx.send(f"Minimum rank `{min_rank}` cannot be higher than maximum rank `{max_rank}`.")
            return
        
    if hasattr(ctx, "interaction") and ctx.interaction:
        await ctx.defer()

    data = {
        "Time zone": time_zone_in_utc, #not displaying this but "time zone display" instead (##)
        "Tournament name": tournament_name,
        "Registration start": regis_start,
        "Registration end": regis_end,
        "Tournament date": tournament_date,
        "Minimum rank": min_rank or "No minimum rank",
        "Maximum rank": max_rank or "No maximum rank",
        "Time format": time_format or "HH:MM dd/mm/yy",
        "Time zone display": (
            f"UTC+{time_zone_in_utc}" if int(time_zone_in_utc) > 0
            else f"UTC{time_zone_in_utc}" if int(time_zone_in_utc) < 0
            else "UTC"
            ),
        "Additional info": additional_info or "N/A",
    }

    keys = list(data.keys())
    
    def summary():
        return "\n".join([
            f"{i}. **{k}:** {v}"
            for i, (k, v) in enumerate(list(data.items())[1:], start=1) #here (##)
        ])

    
    async def show_summary():
        await ctx.send(
            "**Please review the tournament details:**\n\n"
            f"{summary()}\n\n"
            "**Type:**\n"
            f"`y` - to confirm and set the tournament\n"
            f"`n` - to cancel\n"
            f"`1-{len(data) - 1}` - to edit the corresponding field"
        )
    
    await show_summary()

    valid_inputs = ['y', 'n'] + [str(i) for i in range(1, len(data) - 1)]

    def check(m: discord.Message):
        return (
            m.author == ctx.author
            and m.channel == ctx.channel
            and m.content.lower().strip() in valid_inputs
            )

    while True:
        try:
            response = await bot.wait_for('message', check=check, timeout=60)
        except asyncio.TimeoutError:
            await ctx.send("Timed out. Tournament setup cancelled.")
            break
    
        ans = response.content.lower().strip()

        if ans not in valid_inputs:
            await ctx.send("Invalid input. Please try again.")
            continue
        elif ans.lower() == 'n':
            await ctx.send("Tournament setup cancelled.")
            break
        elif ans.lower() == 'y':
            try:
                regis_start_unix = unix_parser(
                    data["Registration start"], 
                    data["Time format"], 
                    int(data["Time zone"])
                )
                regis_end_unix = unix_parser(
                    data["Registration end"], 
                    data["Time format"], 
                    int(data["Time zone"])
                )
                tournament_date_unix = unix_parser(
                    data["Tournament date"], 
                    data["Time format"], 
                    int(data["Time zone"])
                )
            except ValueError as e:
                await ctx.send(f"Error parsing date/time: {e}")
                continue 

            if None in (regis_start_unix, regis_end_unix, tournament_date_unix):
                await ctx.send("Invalid date/time format detected. Please double-check your inputs.")
                continue

            tour_id = await tour_id_gen(timestamp=tournament_date_unix)
            logging.debug("tour_id: %s (%s)", tour_id, type(tour_id))
            try:
                async with connect_db() as db:
                    await db.execute('''
                                    INSERT INTO tournament 
                                     (id, name, date, regis_start, regis_end, min_rank, max_rank, info) VALUES
                                     (? , ?   , ?   , ?          , ?        , ?       , ?       , ?   )
                                     ''',
                                     (
                                        tour_id,
                                        data['Tournament name'],
                                        tournament_date_unix,
                                        regis_start_unix,
                                        regis_end_unix,
                                        data.get("Minimum rank", None),
                                        data.get("Maximum rank", None),
                                        data.get("Additional info", None),
                                     )
                                     )
                    await db.commit()
            except Exception as e:
                await ctx.send(f"Database error: {e}")
                break

            await ctx.send("Tournament has been set successfully!")
            break
        elif ans.isdigit():
            index = int(ans) 
            if 0 <= index < len(keys):
                field_name = keys[index]
                old_value = data[field_name]
                await ctx.send(f"Enter new value for **{field_name}** (current: `{old_value}`):")

                def edit_check(m: discord.Message):
                    return m.author == ctx.author and m.channel == ctx.channel

                try:
                    new_msg = await bot.wait_for("message", timeout=60.0, check=edit_check)
                except asyncio.TimeoutError:
                    await ctx.send("Timeout! Keeping old value.")
                    await show_summary()
                    continue

                new_value = new_msg.content.strip()

                if min_rank and min_rank not in rank_to_role:
                    await ctx.send(f"Invalid minimum rank `{min_rank}`. Allowed ranks: {', '.join(rank_to_role.keys())}. Keeping old value.")
                    return

                if max_rank and max_rank not in rank_to_role:
                    await ctx.send(f"Invalid maximum rank `{max_rank}`. Allowed ranks: {', '.join(rank_to_role.keys())}. Keeping old value.")
                    return

                if min_rank and max_rank:
                    rank_order = list(rank_to_role.keys())
                    if rank_order.index(min_rank) > rank_order.index(max_rank):
                        await ctx.send(f"Minimum rank `{min_rank}` cannot be higher than maximum rank `{max_rank}`. Keeping old value.")
                        return

                if "Registration" in field_name or "Date" in field_name:
                    try:
                        parsed = unix_parser(new_value, data["Time format"], int(data["Time zone"]))
                        if parsed is None:
                            await ctx.send("Invalid time format. Keeping old value.")
                            await show_summary()
                            continue
                    except Exception:
                            await ctx.send("Invalid time format. Keeping old value.")
                            await show_summary()
                            continue
                    
                data[field_name] = new_value

                await ctx.send(f"Updated **{field_name}** to `{new_value}`.")
                await show_summary()
                continue
        else:
            await ctx.send("Invalid input. Please try again.")
            continue


@bot.hybrid_command(
    name='tournament_register',
    description='Register yourself for a tournament',
    aliases=['rt'],
    help='''Register yourself for a tournament
Aliases: tr

**Usage:**
`-tr [tour_id]`

**Parameters**
- `[tour_id]`: The ID of the tournament
'''
)
@app_commands.guilds(*[discord.Object(id=guild_id) for guild_id in ALLOWED_GUILD])
async def tournament_register(ctx: commands.Context, tour_id: str):
    discord_id = str(ctx.author.id)

    async with connect_db() as db:
        async with db.execute("SELECT tetrio_username FROM users WHERE discord_id = ?", (discord_id,)) as c:
            user_row = await c.fetchone()
            if not user_row or not user_row["tetrio_username"]:
                await ctx.send("You are not linked to a Tetr.io account.")
                return
            username = user_row["tetrio_username"]

        async with db.execute("SELECT * FROM tournament WHERE id = ?", (tour_id,)) as c:
            tour_row = await c.fetchone()
            if not tour_row:
                await ctx.send(f"Tournament ID `{tour_id}` not found.")
                return

        min_rank = tour_row["min_rank"]
        max_rank = tour_row["max_rank"]

        async with db.execute("SELECT rank FROM tl WHERE tetrio_username = ?", (username,)) as c:
            rank_row = await c.fetchone()
            if not rank_row:
                await ctx.send("Your rank info is not available yet. Please wait for the next update.")
                return
            user_rank = rank_row["rank"]

        rank_order = list(rank_to_role.keys())
        user_index = rank_order.index(user_rank) if user_rank in rank_order else -1
        min_index = rank_order.index(min_rank) if min_rank in rank_order else 0
        max_index = rank_order.index(max_rank) if max_rank in rank_order else len(rank_order)-1

        if user_index < min_index or user_index > max_index:
            await ctx.send(f"Your rank `{user_rank}` does not meet the tournament requirements "
                           f"(min: `{min_rank}`, max: `{max_rank}`).")
            return

        async with db.execute(
            "SELECT 1 FROM tournament_regis WHERE tetrio_username = ? AND id = ?",
            (username, tour_id)
        ) as c:
            if await c.fetchone():
                await ctx.send("You are already registered for this tournament.")
                return

        await db.execute(
            "INSERT INTO tournament_regis (id, tetrio_username, tournament_name) VALUES (?, ?, ?)",
            (tour_id, username, tour_row["name"])
        )
        await db.commit()
        await ctx.send(f"Successfully registered for tournament `{tour_row['name']}`!")


@bot.hybrid_command(
    name="dump_db",
    description="Prints the contents of the database (all tables) to Discord"
)
@app_commands.guilds(*[discord.Object(id=guild_id) for guild_id in ALLOWED_GUILD])
async def dump_db(ctx: commands.Context):
    if not await mod_check(ctx):
        await ctx.send("You don't have permission to use this command.")
        return

    await print_db(ctx=ctx)


#=============== events ===============#
@bot.event
async def on_command_error(ctx: commands.Context, error):
    if isinstance(error, commands.CommandNotFound):
        cmd = ctx.message.content.split()[0][1:]
        msg = f"Unknown command: `{cmd}`. Type `-help` to see available commands."
        
        await ctx.send(msg)

@bot.event
async def on_ready():
    await init_db()

    global session
    if session is None:
        session = aiohttp.ClientSession()

    await on_start()
    data_update.start()

    logging.info('Logged in.\nv1.0')

@bot.event
async def on_close():
    global session
    if session and not session.closed:
        await session.close()

@bot.event
async def on_member_join(member: discord.Member):
    discord_id = str(member.id)
    rsp = await api_request(SEARCH_URL.format(discord_id))
    users = rsp.get("data", {}).get("users", []) if rsp else []
    username = users[0]["username"] if users else "null"

    if not username or username == "null":
        temp_bl.add(member.id)
        return
    
    async with connect_db() as db:
        await db.execute('''
                         INSERT OR IGNORE INTO users(discord_id, tetrio_username, ar, country)
                         VALUES (?, ?, ?, ?)
                         ''', (discord_id, username, 0, "null"))

        if username != "null":
            await db_update(db=db, username=username, discord_id=discord_id, rsp=await api_request(SUMMARIES_URL.format(username)))

        await db.commit()

#=============== tasks ===============#
@tasks.loop(minutes=5)
async def data_update(): 
    await bot.wait_until_ready()

    async with connect_db() as db:
        async with db.execute("SELECT discord_id, tetrio_username FROM users") as c:
            rows = await c.fetchall()
            for row in rows:
                username = row["tetrio_username"]
                discord_id = row["discord_id"]

                if username in ("null", None):
                    continue

                rsp = await api_request(SUMMARIES_URL.format(username))
                if rsp:
                    await db_update(db=db, username=username, discord_id=discord_id, rsp=rsp)
                    logging.info(f"Updated data for {username}")

                await db.commit()

#==============================#
load_dotenv()
bot.run(os.getenv('TOKEN',''))

