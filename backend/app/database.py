import asyncpg

pool = None

async def init_pool(host, port, user, password, database):
    global pool
    pool = await asyncpg.create_pool(
        host=host, port=port,
        user=user, password=password,
        database=database
    )

async def close_pool():
    await pool.close()

def get_pool():
    return pool