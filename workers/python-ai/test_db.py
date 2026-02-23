import asyncio
import os
import asyncpg

async def main():
    conn = await asyncpg.connect(os.environ.get("DATABASE_URL", "postgresql://trainscanner:trainscanner@localhost:5432/trainscanner"))
    
    rows = await conn.fetch("SELECT raw_payload FROM netex_stops_staging WHERE raw_payload ? 'name:en' LIMIT 1")
    if not rows:
        rows = await conn.fetch("SELECT raw_payload FROM netex_stops_staging LIMIT 1")
        
    for r in rows:
        print(r['raw_payload'])
        
    await conn.close()

if __name__ == '__main__':
    asyncio.run(main())
