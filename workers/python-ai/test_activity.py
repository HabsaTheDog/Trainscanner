import asyncio
import os
os.environ["DATABASE_URL"] = "postgresql://trainscanner:trainscanner@localhost:5432/trainscanner"

from activities import calculate_merge_score

async def main():
    print("Testing calculate_merge_score...")
    res = await calculate_merge_score("dummy_station_id_not_exist")
    print("Result:", res)

if __name__ == "__main__":
    asyncio.run(main())
