from arkitekt import easy
import asyncio

async def main():
    x = easy("nnn", url="lok:8000")

    async with x:
        print(await x.fakts.aload(force_refresh=True))


asyncio.run(main())