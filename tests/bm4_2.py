import asyncio
from time import perf_counter

import aiohttp
from constants import Endpoints, Datas, PORTS, HOST

REQ_COUNT = 500

async def fetch(s: aiohttp.ClientSession, url):
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    
    async with s.post('http://{HOST}:{PORT}/haredis/{ENDPOINT}'.format(
        HOST=HOST,
        PORT=PORTS.API_PORT_2,
        ENDPOINT=Endpoints.async_event_decorated
        ),
        headers=headers, json=Datas.CLEAN_SAMPLE_1) as r:
        
        if r.status != 200:
            r.raise_for_status()
            print("Failed: ", r.status)
        # print(r.status)
        return await r.text()

async def fetch_all(s, urls, result_queue):
    tasks = []
    for url in urls:
        task = asyncio.create_task(fetch(s, url))
        tasks.append(task)
        await asyncio.sleep(0.15)  # Her işlem arasında 0.15 saniye bekleme
        
    for task in asyncio.as_completed(tasks):
        result = await task
        await result_queue.put(result)

async def main():
    urls = range(1, REQ_COUNT)
    
    result_queue = asyncio.Queue()
    
    async with aiohttp.ClientSession() as session:
        while urls:
            batch = urls[:50]  # Her seferinde 50 istek atın
            urls = urls[50:]
            
            await fetch_all(session, batch, result_queue)
            
            # Her 3 saniyede bir dinlenin
            await asyncio.sleep(3)
    
    # Sonuçları görüntüleyin veya işleyin
    while not result_queue.empty():
        result = await result_queue.get()
        print(result)

if __name__ == '__main__':
    start = perf_counter()
    asyncio.run(main())
    stop = perf_counter()
