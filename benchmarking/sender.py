import asyncio
import httpx

async def send_request():
    async with httpx.AsyncClient() as client:
        url = 'http://0.0.0.0:5703/haredis/async-event-decorated'
        headers = {'accept': 'application/json', 'Content-Type': 'application/json'}
        data = {"param1": 2, "param2": 4}
        
        try:
            response = await client.post(url, headers=headers, json=data, timeout=60.0)  # Örnek olarak 5 saniye
            return response
        except httpx.ReadTimeout:
            print("HTTP Timeout!")
            # Zaman aşımı hatası aldığınızda burada işleyebilirsiniz
            return None

async def main():
    num_requests = 5
    
    response_status_codes = []
    
    for _ in range(num_requests):
        await asyncio.sleep(0.15)  # Her istek öncesi 1 saniye bekleme
        response = await send_request()
        print("Request: ", _)
        if response is not None:
            response_status_codes.append(response)
        else:
            print("Timeout!")
    
    return response_status_codes

if __name__ == "__main__":
    result = asyncio.run(main())
    print("Response Status Codes:", result)