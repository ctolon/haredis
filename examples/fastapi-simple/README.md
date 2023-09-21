# haredis - FastAPI simple example

This example shows how to use haredis with FastAPI.

## Run

```bash
pip3 install -r requirements.txt
bash run-redis.sh
python3 main.py
```

Now you can open `http://0.0.0.0:8000/docs` in your browser and try out the API.

### Example

For Testing the Lock - Release Algorithm on API, you can use the following command:

```bash
curl -X 'GET' \
  'http://0.0.0.0:8000/sync_decorated_style?param1=23&param2=23' \
  -H 'accept: application/json'
```

You can send multiple requests at the same time and see that the lock is working.