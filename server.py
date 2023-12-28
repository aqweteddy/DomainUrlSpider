from typing import Optional, List, Dict
import time
from fastapi import FastAPI
from pydantic import BaseModel
from server_controller import ServerController
from urllib.parse import urlparse

app = FastAPI()

with open('url.txt', 'r') as f:
    SEEDS_URLS = [line.strip() for line in f.readlines()]
print(SEEDS_URLS)

controller = ServerController(SEEDS_URLS, "")
start_time = time.time()

class CrawledPages(BaseModel):
    pages: List[Dict[str, str]] = None
    crawled_urls: List[str] = None


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/register/{cli_id}")  # client register
def register_client(cli_id: str):
    try:
        controller.add_client(cli_id)
        return {'status': "ok"}
    except KeyError as e:
        return {"status": "error", "message": str(e)}

@app.get('/get-allowed-domain')
def get_allowed_domain():
    domains = []
    for url in SEEDS_URLS:
        domains.append(urlparse(url).netloc.replace('www.', ''))
    return {"status": "ok", "domains": domains}

    
@app.get("/fetchurls/{cli_id}")  # client fetch url
def fetch_url(cli_id: str, num: int):
    try:
        urls = controller.get_urls(cli_id, num)
        return {"status": "ok", "urls": urls, 'nums': len(urls)}  # List[str]
    except KeyError as e:
        return {"status": "error", "message": str(e)}


@app.get('/client-num')  # numbers of clients
def client_num():
    return {"status": "ok", "nums": len(controller.clients_que)}


@app.get('/url-in-queue-num')  # numbers of urls in queue
def url_in_queue():
    num = sum(map(lambda x: len(x), controller.clients_que.values()))
    return {
            "status": "ok",
            "nums": num
            }

@app.get('/crawled-url-num')  # numbers of urls in queue
def crawled_url_num():
    return {
            "status": "ok",
            "nums": controller.crawled_num,
            "times": time.time() - start_time
            }

@app.post('/save/')  # save crawled webpage
async def save(pages: CrawledPages):
    pages = pages.dict()
# def save(cli_id: str, crawled_urls: List[str], pages: Dict[str, str]):
    controller.push_to_db(pages['pages'])
    controller.add_urls(pages['crawled_urls'])
    # print(pages.crawled_urls)
    return {'status': 'ok'}

@app.get('/save-que')
async def save_queue():
    tot_que = []
    for v in controller.clients_que.values():
        tot_que += [a.url for a in v.url_queue] + [a.url for a in v.cold_queue]

    with open('queue.cache', 'w') as f:
        f.write('\n'.join(tot_que))
