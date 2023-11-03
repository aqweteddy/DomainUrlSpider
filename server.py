from typing import Optional, List, Dict
import time
from fastapi import FastAPI
from pydantic import BaseModel
from server_controller import ServerController

app = FastAPI()
SEEDS_URLS = [
    'https://www.ey.gov.tw/Page/61E6295406231218/',
    'https://www.president.gov.tw/Page/106',
    'https://www.gov.taipei/default.aspx',
    'https://www.dorts.gov.taipei',
    'https://www.gov.tw/',
    'https://www.k12ea.gov.tw/',
    'https://www.moi.gov.tw/',
    'https://www.ndc.gov.tw/',
    'https://www.hl.gov.tw/',
    'https://english.gov.taipei/',
    'https://tuic.gov.taipei/zh'
]

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