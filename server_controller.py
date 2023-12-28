from typing import List, Dict
import heapq
from utils.cms import CountMinSketch
from utils.function import get_domain
import jsonlines, os
from rbloom import Bloom


class UrlItem:
    def __init__(self, url, weight) -> None:
        self.url = url
        self.weight = weight

    def __lt__(self, other):
        return self.weight < other.weight


class ClientQueue:
    def __init__(self, cli_id: str, heap_max_size: int=100000) -> None:
        self.cli_id = cli_id
        self.heap_max_size = heap_max_size
        self.cms = CountMinSketch()
        self.url_queue = []
        self.cold_queue = []

    def count_domain(self, domain: str):
        return self.cms.query(domain)

    def add(self, url: str, domain: str, weight: int):
        self.cms.add(domain)

        # if len(self.url_queue) > self.heap_max_size:
        self.cold_queue.append(UrlItem(url, weight))
        # else:
        #     heapq.heappush(self.url_queue, UrlItem(url, weight))

    def get_urls(self, nums: int):
        urls = []
        
        if len(self.url_queue) < 1000:
            while len(self.url_queue) < self.heap_max_size and len(self.cold_queue) > 0:
                heapq.heappush(self.url_queue, self.cold_queue[0])
                self.cold_queue.pop(0)
        
        for _ in range(min(nums, len(self.url_queue))):
            url_item = heapq.heappop(self.url_queue)
            urls.append(url_item.url)
        return urls

    def __len__(self):
        return len(self.url_queue) + len(self.cold_queue)

    def __repr__(self) -> str:
        result = f'numbers of url: {len(self.url_queue)}\n'
        # for u in self.url_queue:
        #     result += f'{u.url}\n'
        return result


class ServerController:
    def __init__(self, init_urls: List[str], db_url: str) -> None:
        self.db_url: str = db_url
        self.clients_que: Dict[str, ClientQueue] = {}
        self.init_urls = init_urls
        #self.bf = BloomFilter(max_elements=10**8, error_rate=0.01)
        self.bf = Bloom(100_000_000, 0.01)
        self.tmp_result = []
        self.crawled_num = 0
        self.num_files = 1

    def add_client(self, cli_id: str):
        if cli_id not in self.clients_que.keys() or len(self.clients_que[cli_id]) == 0:
            self.clients_que[cli_id] = ClientQueue(cli_id)
        else:
            print(f'{cli_id} already in use.')
            return
        
        if len(self.clients_que) == 1:
            for url in self.init_urls:
                self.clients_que[cli_id].add(url,
                                            get_domain(url), 0)
        else:
            # add some url to new client
            qued_urls = list(self.clients_que.values())[0].get_urls(100)
            for url in qued_urls:
                self.clients_que[cli_id].add(
                    url, get_domain(url), 0
                )
            

    def add_urls(self, urls: List[str]):
        for url in urls:
            if url in self.bf:
                continue
            self.bf.add(url)
            domain = get_domain(url)
            min_val, min_cli = float('inf'), ''
            for cli_id, cli in self.clients_que.items():
                cnt = cli.count_domain(domain)
                if min_val > cnt:
                    min_val, min_cli = cnt, cli_id

            self.clients_que[min_cli].add(url, domain, min_val)
  
    def get_urls(self, cli_id: str, nums: int):
        try:
            return self.clients_que[cli_id].get_urls(nums)
        except KeyError:
            raise KeyError(f'{cli_id} not exist')

    def push_to_db(self, content: List[Dict]):
        self.crawled_num += len(content)
        self.tmp_result.extend(content)
        
        if len(self.tmp_result) > 10000:
            jsonl_path = f'result/chunk_{self.num_files}.jsonl'
            if not os.path.isfile(jsonl_path):
                open(jsonl_path, 'w').close()
            
            with jsonlines.open(f'result/chunk_{self.num_files}.jsonl', 'a') as f:
                for t in self.tmp_result:
                    f.write(t)
            if self.crawled_num > self.num_files * 50_0000:
                self.num_files += 1
            self.tmp_result = []


if __name__ == '__main__':
    controller = ServerController(init_url='https://dcard.tw', db_url="df")
    controller.add_client('test_cli')
    controller.add_client('test_cli1')
    import time
    from tqdm import tqdm

    start = time.time()
    for i in tqdm(range(10**6)):
        controller.add_urls([f"https://zhuanlan.zhihu.com/p/74219095{i}"])
    for i in range(100):
        controller.add_urls([f"https://zhuanlan.zhihu.com/p/74219095{i}"])
    print(time.time() - start)
    print(controller.clients_que)
    print(controller.get_urls("test_cli", 10))
