import asyncio
import time
import concurrent
import logging
from typing import List

import aiohttp

from item import PageItem, ResponseItem
from pipelines import PipelineToDB, PipelineMainContent
from utils.url_pool import UrlPool
from fake_useragent import UserAgent
from urllib.parse import urlparse
from trafilatura.downloads import fetch_url
from trafilatura.spider import focused_crawler
from pipelines import PipelineBase

import courlan


class Crawler:
    def __init__(self,
                 allow_domain: List[str],
                 pipelines: List,
                 max_thread: int = 10,
                 max_concurrent_request=100):
        self.pipelines = pipelines
        self.logger = logging.getLogger('[Spider]')

        self.url_que = []
        # self.url_que = UrlPool(max_depth)
        self.max_thread = max_thread
        self.max_concurrent_request = max_concurrent_request

        self.allow_domain = allow_domain
        self.cnt_request = 0
        self.rand_ua = UserAgent(os=['windows'], browsers=['chrome', "edge"],)

        for pipeline in self.pipelines:
            pipeline.open_spider()

    def start_batch(self, urls: List[str]):
        """start spider

        Arguments:
            start_url {List[str]} -- urls
        """
        self.start_time = time.time()
        for url in urls:
            self.url_que.append(url)

        crawled_urls, crawled_pages = self.request_handler()
        # self.logger.warning(f'request_cnt: {self.cnt_request}')

        for pipeline in self.pipelines:
            pipeline.close()

        return crawled_urls, crawled_pages

    def request_handler(self):
        loop = asyncio.get_event_loop()

        # while self.url_que.pq_size() != 0:
        crawled_urls = []
        crawled_pages = []
        while len(self.url_que) != 0:
            results = loop.run_until_complete(self.fetch_all())
            with concurrent.futures.ProcessPoolExecutor(
                    self.max_thread) as executor:
                futures = []
                for resp_item in results:
                    futures.append(
                        executor.submit(Crawler.pipelines_handler, resp_item,
                                        self.pipelines))
                for future in concurrent.futures.as_completed(futures):
                    try:
                        page = future.result()
                        if not page:
                            continue
                        if isinstance(page.a, list):
                            crawled_urls += [href for href in page.a if self.allow_url(href)]
                        page = dict(page)
                        # page.pop('a')
                        if page['title']:
                            crawled_pages.append(page)
                    except Exception as e:
                        self.logger.warning(f'[Pipelines]: {e}')
        
        return list(set(crawled_urls)), crawled_pages
    
    # def fetch_trafilatura(self):
    #     urls = self.url_que
    #     self.url_que.clear()
        

    async def fetch_all(self):
        # self.logger.debug(f'url_pool_size: {len(self.url_que)}')


        tasks = []
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True,) as session:
            urls = self.url_que[:self.max_concurrent_request]
            self.url_que = self.url_que[self.max_concurrent_request:]
            self.cnt_request += len(urls)

            for url in urls:
                task = asyncio.create_task(self.__fetch(url, session))
                tasks.append(task)
            results = await asyncio.gather(*tasks)
        
        return results

    def allow_url(self, href: str):
        if not self.allow_domain:
            return True
        if not href.startswith('https:'):
            return False
        # first_href = urlparse(href).netloc
        is_valid, url = courlan.validate_url(href)
        if not is_valid:
            return False
        first_href = url.netloc
        href = courlan.scrub_url(href)
        href = courlan.normalize_url(href, strict=True)
        href = courlan.clean_url(href, )
        
        if href[9:].strip('/').count('/') > 4:
            return False
    
        fl = False
        for domain in self.allow_domain:
            if domain in first_href:
                fl = True
                break
        
        if not fl:
            return False

        href = href.lower()
        
        for file_type in [
                '.mpg', '.wmv', '.mov', '.jpg', '.avi', '.rmvb', '.jpeg', '.png', '.js', '.css', '.gif', '.zip', '.ods', '.pdf', 'docx', '.doc', 'xls', 'xlsx', '.mp3', '.wav', '.js', '.odt', '.svg', '.ppt', '.pptx'
        ]:
            # if href.lower().endswith(file_type):
            if file_type in href:
                return False
        
        BANNED_TERMS = ['/search', '/upload', '/contact', '/download', 'file/']
        for term in BANNED_TERMS:
            if term in href:
                return False
        
        return True

    @staticmethod
    def pipelines_handler(resp_item: ResponseItem, pipelines: List[PipelineBase]):
        if resp_item.drop:
            return None
        
        for pipeline in pipelines:
            resp_item = pipeline.in_resp_queue(resp_item)

        if resp_item.drop:
            return None

        for pipeline in pipelines:
            tmp = pipeline.convert_resp_to_page(resp_item)
            if tmp:
                break

        page_item = tmp
        for pipeline in pipelines:
            page_item = pipeline.in_page_queue(page_item)

        return page_item

    async def __fetch(self, url: str, session: aiohttp.ClientSession):
        """send aio request

        Arguments:
            url {str} -- a url

        Returns:
            item.ResponseItem()
        """

        item = ResponseItem()
        item.url = url
        # resp = fetch_url(url, no_ssl=True, decode=False)
        # try:
        #     item.html = resp.data
        #     item.resp_code = resp.status
        # except AttributeError as e:
        #     self.logger.warning(f"url: {url} Error:{e}")
        #     item.drop = True
        # status = None
        for i in range(2):
            headers = {
                'User-Agent': self.rand_ua.random
            }
            try:
                async with session.get(url, headers=headers,
                                    verify_ssl=False,
                                    allow_redirects=False) as resp:
                    item.html = await resp.read()
                    item.resp_code = resp.status
                    item.drop = False if item.resp_code in [200, 201] else True
                break
            except Exception as e:
                item.drop = True
                # status = True
                # self.logger.warning(f"url: {url} with ua {headers}")
        if item.drop:
            self.logger.info(f'{url} error.')
        # if status is not None:
        #     self.logger.warning(f"{url} are back!!")
        return item


if __name__ == '__main__':
    from argparse import ArgumentParser
    import requests
    from urllib.parse import urljoin
    import json

    parser = ArgumentParser()
    parser.add_argument('--server', type=str, default='http://localhost:8087')
    parser.add_argument('--cli_id', type=str, default='cli1')
    parser.add_argument('--num_threads', type=int, default=10)
    parser.add_argument('--max_concurrent_req', type=int, default=100)
    
    args = parser.parse_args()

    resp = requests.get(urljoin(args.server,
                                f'/register/{args.cli_id}')).json()
    print(resp)
    crawler = Crawler(pipelines=[PipelineMainContent()],
                      allow_domain=[
                                    # 'www.taiwannews.com.tw', 
                                    # 'english.ftvnews.com.tw', 
                                    # 'icrt.com.tw',
                                    # 'eslite.com',
                                    'gov.tw',
                                    'gov.taipei',
                                    # 'eslitecorp.com'
                                    # 'features.ltn.com.tw/english',
                                    # 'www.taipeitimes.com',
                                    # 'gnn.gamer.com.tw',
                                    # 'acg.gamer.com.tw'
                                    ],
                      max_thread=args.num_threads,
                      max_concurrent_request=args.max_concurrent_req
                    )
    while True:
        urls = requests.get(urljoin(args.server, f'fetchurls/{args.cli_id}'),
                            params={
                                'num': args.num_threads * args.max_concurrent_req
                            }).json()['urls']
        if len(urls) == 0:
            crawler.logger.warning('no url in queue.')
            break
        crawled_urls, crawled_pages = crawler.start_batch(urls)
        crawler.logger.warning(f"url in queue: {requests.get(urljoin(args.server, f'/url-in-queue-num')).json()['nums']}" " "
                               f"crawled request: {requests.get(urljoin(args.server, f'/crawled-url-num')).json()['nums']}")
        requests.post(urljoin(args.server, f'/save/'),
                        json={
                            'crawled_urls': crawled_urls,
                            "pages": crawled_pages
                        })

