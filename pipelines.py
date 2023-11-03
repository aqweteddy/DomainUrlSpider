import asyncio
import json
import logging
# from utils.parser import HtmlParser
from typing import List
from urllib.parse import urlencode, urljoin

import aiohttp, re

from item import PageItem, ResponseItem
from utils.function import get_domain
import trafilatura


class PipelineBase:
    def __init__(self):
        """init"""
        self.logger = logging.getLogger('[Pipeline]')

    def open_spider(self):
        """
        run when open spider
        """
        pass

    def in_resp_queue(self, data):
        """call from resp_queue

        Arguments:
            resp {ResponseItem}
        return:
            {ResponseItem}
        """
        return data

    def convert_resp_to_page(self, resp):
        """convert resp_item to page_item

        Arguments:
            resp {RespItem} -- responseItem
        """
        return None

    def in_page_queue(self, data):
        """call from resp_queue

        Arguments:
            data {PageItem}
        return:
            {pageItem}
        """
        return data

    def save(self, data):
        """save data to db

        Arguments:
            data {PageItem} -- data
        """

    def close(self):
        pass


class PipelineMainContent(PipelineBase):
    def __init__(self):
        super().__init__()
        self.pat_remove_url = re.compile(r'\[.*\]\([^)]+\)', re.MULTILINE)
        

    def convert_resp_to_page(self, resp: ResponseItem):
        item = PageItem()

        item.url = resp.url
        item.depth_from_root = resp.depth_from_root
        
        html = trafilatura.load_html(
            resp.html
        )
        
        item.a = []
        try:
            for h in html.iterlinks():
                url = h[2]
                if url.startswith('http'):
                    item.a.append(url)
                elif url.startswith('/'):
                    item.a.append(urljoin(item.url, url))
        except AttributeError:
            pass
        try:
            parsed_page = trafilatura.bare_extraction(
                html, deduplicate=True, 
                include_comments=False,
                no_fallback=True, 
                favor_precision=True,
                favor_recall=False,
            )
        except:
            return None
        
        if parsed_page is not None:
            item.title = parsed_page.get('title', '')
            item.domain = get_domain(item.url)
            item.body = parsed_page.get('text', '')
            return item
        return None


class PipelineToDB(PipelineBase):
    def __init__(self, host: str = 'http://nudb1.ddns.net:5804/nudb', db_name: str = 'search_engine'):
        super().__init__()
        self.host = host
        self.db_name = db_name
        self.pool = []
        self.pushed_cnt = 0
        self.now_chunk = 0

    def save(self, data):
        self.pool.append(dict(data))

        if len(self.pool) > 1000:
            self.logger.warning('batch push to DB')
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.batch_rput())
            self.pushed_cnt += len(self.pool)
            self.pool.clear()
            self.logger.warning(f'Numbers of finish Items: {self.pushed_cnt}')

    async def batch_rput(self):
        async with aiohttp.ClientSession() as session:
            batch_data = []
            tasks = []
            for data in self.pool:
                batch_data.append(data)
                if len(batch_data) == 20:
                    task = asyncio.create_task(self.rput(session, batch_data))
                    tasks.append(task)
                    batch_data = []
            task = asyncio.create_task(self.rput(session, batch_data))
            tasks.append(task)
            result = await asyncio.gather(*tasks)

    async def rput(self, session, data: list, format: str = 'json'):
        if format == 'json':
            data = json.dumps(data, ensure_ascii=False)
        url = f'{self.host}/rput?{urlencode({"db": self.db_name, "format": format, "record": data})}'
        try:
            async with session.get(url, verify_ssl=False) as resp:
                js = await resp.json()
                resp_code = resp.status
            return resp_code
        except Exception:
            pass
