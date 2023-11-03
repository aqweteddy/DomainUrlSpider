from typing import List

from utils.function import get_domain


class PageItem:
    body: str = ''
    url: str = ''
    domain: str = ''
    title: str = ''
    depth_from_root: int = ''
    a: List[str] = []
    ip: str = ''

    def __iter__(self):
        yield 'body', str(self.body) if self.body else ''
        yield 'url', str(self.url) if self.url else ''
        yield 'domain', str(self.domain) if self.domain else ''
        yield 'title', str(self.title) if self.title else ''


class ResponseItem:
    resp_code: int = None
    html: str = None
    url: str = None
    depth_from_root: int = 0
    drop: bool = False

    def __repr__(self):
        return f"""
                resp_code: {self.resp_code}
                url: {self.url}
                """


if __name__ == '__main__':
    print(get_domain('https://google.com/'))
