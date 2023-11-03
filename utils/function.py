from urllib.parse import urlparse


def get_domain(url: str):
    url = urlparse(url).netloc
    return url
