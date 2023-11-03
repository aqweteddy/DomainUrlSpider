from utils.function import get_domain


class UrlItem:
    def __init__(self, url: str, depth_from_root: int = 0):
        self.depth = depth_from_root
        self.url = url
        self.used = False

    def __hash__(self):
        return hash(self.url)

    def __eq__(self, b):
        return self.url == b.url

    def get(self):
        self.used = True
        return (self.url, self.depth)


class UrlPool:
    def __init__(self, max_depth: int = 2):
        self.url_db = dict()  # {domain: set((url, depth), ....)}
        self.prev_db = dict()
        self.cnt_url = 0
        self.max_depth = max_depth

    def add(self, url: str, depth: int = 0):
        """add url to pool

        Arguments:
            url {str} -- url

        Keyword Arguments:
            depth {int} -- depth from root (default: {0})
        """
        if depth > self.max_depth:
            return

        domain = get_domain(url)
        try:
            self.url_db[domain].add(UrlItem(url, depth))
            self.cnt_url += 1
        except KeyError:
            self.url_db[domain] = set([UrlItem(url, depth)])
            self.cnt_url += 1
        except Exception:
            pass

    def __len__(self):
        return self.cnt_url

    def get_batch(self, k: int):
        """[get k url pairs]

        Arguments:
            k {int} -- numbers of url
        """
        result = []
        for (domain, url_set) in self.url_db.items():
            for url_pair in url_set:
                if not url_pair.used:
                    result.append(url_pair.get())
                    k -= 1
                    if k == 0:
                        break
            if k == 0:
                break
        self.cnt_url = self.cnt_url - k if self.cnt_url > k else 0
        return result


if __name__ == '__main__':
    pool = UrlPool()
    for i in range(100000):
        pool.add(f'https://www.google.com{i}', 2)

    print(len(pool.get_batch(99999)))
    print(len(pool))
