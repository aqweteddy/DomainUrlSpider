# Server Client Search Engine Crawler

NTU HPC 2022 Final Project

* server: 負責分配URL、接收資料
* client: 從server 拿到 url 後爬取網站，回傳蒐集到的網站內容和URL 給server

## Installation

```bash
git clone https://github.com/aqweteddy/ServerClientSearchEngineCrawler.git
conda activate <env name>
pip install --no-binary lxml html5-parser
pip install -r requirements.txt
```

## Usage

* 先開啟 server
```bash
uvicorn server:app --host 0.0.0.0 --port 8087 
```

* 再開啟 client
可在其他電腦上單獨執行client
```bash
python client.py --cli_id cli1
```

## 作法

### server

* bloom filter 檢查 client 回傳的 url 是否有爬取過
* 維護一個 ClientQueue，有K個 client manager，每個 client manager 維護一個 Count Min Sketch，當分配 url給各個 client時，根據每個 client domain 爬取次數來分配 url，也就是說，若 client 1 存取 google.com 較多次，就會優先分配 google.com 的 url 給其他 client

### client

* aiohttp 實踐異步爬蟲
* main content analyze

## Bash

* Clean javascript render pages.
```bash
cat *.jsonl | egrep -iv "javascript" > tmp.jsonl
```

## TODO

[] database
[] mutex lock