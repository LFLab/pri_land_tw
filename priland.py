import re
import json
import asyncio
from html import unescape
from functools import partial

import aiohttp
from bs4 import BeautifulSoup as bs

URL = r"http://pri.land.moi.gov.tw/agents_query/iamqry_11a.asp?Page=%s"
D_URL = r"http://pri.land.moi.gov.tw/agents_query/iamqry_11d2.asp?rowid=%s"


def parse_uid(txt):
    return re.findall(r"rowid=(.+)&s.*acertname=(.+)&practname", txt, re.M)


def parse_pages(txt):
    m = re.search(r"1/(\d+)</F", txt)
    return int(m.group(1)) if m else 0


def cb(uid, name, fut):
    d = fut.result()
    d['uid'] = uid
    return d


async def fetch_details(session, url):
    async with session.get(url) as r:
        txt = await r.text('big5-hkscs')
    return {(i['name'], i['value']) for i in bs(txt, 'html.parser').select('input')}


async def fetch(session, url, page=1):
    async with session.get(url) as r:
        txt = await r.text()
    print(txt, url, page)
    return []
    pages, uids = parse_pages(txt), parse_uid(txt)
    futs, data = [], []
    if pages:
        futs = [asyncio.ensure_future(fetch(session, URL % p, p)) for p in range(2, pages+1)]
        print("Total pages:", pages, len(futs))
        data = await asyncio.gather(*futs)
        data = [j for i in data for j in i]

    items = []
    for uid, name in uids:
        f = asyncio.ensure_future(fetch_details(session, D_URL % uid))
        f.add_done_callback(partial(cb, uid, unescape(name)))
        items.append(f)
    data.extend(await asyncio.gather(*items))

    return data


def main():
    loop = asyncio.get_event_loop()
    conn = aiohttp.TCPConnector(limit=200)
    session = aiohttp.ClientSession(connector=conn)
    dataset = loop.run_until_complete(fetch(session, URL % 1))
    loop.close()
    session.close()
    with open("data.json", "w", encoding="utf8") as f:
        json.dump(dataset, f)


if __name__ == '__main__':
    main()
