import os
import re
import json
import asyncio
from html import unescape
from datetime import datetime
from functools import partial

import aiohttp
from bs4 import BeautifulSoup as bs

URL = r"http://pri.land.moi.gov.tw/agents_query/iamqry_11a.asp?Page=%s"
D_URL = r"http://pri.land.moi.gov.tw/agents_query/iamqry_11d2.asp?rowid=%s"
RECORDED = dict()
DATA = list()


def parse_uid(txt):
    return re.findall(r"rowid=(.+)&s.*acertname=(.+)&practname", txt, re.M)


def parse_pages(txt):
    m = re.search(r"1/(\d+)</F", txt)
    return int(m.group(1)) if m else 0


def cb(uid, name, fut):
    d = fut.result()
    d['uid'] = uid
    print("[%s] Data UID %s fetched." % (datetime.now(), uid))
    d['name'] = name
    RECORDED.get('uids', set()).discard(uid)


async def fetch_details(session, url):
    async with session.get(url) as r:
        txt = await r.text('big5-hkscs')
    vals = bs(txt, 'html.parser').select('input')
    if vals:
        return dict((i['name'], i['value']) for i in vals)
    else:
        print("[%s] Failed to fetch details, retry later..." % datetime.now())
        await asyncio.sleep(10)
        return await fetch_details(session, url)


async def fetch(session, url, page=1):
    async with session.get(url) as r:
        txt = await r.text('big5-hkscs')
    pages, uids = parse_pages(txt), parse_uid(txt)

    if uids:
        RECORDED['uids'] = set(uids).union(RECORDED.get('uids', []))
        RECORDED.get('pages', set()).discard(page)
    else:
        print("[%s] failed to fetch page %s try again later...." % (datetime.now(), page))
        await asyncio.sleep(10)
        await fetch(session, url, page)

    if pages:
        RECORDED['pages'] = set(range(2, pages+1))
        print("Total pages:%s/%s" % (page, pages))
        futs = [asyncio.ensure_future(fetch(session, URL % p, p)) for p in RECORDED['pages']]
        f = await asyncio.gather(*futs)

    items = []
    for uid, name in uids:
        f = asyncio.ensure_future(fetch_details(session, D_URL % uid))
        f.add_done_callback(partial(cb, uid, unescape(name)))
        items.append(f)

    await asyncio.gather(*items)


def main():
    if not os.path.exists("_record.json"):
        with open("_record.json", 'w') as f:
            json.dump([], f)
    if not os.path.exists("data.json"):
        with open("data.json", 'w') as f:
            json.dump([], f)

    with open("_record.json") as f:
        RECORDED.update(json.load(f))
    try:
        loop = asyncio.get_event_loop()
        conn = aiohttp.TCPConnector(limit=15)
        session = aiohttp.ClientSession(connector=conn)
        urls = RECORDED.get('pages', [1])
        tasks = asyncio.gather(*[fetch(session, URL % i) for i in urls])
        loop.run_until_complete(tasks)
    finally:
        session.close()
        loop.close()
        with open("data.json", "r+", encoding="utf8") as f:
            prev = json.load(f)
            prev.extend(DATA)
            f.seek(0)
            json.dump(prev, f)


if __name__ == '__main__':
    main()
