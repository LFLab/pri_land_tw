import os
import re
import sys
import json
import asyncio
from html import unescape
from random import shuffle
from datetime import datetime
from functools import partial

import aiohttp
from bs4 import BeautifulSoup as bs

URL = r"http://pri.land.moi.gov.tw/agents_query/iamqry_11a.asp?Page=%s"
D_URL = r"http://pri.land.moi.gov.tw/agents_query/iamqry_11d2.asp?rowid=%s"
RECORDED = dict()
DATA = [[]]
PROXY = asyncio.Queue()


def parse_uid(txt):
    return re.findall(r"rowid=(.+)&s.*acertname=(.+)&practname", txt, re.M)


def parse_pages(txt):
    m = re.search(r">1/(\d+)</F", txt)
    return int(m.group(1)) if m else 0


def cb(uid, name, fut):
    d = fut.result()
    if d:
        print("[%s] Data UID %s fetched." % (datetime.now(), uid))
        d['uid'] = uid
        d['name'] = name
        DATA.append(d)
    RECORDED.get('uids', set()).discard(uid)


async def put_delay(queue, item, delay=1):
    await asyncio.sleep(delay)
    await queue.put(item)


async def fetch_details(session, url):
    uid = url.replace(D_URL % "", "")
    if uid in set(i['uid'] for i in DATA[1:]):
        print("[%s] Skip fetch_details due to UID %s exists." % (datetime.now(), uid))
        return

    try:
        p = await PROXY.get()
        async with session.get(url, proxy=p) as r:
            txt = str(await r.read(), "big5hkscs", errors="replace")
            # ref: http://bit.ly/2wj8RFV
    except (LookupError, TypeError):
        RECORDED['decode_err'].append(url)
        print("[%s] Force to decode in UID %s" % (datetime.now(), uid))
        txt = str(txt, error="replace")
        vals = bs(txt, 'html.parser').select('input')
    except BaseException as e:
        print("[%s] Exception %r occured, try another proxy." % (datetime.now(), e), file=sys.stderr)
        asyncio.ensure_future(put_delay(PROXY, p))
        return await fetch_details(session, url)
    else:
        vals = bs(txt, 'html.parser').select('input')

    if vals:
        await PROXY.put(p)
        return dict((i['name'], i['value']) for i in vals)
    else:
        print("[%s] Failed to fetch details, try another proxy." % datetime.now())
        asyncio.ensure_future(put_delay(PROXY, p, 601))
        return await fetch_details(session, url)


async def fetch(session, url, page=1):
    try:
        p = await PROXY.get()
        async with session.get(url, proxy=p) as r:
            txt = str(await r.read(), "big5hkscs", errors="replace")
            # ref: http://bit.ly/2wj8RFV
    except (LookupError, TypeError):
        RECORDED['decode_err'].append(url)
        print("[%s] Force to decode in page %s" % (datetime.now(), page))
        txt = str(txt, error="replace")
        pages, uids = parse_pages(txt), parse_uid(txt)
    except BaseException as e:
        print("[%s] Exception %r occured, try another proxy." % (datetime.now(), e), file=sys.stderr)
        asyncio.ensure_future(put_delay(PROXY, p))
        return await fetch(session, url, page)
    else:
        pages, uids = parse_pages(txt), parse_uid(txt)

    if uids:
        RECORDED['uids'] = set(i for i, _ in uids).union(RECORDED.get('uids', []))
        RECORDED['pages'] = set(RECORDED.get('pages', [])) - {page}
        # to skip exist uids.
        uids = [(i, n) for i, n in uids if i not in DATA[0]]
        DATA[0].extend(i for i, _ in uids)
    else:
        print("[%s] failed to fetch page %s, try another proxy." % (datetime.now(), page))
        asyncio.ensure_future(put_delay(PROXY, p, 601))
        return await fetch(session, url, page)

    items = []
    if pages:
        RECORDED['pages'] = set(range(2, pages+1))
        print("Total pages:%s/%s" % (page, pages))
        items = [asyncio.ensure_future(fetch(session, URL % p, p)) for p in RECORDED['pages']]

    for uid, name in uids:
        f = asyncio.ensure_future(fetch_details(session, D_URL % uid))
        f.add_done_callback(partial(cb, uid, unescape(name)))
        items.append(f)

    await PROXY.put(p)
    await asyncio.gather(*items, return_exceptions=True)


def main():
    if not os.path.exists("_record.json"):
        with open("_record.json", 'w') as f:
            json.dump([], f)
    if not os.path.exists("data.json"):
        with open("data.json", 'w') as f:
            json.dump([], f)

    with open("_record.json") as f:
        d = f.read()
    RECORDED.update(json.loads(d or "{}"))
    RECORDED['decode_err'] = RECORDED.get('decode_err', [])

    with open("proxy.json") as f:
        proxies = json.load(f)
    PROXY.put_nowait(None)
    shuffle(proxies)
    [PROXY.put_nowait(i) for i in proxies]

    with open("data.json") as f:
        data = json.load(f)
    DATA[0].extend(data[0])
    DATA.extend(data[1:])

    try:
        loop = asyncio.get_event_loop()
        conn = aiohttp.TCPConnector(limit=200)
        session = aiohttp.ClientSession(connector=conn)
        urls = RECORDED.get('pages', [1])
        tasks = asyncio.gather(*[fetch(session, URL % i, i) for i in urls], return_exceptions=True)
        f = loop.run_until_complete(tasks)
    except asyncio.TimeoutError:
        f.cancel()
    else:
        print(f)
    finally:
        print(RECORDED)
        with open("_record.json", "w", encoding='utf8') as f:
            RECORDED['pages'] = list(RECORDED['pages'])
            RECORDED['uids'] = list(RECORDED['uids'])
            json.dump(RECORDED, f)

        with open("data.json", "w", encoding="utf8") as f:
            json.dump(DATA, f)

        session.close()
        loop.close()

if __name__ == '__main__':
    main()
