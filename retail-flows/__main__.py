
from datetime import datetime, timedelta
from functools import cached_property
from itertools import batched
from urllib.parse import urlencode
from urllib3 import HTTPResponse, Retry
from typing import Any, Dict, Final, Iterator, List, Optional, Self, TypedDict

import argparse
import certifi
import csv
import json
import pandas as pd
import os
import sqlite3
import urllib3

t0: Final[datetime] = datetime(2016, 1, 15)
t1: Final[datetime] = datetime.now()

NASDAQ_TYPE_MAP: Final[Dict[str, type]] = {
    'Date': datetime,
    'text': str,
    'double': float,
    'Integer': int,
}

class ColumnDict(TypedDict, total=True):
    name: str
    column_type: type

class Column():
    def __init__(self: Self, name: str, column_type: type):
        self._name = name
        self._column_type = column_type

    @cached_property
    def name(self: Self) -> str:
        return self._name

    @cached_property
    def column_type(self: Self) -> type:
        return self._column_type

    def __repr__(self: Self) -> str:
        return f'Column(name={self.name}, type={self.column_type})'

    @staticmethod
    def from_dict(d: Dict[str, str]) -> Self:
        if 'name' not in d:
            raise ValueError(f"missing 'name' in ColumnDict.from_dict({d})")

        if 'type' not in d:
            raise ValueError(f"missing 'type' in ColumnDict.from_dict({d})")

        return Column(
            name=d.get('name'),
            column_type=NASDAQ_TYPE_MAP[d.get('type')],
        )

    def to_dict(self: Self) -> ColumnDict:
        return TypedDict({'name': self.name, 'type': self.column_type})

class RowDict(TypedDict, total=True):
    date: datetime
    name: str
    activity: float
    sentiment: int

class Row():
    def __init__(self: Self, cols: List[Any]):

        self._date = cols[0]
        self._name = cols[1]
        self._activity = float(cols[2])
        self._sentiment = int(cols[3])

    @cached_property
    def date(self: Self) -> datetime:
        return self._date

    @cached_property
    def name(self: Self) -> str:
        return self._name

    @cached_property
    def activity(self: Self) -> float:
        return self._activity

    @cached_property
    def sentiment(self: Self) -> int:
        return self._sentiment

    def __repr__(self: Self) -> str:
        return f'Row(date={self.date}, name={self.name}, activity={self.activity}, sentiment={self.sentiment})'

class Client():
    BASE_URL: Final[str] = 'https://data.nasdaq.com/api/v3/datatables'

    HEADERS: Final[Dict[str, str]] = {
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'User-Agent': 'retail-flows-client/0.1.0',
    }

    def __init__(self: Self, api_key: str, retries: int = 10):

        self._api_key = api_key
        self._retries = retries

        self._manager = urllib3.PoolManager(
            num_pools=10,
            headers=Client.HEADERS,
            ca_certs=certifi.where(),
            cert_reqs='CERT_REQUIRED',
            retries=Retry(
                total=self.retries,
                status_forcelist=[413, 429, 499, 500, 502, 503, 504],  # default 413, 429, 503
                backoff_factor=0.1,  # [0.0s, 0.2s, 0.4s, 0.8s, 1.6s, ...]
            ),  # use the customized Retry instance
        )

        pass

    @property
    def api_key(self: Self) -> str:
        return self._api_key

    @property
    def manager(self: Self) -> urllib3.PoolManager:
        return self._manager

    @property
    def retries(self: Self) -> int:
        return self._retries

    def ticker_coverage(self: Self) -> List[str]:
        resp: HTTPResponse = self.manager.request('GET', 'https://static.quandl.com/coverage/NDAQ_RTAT.csv')
        return list(resp.data.decode('utf-8').splitlines())[1:]

    def last_update(self: Self, cur: sqlite3.Cursor, ticker: str) -> datetime:
        cur.execute('SELECT MAX(date) FROM rtat WHERE ticker = ?', (ticker,))

        result = cur.fetchone()[0]

        if result is None:
            return t0
        else:
            return datetime.strptime(result, '%Y-%m-%d')

    def retail_track(
            self: Self,
            tickers: Iterator[str],
            timestamps: Iterator[datetime]
        ) -> urllib3.HTTPResponse:

        dates = [t.strftime('%Y-%m-%d') for t in list(timestamps)]

        if len(dates) <= 0:
            raise ValueError(f"no dates provided.")
        else:
            params = urlencode({
                'date': str.join(',', dates),
                'ticker': str.join(',', tickers),
                'api_key': self.api_key,
            })

            resp = self.manager.request('GET', f'{Client.BASE_URL}/NDAQ/RTAT?{params}').data.decode('utf-8')
            table = json.loads(resp)['datatable']

            return list(map(Row, table['data']))

def daily(t0: datetime, t1: datetime) -> Iterator[datetime]:
    t = t0

    while t <= t1:
        weekday = t.weekday()

        if weekday == 5 or weekday == 6:  # Sat (5) - Sun (6)
            t += timedelta(days=7 - weekday)

        yield t

        t += timedelta(days=1)

def main():
    parser = argparse.ArgumentParser(description='Fetch retail trading activity and sentiment data from Nasdaq.')
    parser.add_argument('--api-key', type=str, required=True, help='Your Nasdaq API key.')
    args = parser.parse_args()

    con = sqlite3.connect("retail-flows.db")
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS rtat (
            date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            activity REAL NOT NULL,
            sentiment INTEGER NOT NULL,
            PRIMARY KEY (date, ticker)
        )
    """)

    con.commit()

    client: Final[Client] = Client(api_key=args.api_key)

    i: int = 0

    for tickers in batched(client.ticker_coverage(), 10):
        for timestamps in batched(daily(t0, t1), 10):
            resp = client.retail_track(tickers=tickers, timestamps=timestamps)

            for row in resp:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO rtat (date, ticker, activity, sentiment)
                    VALUES (?, ?, ?, ?)
                    """,
                    (row.date, row.name, row.activity, row.sentiment)
                )

            print(i * 100)
            i+=1
    # for ticker in client.ticker_coverage():
    #     t = client.last_update(cur, ticker)

    #     print(f'Fetching {ticker} from {t} to {t1}')

    #     while t <= t1:

    #         for row in resp:
    #             cur.execute(
    #                 """
    #                 INSERT OR REPLACE INTO rtat (date, ticker, activity, sentiment)
    #                 VALUES (?, ?, ?, ?)
    #                 """,
    #                 (row.date, row.name, row.activity, row.sentiment)
    #             )

    #         con.commit()
    #         t += timedelta(days=1)
    #         print(t)


    # for ticker in client.ticker_coverage():
    #     cur.execute(
    #         """
    #         """,
    #         (ticker, t0)
    #     )




    # t = args.t0

    # while t <= args.t1:
    #     resp = client.retail_track(
    #         ticker=args.ticker,
    #         timestamps=[t]
    #     )

    #     print(resp)
    #     t += timedelta(days=1)

    # resp = client.retail_track(
    #     ticker='MSTR',
    #     timestamps=[
    #         datetime(2023, 6, 5),
    #         datetime(2023, 6, 6),
    #     ]
    # )

    # print(resp)


if __name__ == '__main__':
    main()