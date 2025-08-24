
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

# t0, the time that RTAT data is available from
t0: Final[datetime] = datetime(2016, 1, 15)

# t1, the time that RTAT data is available up to (now)
t1: Final[datetime] = datetime.now()

# n_days, the number of days between t0 and t1. This is used to determine how
# many rows to fetch via the API.
n_days: Final[int] = (t1 - t0).days

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

        self._date = datetime.strptime(cols[0], '%Y-%m-%d')
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

        self._con = sqlite3.connect("retail-flows.db")
        self._cur = self.con.cursor()

        self._cur.execute("""
            CREATE TABLE IF NOT EXISTS rtat (
                date TEXT NOT NULL,
                ticker TEXT NOT NULL,
                activity REAL NOT NULL,
                sentiment INTEGER NOT NULL,
                PRIMARY KEY (date, ticker)
            )
        """)

        self.con.commit()

        pass

    @property
    def api_key(self: Self) -> str:
        return self._api_key

    @property
    def con(self: Self) -> sqlite3.Connection:
        return self._con

    @property
    def cur(self: Self) -> sqlite3.Cursor:
        return self._cur

    @property
    def manager(self: Self) -> urllib3.PoolManager:
        return self._manager

    @property
    def retries(self: Self) -> int:
        return self._retries

    def ticker_coverage(self: Self) -> Iterator[List[str]]:
        resp: HTTPResponse = self.manager.request('GET', 'https://static.quandl.com/coverage/NDAQ_RTAT.csv')
        return list(resp.data.decode('utf-8').splitlines())[1:]

    def needs_update(self: Self, timestamps: datetime | Iterator[datetime]) -> Iterator[str]:
        if isinstance(timestamps, datetime):
            timestamps = [timestamps]
        elif isinstance(timestamps, tuple):
            timestamps = list(timestamps)
        elif not isinstance(timestamps, list):
            raise ValueError(f"dates must be a datetime or list of datetimes, got {repr(timestamps)}")

        for timestamp in timestamps:
            self.cur.execute('SELECT ticker, COUNT(ticker) FROM rtat WHERE date > ?', (timestamp,))

            ticker, n = self.cur.fetchone()

            if n > 0:
                yield ticker

    def last_update(self: Self, ticker: str) -> datetime:
        self.cur.execute('SELECT MAX(date) FROM rtat WHERE ticker = ?', (ticker,))

        result = self.cur.fetchone()[0]

        if result is None:
            return t0
        else:
            return datetime.strptime(result, '%Y-%m-%d')

    def retail_track(
            self: Self,
            tickers: Iterator[str],
            timestamps: Iterator[datetime]
        ) -> Iterator[Row]:

        dates = [t.strftime('%Y-%m-%d') for t in list(timestamps)]

        if len(dates) <= 0:
            raise ValueError(f"no dates provided.")
        else:
            params = urlencode({
                'date': str.join(',', dates),
                'ticker': str.join(',', list(tickers)),
                'api_key': self.api_key,
            })

            resp = self.manager.request('GET', f'{Client.BASE_URL}/NDAQ/RTAT?{params}').data.decode('utf-8')
            body = json.loads(resp)

            if isinstance(body, dict):
                body = body.get('datatable')
            else:
                raise ValueError(f"missing 'datatable' in response: {body}")

            if isinstance(body, dict):
                body = body.get('data')
            else:
                raise ValueError(f"missing 'data' in response datatable: {body}")

            rows = map(Row, body)

            for row in rows:
                self.cur.execute(
                    """
                    INSERT OR REPLACE INTO rtat (date, ticker, activity, sentiment)
                    VALUES (?, ?, ?, ?)
                    """,
                    (row.date, row.name, row.activity, row.sentiment)
                )

            self.con.commit()

def daily(t0: datetime, t1: datetime) -> Iterator[datetime]:
    t = t0

    while t <= t1:
        weekday = t.weekday()

        if weekday == 5 or weekday == 6:  # Sat (5) - Sun (6)
            t += timedelta(days=7 - weekday)

        yield t

        t += timedelta(days=1)

def main():
    parser = argparse.ArgumentParser(
        description='Fetch retail trading activity and sentiment (RTAT) data from NASDAQ.'
    )

    parser.add_argument('--api-key',
        type=str,
        required=True,
        help='Your Nasdaq API key.'
    )

    parser.add_argument('--batch-size',
        type=int,
        default=100,
        help='The number of tickers and dates to fetch per request. Default is 10.')


    args = parser.parse_args()

    client: Final[Client] = Client(api_key=args.api_key)

    for timestamps in batched(daily(t0, t1), args.batch_size):
        for tickers in batched(client.ticker_coverage(), args.batch_size):
            resp = client.retail_track(tickers=tickers, timestamps=timestamps)
            print(tickers, timestamps)

if __name__ == '__main__':
    main()