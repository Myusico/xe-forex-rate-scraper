import asyncio
import os
import random
import time
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

# async def get_rate(context, to_currency: str, date: str):
#     """Fetches exchange rates for a given set of currencies against a base currency on a given date.

#     Args:
#         context: The context of the playwright browser instance.
#         from_currencies: A list of currency codes to fetch exchange rates for.
#         to_currency: The base currency to fetch exchange rates against.
#         date: The date to fetch exchange rates for in the format 'YYYY-MM-DD'.

#     Returns:
#         A list of exchange rates in the same order as the input currencies.
#     """
#     url = f"https://www.xe.com/en-gb/currencytables/?from={to_currency}&date={date}#table-section"
#     page = await context.new_page()
#     await page.goto(url)
#     await page.wait_for_timeout(5000)
#     table = page.locator("div#table-section").locator("table")
#     rates = []
#     await page.close()
#     return rates


async def get_rates(to_currency: str, start_date, end_date):
    """Runs the script to fetch exchange rates from XE.com for a given set of currencies
    against a base currency on a date range.
    """
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    dates = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    return await get_rates_for_dates(to_currency, dates)


async def update_rates(to_currency):
    file = Path(f"data/to_{to_currency}_rates.csv")
    old_df = pd.read_csv(file, index_col="Date")
    old_df.index = pd.to_datetime(old_df.index)
    start_date = max(old_df.index) + timedelta(days=1)
    end_date = datetime.now()
    await get_rates(to_currency, start_date, end_date)


async def get_rates_for_dates(to_currency, dates):
    """Runs the script to fetch exchange rates from XE.com for a given set of currencies
    against a base currency on a list of dates.
    """
    file = Path(f"data/to_{to_currency}_rates.csv")
    if os.path.exists(file):
        old_df = pd.read_csv(file, index_col="Date")
    else:
        old_df = pd.DataFrame()
    batch_size = 10
    date_batches = [dates[i : i + batch_size] for i in range(0, len(dates), batch_size)]
    data = []
    sleep_time = 60
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        for batch in date_batches:
            print(f"{datetime.now()}: getting rates from {batch[0]} to {batch[-1]}")
            coros = [get_rates_for_date(context, to_currency, date) for date in batch]
            data = await asyncio.gather(*coros)
            new_df = pd.concat(data, axis=1)
            new_df.columns = batch
            new_df = new_df.T
            old_df.dropna(how="all", inplace=True)
            old_df = old_df.combine_first(new_df)
            old_df.index.name = "Date"
            old_df.to_csv(file)

            print(f"Sleeping for {sleep_time} seconds to avoid IP ban...")
            time.sleep(sleep_time)  # sleep to avoid IP ban from frequent requests


async def get_rates_for_date(context, to_currency: str, date: str):
    url = f"https://www.xe.com/en-gb/currencytables/?from={to_currency}&date={date}#table-section"
    page = await context.new_page()
    await page.goto(url)
    await page.wait_for_timeout(5000)
    table_locator = page.locator("div#table-section").locator("table")
    table_html = StringIO(await table_locator.evaluate("element => element.outerHTML"))
    # table_html = await table.outer_html()
    # print(table_html)
    day_df = pd.read_html(table_html)[0]
    day_df.set_index("Currency", inplace=True)
    await page.close()
    return day_df[f"{to_currency} per unit"]


if __name__ == "__main__":
    currency = "USD"
    # asyncio.run(get_rates(currency, "2020-01-01", "2024-12-31"))
    asyncio.run(update_rates(currency))
