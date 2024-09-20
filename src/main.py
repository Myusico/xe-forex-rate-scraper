import os
import asyncio
import time
from datetime import datetime
import pandas as pd
from playwright.async_api import async_playwright

top_50_currencies = [
    "USD",
    "EUR",
    "JPY",
    "GBP",
    "AUD",
    "CAD",
    "CHF",
    "CNY",
    "HKD",
    "NZD",
    "SEK",
    "KRW",
    "SGD",
    "NOK",
    "MXN",
    "INR",
    "RUB",
    "ZAR",
    "TRY",
    "BRL",
    "TWD",
    "DKK",
    "PLN",
    "THB",
    "IDR",
    "HUF",
    "CZK",
    "ILS",
    "CLP",
    "PHP",
    "AED",
    "COP",
    "SAR",
    "MYR",
    "RON",
    "PEN",
    "VND",
    "EGP",
    "PKR",
    "KZT",
    "QAR",
    "UAH",
    "KES",
    "DZD",
    "ARS",
    "BDT",
    "LKR",
    "IQD",
    "IRR",
    "OMR",
]


async def get_exchange_rates(
    context, from_currencies: list, to_currency: str, date: str
):
    url = f"https://www.xe.com/en-gb/currencytables/?from={to_currency}&date={date}#table-section"
    page = await context.new_page()
    await page.goto(url)
    await page.wait_for_timeout(5000)
    table = page.locator("table[class='sc-8b336fdc-3 foLGOz']")
    rates = []
    for currency in from_currencies:
        table_data = (
            table.locator(f"th:has-text('{currency}')").locator("..").locator("td")
        )
        rates.append(
            await table_data.last.text_content() if await table_data.count() else None
        )

    await page.close()
    return rates


async def main():
    to_currency = "USD"
    from_currencies = top_50_currencies[:20]
    from_currencies.remove(to_currency)
    start_date = "2022-10-01"
    end_date = "2024-09-19"
    date_list = (
        pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d").tolist()
    )

    batch_size = 10
    date_batches = [
        date_list[i : i + batch_size] for i in range(0, len(date_list), batch_size)
    ]
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        for batch in date_batches:
            rates = []
            print(f"{datetime.now()}: getting rates from {batch[0]} to {batch[-1]}")
            coros = [
                get_exchange_rates(context, from_currencies, to_currency, date)
                for date in batch
            ]
            rates += await asyncio.gather(*coros)
            new_df = pd.DataFrame(rates, index=batch, columns=from_currencies)
            new_df.index.name = "Date"
            if os.path.exists(f"data/to_{to_currency}_rates.csv"):
                old_df = pd.read_csv(
                    f"data/to_{to_currency}_rates.csv", index_col="Date"
                )
                new_df=new_df.combine_first(old_df)
            new_df.to_csv(f"data/to_{to_currency}_rates.csv")

            time.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
