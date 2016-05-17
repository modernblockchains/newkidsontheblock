Poloniex Trading Data API Scraper
========

These scripts use the public Poloniex API to get trading history data, down to individual trades.
An API key is not necessary for public actions.

Files
  - `poloniex.py`: A Poloniex API wrapper adopted from a link provided on the Poloniex website (not currently used)
  - `poloniex_eth_rates.py`: Some helper functions to fetch pricing data from the API

Typically you would run: `python -i poloniex_eth_rates.py` then:
  - Run the function `get_currency_data_by_day` OR load a pickle in with the data already
  - Run the `get_daily_aggregate` function on the data
  - Output yo shiz!
