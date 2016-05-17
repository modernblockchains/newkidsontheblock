import poloniex
import urllib2
import json
import datetime
from collections import defaultdict
import psycopg2
import psycopg2.extras

SEC_IN_DAY = 86400
CUT_OFF_TIME = 1460973599 

def get_currency_data_by_day(pair="BTC_ETH", fr=1438269988, to=1460973599):
    min_time, max_time = (fr, fr + SEC_IN_DAY - 1)
    data_by_day = {}
    while max_time < to:
        print("Getting data for: %s..." % (datetime.datetime.fromtimestamp(int(min_time)).strftime("%Y-%m-%d %H:%M:%S"),)),
        data = get_currency_pair_data(pair, min_time, max_time)
        data_by_day[(min_time, max_time)] = data
        print(" %d trades." % (len(data),))
        min_time, max_time = (max_time + 1, max_time + SEC_IN_DAY)

    max_time = to
    print("Getting data for final day: %s..." % (datetime.datetime.fromtimestamp(int(min_time)).strftime("%Y-%m-%d %H:%M:%S"),)),
    data = get_currency_pair_data(pair, min_time, max_time)
    data_by_day[(min_time, max_time)] = data
    print(" %d trades." % (len(data),))

    return data_by_day


def get_currency_pair_data(pair="BTC_ETH", fr=(CUT_OFF_TIME - SEC_IN_DAY), to=CUT_OFF_TIME):
    result = urllib2.urlopen(urllib2.Request('http://poloniex.com/public?command=' + "returnTradeHistory" + '&currencyPair=' + str(pair) + "&start=" + str(fr) + "&end=" + str(to)))
    return json.loads(result.read())



def get_daily_aggregate(data_by_day):
    aggregate_data_by_day = {}
    for time_range, data in data_by_day.items():
        from_timestamp = datetime.datetime.fromtimestamp(int(time_range[0])).strftime("%Y-%m-%d %H:%M:%S")
        to_timestamp = datetime.datetime.fromtimestamp(int(time_range[1])).strftime("%Y-%m-%d %H:%M:%S")
        buys_only = [d for d in data if d["type"] == "buy"]
        sells_only = [d for d in data if d["type"] == "sell"]
        initializer = float()
        aggregate = {}
        if len(data) > 0:
            aggregate["rate_avg"] = reduce(lambda x, y: x + float(y["rate"]), data, initializer)/len(data)
            aggregate["amount_avg"] = reduce(lambda x, y: x + float(y["amount"]), data, initializer)/len(data)
            aggregate["amount_sum"] = reduce(lambda x, y: x + float(y["amount"]), data, initializer)
            aggregate["total_avg"] = reduce(lambda x, y: x + float(y["total"]), data, initializer)/len(data)
            aggregate["total_sum"] = reduce(lambda x, y: x + float(y["total"]), data, initializer)
            if len(buys_only) > 0:
                aggregate["buy_amount_sum"] = reduce(lambda x, y: x + float(y["amount"]), buys_only, initializer)
                aggregate["buy_amount_avg"] = reduce(lambda x, y: x + float(y["amount"]), buys_only, initializer)/len(buys_only)
                aggregate["buy_total_sum"] = reduce(lambda x, y: x + float(y["total"]), buys_only, initializer)
                aggregate["buy_total_avg"] = reduce(lambda x, y: x + float(y["total"]), buys_only, initializer)/len(buys_only)
            if len(sells_only) > 0:
                aggregate["sell_amount_sum"] = reduce(lambda x, y: x + float(y["amount"]), sells_only, initializer)
                aggregate["sell_amount_avg"] = reduce(lambda x, y: x + float(y["amount"]), sells_only, initializer)/len(sells_only)
                aggregate["sell_total_sum"] = reduce(lambda x, y: x + float(y["total"]), sells_only, initializer)
                aggregate["sell_total_avg"] = reduce(lambda x, y: x + float(y["total"]), sells_only, initializer)/len(sells_only)
        print("Aggregated %s --> %s  - count: %d avg: %f  sum: %f" % (from_timestamp, to_timestamp, len(data), aggregate["rate_avg"], aggregate["total_sum"]))
        else:
            print("Skipped %s --> %s  - NO DATA" % (from_timestamp, to_timestamp))
        aggregate_data_by_day[time_range] = aggregate
    return aggregate_data_by_day


def aggregate_to_db(agg_data, pair="BTC_ETH", schema_name="ethereum"):
    db = psycopg2.connect("dbname='db_blockchain' user='blokchains' host='localhost' password=''", cursor_factory=psycopg2.extras.DictCursor)
    cursor = db.cursor()

    from_curr, to_curr = pair.split("_")

    for time_range, data in agg_data.items():
        from_timestamp = datetime.datetime.fromtimestamp(int(time_range[0])).strftime("%Y-%m-%d %H:%M:%S")
        to_timestamp = datetime.datetime.fromtimestamp(int(time_range[1])).strftime("%Y-%m-%d %H:%M:%S")
        if data:
            print("Inserting data for %s --> %s" % (from_timestamp, to_timestamp))
            default_data = defaultdict(float, data)
            cursor.execute("""INSERT INTO %s.PoloniexPriceAggregate
                (from_currency, to_currency,
                from_time, to_time,
                rate_avg,
                trade_to_avg, trade_to_sum, trade_from_avg, trade_from_sum,
                buy_trade_to_avg, buy_trade_to_sum, buy_trade_from_avg, buy_trade_from_sum,
                sell_trade_to_avg, sell_trade_to_sum, sell_trade_from_avg, sell_trade_from_sum)
                VALUES (%%s, %%s,
                    to_timestamp(%%s), to_timestamp(%%s),
                    %%f,
                    %%f, %%f, %%f, %%f,
                    %%f, %%f, %%f, %%f,
                    %%f, %%f, %%f, %%f)""" % schema_name,
                (from_curr, to_curr,
                    time_range[0], time_range[1],
                    default_data["rate_avg"],
                    default_data["amount_avg"], default_data["amount_sum"], default_data["total_avg"], default_data["total_sum"], 
                    default_data["buy_amount_avg"], default_data["buy_amount_sum"], default_data["buy_total_avg"], default_data["buy_total_sum"], 
                    default_data["sell_amount_avg"], default_data["sell_amount_sum"], default_data["sell_total_avg"], default_data["sell_total_sum"])
                )
            db.commit()
        else:
            print("Skipping data for %s --> %s - NO DATA"

