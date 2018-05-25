#!/usr/bin/env python

import dateutil.parser
from datetime import datetime
from datetime import timedelta
from time import sleep
import os.path
import dateutil.tz
import pickle
import apikeys
import krakenex
import bittrex
import binance.client
from binance.exceptions import BinanceAPIException
import gdax
import coinbase.wallet.client as coinbase_client
from decimal import Decimal


def dp(d):
    try:
        x = dateutil.parser.parse(d)
    except ValueError:
        print(f"bad date: {d}")
        raise
    return addtz(x)


def bithumb_dp(d):
    return addtz(datetime.strptime(d, "%Y-%m-%d%H:%M:%S"))


def addtz(x):
    if not x.tzinfo:
        return x.replace(tzinfo=dateutil.tz.tz.tzlocal())
    return x


def binance_sym(sym):
    syms = {"BCH": "BCC"}
    if sym in syms.keys():
        return syms[sym]
    else:
        return sym


def normalize_sym(sym):
    syms = {"XETH": "ETH", "XXBT": "BTC", "BCC": "BCH"}
    if sym in syms.keys():
        return syms[sym]
    else:
        return sym


def normalize_txtype(txtype):
    if txtype in [
        "buy",
        "sell",
        "match",
        "LIMIT_SELL",
        "LIMIT_BUY",
        "trade",
        "BUY",
        "SELL",
    ]:
        return "trade"
    elif txtype in [
        "deposit",
        "transfer",
        "send",
        "fiat_deposit",
        "fiat_withdrawal",
        "exchange_deposit",
        "withdraw",
        "withdrawal",
        "exchange_withdrawal",
    ]:
        return "transfer"
    elif txtype in ["fee", "rebate", "commission", "stolen"]:
        return "fee"
    elif txtype in ["spent"]:
        return "spent"
    elif txtype in ["gift"]:
        return "gift"
    else:
        raise ValueError(f"no such txtype {txtype}")


def get_all_transactions():
    transactions = []
    exchs = ["gdax", "coinbase", "binance", "kraken", "bittrex", "bithumb", "other"]
    for ex in exchs:
        if os.path.exists(f"{ex}.pickle"):
            # print(f'unpickling {ex}')
            with open(f"{ex}.pickle", "rb") as f:
                t = pickle.load(f)
        else:
            # print(f'loading {ex}')
            t = get_transactions(ex)
            with open(f"{ex}.pickle", "wb") as f:
                pickle.dump(t, f)
        transactions += t
    return transactions


def gdax_price(market, ts):
    if os.path.exists("gdax_price.pickle"):
        with open("gdax_price.pickle", "rb") as f:
            p = pickle.load(f)
    else:
        p = {}
    if (market, ts) in p:
        amt = p[market, ts]
    else:
        c = gdax.AuthenticatedClient(
            apikeys.gdax["apiKey"], apikeys.gdax["secret"], apikeys.gdax["password"]
        )
        data = c.get_product_historic_rates(
            market, start=ts.isoformat(), end=(ts + timedelta(minutes=1)).isoformat()
        )
        sleep(0.5)
        try:
            amt = Decimal(data[0][1])
        except IndexError:
            print(market, ts, data)
            raise
        with open("gdax_price.pickle", "wb") as f:
            p[market, ts] = amt
            pickle.dump(p, f)
    # print(f"gdax price {market} {amt:0.2f}")
    return amt


def binance_price(market, ts):
    if os.path.exists("binance_price.pickle"):
        with open("binance_price.pickle", "rb") as f:
            p = pickle.load(f)
    else:
        p = {}
    if (market, ts) in p:
        amt = p[market, ts]
    else:
        c = binance.client.Client(apikeys.binance["apiKey"], apikeys.binance["secret"])
        st = ts.replace(second=0, microsecond=0)
        et = st + timedelta(minutes=1)
        sleep(0.5)
        data = c.get_klines(
            symbol=market,
            interval="1m",
            startTime=int(st.timestamp()) * 1000,
            endTime=int(et.timestamp()) * 1000,
        )
        amt = Decimal(data[0][3])
        with open("binance_price.pickle", "wb") as f:
            p[market, ts] = amt
            pickle.dump(p, f)

    # print(f"binance price {market} {amt:3g}")
    return amt


def get_usd_for_pair(a, b, ts):
    """this needs to return 2 values
    the value of 1 sym1 in USD, and the value of 1 sym2 in USD
    """
    sym1, amt1 = a
    sym2, amt2 = b
    if sym1 in ["USD", "USDT"]:
        return 1, abs(amt1 / amt2)
    elif sym2 in ["USD", "USDT"]:
        return abs(amt2 / amt1), 1
    if sym1 in ["BTC", "LTC", "ETH"]:
        usd_price = gdax_price(f"{sym1}-USD", ts)
        sym2_price = usd_price * abs(amt1) / abs(amt2)
        # print(f"{sym1} ${usd_price:0.2f} {sym2} ${sym2_price:0.2f} = {usd_price:0.2f} * {abs(amt1):0.2f} / {abs(amt2):0.2f}")
        return usd_price, sym2_price
    if sym2 in ["BTC", "LTC", "ETH"]:
        usd_price = gdax_price(f"{sym2}-USD", ts)
        sym1_price = usd_price * abs(amt2) / abs(amt1)
        # print(f"{sym1} ${sym1_price:0.2f} = {usd_price:0.2f} * {abs(amt1):0.2f} / {abs(amt2):0.2f} {sym2} ${usd_price:0.2f}")
        return sym1_price, usd_price
    # exclude symbols we know aren't on binance
    elif sym1 not in ["KRW", "XLM"]:
        # print(f"getting {sym1}BTC from binance")
        sym1_btc_price = binance_price(f"{binance_sym(sym1)}BTC", ts)
        btcusd_price = gdax_price(f"BTC-USD", ts)
        usd_price = sym1_btc_price * btcusd_price
        sym2_price = usd_price * abs(amt1) / abs(amt2)
        # print(f"{sym1} ${usd_price:0.2f} {sym2} ${sym2_price:0.2f} = {usd_price:0.2f} * {abs(amt1):0.2f} / {abs(amt2):0.2f}")
        return usd_price, sym2_price
    elif sym2 not in ["KRW", "XLM"]:
        # print(f"getting {sym2}BTC from binance")
        sym2_btc_price = binance_price(f"{binance_sym(sym2)}BTC", ts)
        btcusd_price = gdax_price(f"BTC-USD", ts)
        usd_price = sym2_btc_price * btcusd_price
        sym1_price = usd_price * abs(amt2) / abs(amt1)
        # print(f"{sym1} ${sym1_price:0.2f} = {usd_price:0.2f} * {abs(amt1):0.2f} / {abs(amt2):0.2f} {sym2} ${usd_price:0.2f}")
        return sym1_price, usd_price

    raise ValueError(f"can't get exchange rate for {a} {b} {ts}")


def get_current_usd(a, ts=None):
    if not ts:
        ts = datetime.now().replace(
            minute=0, second=0, microsecond=0, tzinfo=dateutil.tz.tz.tzlocal()
        )
    if a.sym in ["USD", "USDT"]:
        return 1
    if a.sym in ["KRW"]:
        return 1 / 1165
    elif a.sym in ["BTC", "LTC", "ETH"]:
        return gdax_price(f"{a.sym}-USD", ts)
    elif a.sym == "BCH" and ts > datetime(
        year=2018, month=1, day=25, tzinfo=dateutil.tz.tz.tzlocal()
    ):
        return gdax_price(f"{a.sym}-USD", ts)
    else:
        try:
            btc_price = binance_price(f"{binance_sym(a.sym)}BTC", ts)
        except (TypeError, BinanceAPIException):
            print(f"ERROR can't get price for {a.sym}")
            return 0
        btcusd_price = gdax_price(f"BTC-USD", ts)
        usd_price = btc_price * btcusd_price
        return usd_price


def other_transactions():
    transactions = []
    with open("othertx.txt", encoding="utf-8") as f:
        for line in f:
            date, txtype, exchange, amount, sym = line.rstrip().split(",")
            ts = dp(date)
            transactions.append([ts, exchange, txtype, sym, Decimal(amount)])
    return transactions


def kraken_transactions():
    transactions = []
    apiclient = krakenex.API(
        key=apikeys.kraken["apiKey"], secret=apikeys.kraken["secret"]
    )
    ledgers = apiclient.query_private("Ledgers")
    for ledgerid, ledger in ledgers["result"]["ledger"].items():
        # print(ledger)
        ts = addtz(datetime.fromtimestamp(ledger["time"]))
        transactions.append(
            [ts, "kraken", ledger["type"], ledger["asset"], Decimal(ledger["amount"])]
        )
        if Decimal(ledger["fee"]) > 0.0:
            transactions.append(
                [ts, "kraken", "fee", ledger["asset"], -Decimal(ledger["fee"])]
            )

    return transactions


def bithumb_transactions():
    transactions = []
    with open("bithumb.txt", encoding="utf-8") as f:
        f.readline().split("\t")
        for line in f:
            rec = line.replace('"', "").split("\t")
            ts = bithumb_dp(rec[0])
            sym = rec[1]
            order = rec[2]
            qty_coin = Decimal("".join([c for c in rec[3] if c.isdigit() or c == "."]))
            settlement = Decimal(
                "".join([c for c in rec[7] if c.isdigit() or c == "."])
            )
            if rec[6] == "-":
                fee = Decimal(0)
                fee_sym = "KRW"
            else:
                fee = Decimal("".join([c for c in rec[6] if c.isdigit() or c == "."]))
                fee_sym = rec[6][-3:]

            # still need to check that all of the transaction directions go the right way
            if "BUY" in order:
                transactions.append([ts, "bithumb", order, sym, qty_coin, rec])
                transactions.append([ts, "bithumb", order, "KRW", -settlement, rec])
                transactions.append([ts, "bithumb", "fee", fee_sym, -fee, rec])
            elif "SELL" in order:
                transactions.append([ts, "bithumb", order, sym, -qty_coin, rec])
                transactions.append([ts, "bithumb", order, "KRW", settlement, rec])
                transactions.append([ts, "bithumb", "fee", fee_sym, -fee, rec])
            elif "DEPOSIT" in order:
                transactions.append([ts, "bithumb", "deposit", sym, qty_coin, rec])
                transactions.append([ts, "bithumb", "fee", fee_sym, -fee, rec])
            elif "WITHDRAWAL" in order:
                transactions.append([ts, "bithumb", "withdrawal", sym, -qty_coin, rec])
                transactions.append([ts, "bithumb", "fee", fee_sym, -fee, rec])
            else:
                print(f"unknown order type {rec}")
    return transactions


def bittrex_transactions():
    transactions = []
    with open("bittrex.txt", encoding="utf-8") as f:
        f.readline().split("\t")
        for line in f:
            rec = line.split("\t")
            # uuid = rec[0]
            base, quote = rec[1].split("-")
            order = rec[2]
            qty = Decimal(rec[3])
            # limit = Decimal(rec[4])
            commission = Decimal(rec[5])
            price = Decimal(rec[6])
            ts = dp(rec[8])
            if "BUY" in order:
                transactions.append([ts, "bittrex", order, base, -price, rec])
                transactions.append([ts, "bittrex", "fee", base, -commission, rec])
                transactions.append([ts, "bittrex", order, quote, qty, rec])
            elif "SELL" in order:
                transactions.append([ts, "bittrex", order, base, price, rec])
                transactions.append([ts, "bittrex", "fee", base, -commission, rec])
                transactions.append([ts, "bittrex", order, quote, -qty, rec])
            else:
                print(f"unknown order type {rec}")
    apiclient = bittrex.Bittrex(apikeys.bittrex["apiKey"], apikeys.bittrex["secret"])
    dh = apiclient.get_deposit_history()
    wh = apiclient.get_withdrawal_history()
    for tx in dh["result"]:
        ts = dp(tx["LastUpdated"])
        transactions.append(
            [ts, "bittrex", "deposit", tx["Currency"], Decimal(tx["Amount"]), tx]
        )
    for tx in wh["result"]:
        ts = dp(tx["Opened"])
        transactions.append(
            [ts, "bittrex", "withdrawal", tx["Currency"], -Decimal(tx["Amount"]), tx]
        )
    return transactions


def binance_transactions():
    transactions = []
    txs = {}
    assets = set()
    apiclient = binance.client.Client(
        apikeys.binance["apiKey"], apikeys.binance["secret"]
    )
    txs["withdraw"] = apiclient.get_withdraw_history()
    txs["deposit"] = apiclient.get_deposit_history()
    for d in ["withdraw", "deposit"]:
        for tx in txs[d][d + "List"]:
            if "successTime" in tx.keys():
                ts = addtz(datetime.fromtimestamp(tx["successTime"] / 1000))
            else:
                ts = addtz(datetime.fromtimestamp(tx["insertTime"] / 1000))
                assets.add(tx["asset"])
            if d == "withdraw":
                transactions.append(
                    [ts, "binance", d, tx["asset"], -Decimal(tx["amount"]), tx]
                )
            else:
                transactions.append(
                    [ts, "binance", d, tx["asset"], Decimal(tx["amount"]), tx]
                )
    pr = apiclient.get_products()
    new_assets = set()
    for p in pr["data"]:
        if p["baseAsset"] in assets or p["quoteAsset"] in assets:
            for tx in apiclient.get_my_trades(symbol=p["symbol"]):
                sleep(0.25)
                if p["baseAsset"] not in assets:
                    new_assets.add(p["baseAsset"])
                if p["quoteAsset"] not in assets:
                    new_assets.add(p["quoteAsset"])
                ts = addtz(datetime.fromtimestamp(tx["time"] / 1000))
                if tx["isBuyer"] is True:
                    transactions.append(
                        [
                            ts,
                            "binance",
                            "buy",
                            p["quoteAsset"],
                            -Decimal(tx["qty"]) * Decimal(tx["price"]),
                        ]
                    )
                    transactions.append(
                        [ts, "binance", "buy", p["baseAsset"], Decimal(tx["qty"])]
                    )
                else:
                    transactions.append(
                        [
                            ts,
                            "binance",
                            "sell",
                            p["quoteAsset"],
                            Decimal(tx["qty"]) * Decimal(tx["price"]),
                        ]
                    )
                    transactions.append(
                        [ts, "binance", "sell", p["baseAsset"], -Decimal(tx["qty"])]
                    )
                transactions.append(
                    [
                        ts,
                        "binance",
                        "commission",
                        tx["commissionAsset"],
                        -Decimal(tx["commission"]),
                    ]
                )
    return transactions


def gdax_transactions():
    transactions = []
    apiclient = gdax.AuthenticatedClient(
        apikeys.gdax["apiKey"], apikeys.gdax["secret"], apikeys.gdax["password"]
    )
    gdax_accounts = apiclient.get_accounts()
    for a in gdax_accounts:
        ah = apiclient.get_account_history(account_id=a["id"])
        for txs in ah:
            for tx in txs:
                created = dp(tx["created_at"])
                if tx["type"] in ["fee", "match"]:
                    transactions.append(
                        [
                            created,
                            "gdax",
                            tx["type"],
                            a["currency"],
                            Decimal(tx["amount"]),
                            tx["details"],
                        ]
                    )
                else:
                    transactions.append(
                        [
                            created,
                            "gdax",
                            tx["type"],
                            a["currency"],
                            Decimal(tx["amount"]),
                            tx["details"],
                        ]
                    )
    return transactions


def coinbase_transactions():
    transactions = []
    apiclient = coinbase_client.Client(
        apikeys.coinbase["apiKey"], apikeys.coinbase["secret"]
    )
    c = apiclient
    for a in c.get_accounts()["data"]:
        for tx in c.get_transactions(a["id"])["data"]:
            created = dp(tx["created_at"])
            if tx["type"] in ["fiat_deposit", "fiat_withdrawal"]:
                transactions.append(
                    [
                        created,
                        "coinbase",
                        tx["type"],
                        tx["native_amount"]["currency"],
                        Decimal(tx["native_amount"]["amount"]),
                        tx,
                    ]
                )
                transactions.append(
                    [
                        created,
                        "bofa",
                        tx["type"],
                        tx["native_amount"]["currency"],
                        -Decimal(tx["native_amount"]["amount"]),
                        tx,
                    ]
                )
            elif (
                tx["type"] == "buy"
                and "Bank of" in tx["details"]["payment_method_name"]
            ):
                # for a credit card payment, create 2 ledger entries transferring usd from the bank
                transactions.append(
                    [
                        created,
                        "bofa",
                        "fiat_deposit",
                        tx["native_amount"]["currency"],
                        -Decimal(tx["native_amount"]["amount"]),
                        tx,
                    ]
                )
                transactions.append(
                    [
                        created,
                        "coinbase",
                        "fiat_deposit",
                        tx["native_amount"]["currency"],
                        Decimal(tx["native_amount"]["amount"]),
                        tx,
                    ]
                )
                # now debit the USD from the coinbase account as a buy
                transactions.append(
                    [
                        created,
                        "coinbase",
                        tx["type"],
                        tx["native_amount"]["currency"],
                        -Decimal(tx["native_amount"]["amount"]),
                        tx,
                    ]
                )
                # and credit the cryptocurrency bought as a buy
                transactions.append(
                    [
                        created,
                        "coinbase",
                        tx["type"],
                        tx["amount"]["currency"],
                        Decimal(tx["amount"]["amount"]),
                        tx,
                    ]
                )
            else:
                transactions.append(
                    [
                        created,
                        "coinbase",
                        tx["type"],
                        tx["amount"]["currency"],
                        Decimal(tx["amount"]["amount"]),
                        tx,
                    ]
                )

    return transactions


def get_transactions(exchange):
    if exchange == "gdax":
        return gdax_transactions()
    elif exchange == "coinbase":
        return coinbase_transactions()
    elif exchange == "binance":
        return binance_transactions()
    elif exchange == "kraken":
        return kraken_transactions()
    elif exchange == "bittrex":
        return bittrex_transactions()
    elif exchange == "bithumb":
        return bithumb_transactions()
    elif exchange == "other":
        return other_transactions()
