#!/usr/bin/env python

import apikeys
import krakenex
import bittrex
import binance.client
import gdax
import coinbase.wallet.client as coinbase_client
import pickle
import os.path
import prettytable
import sys
from collections import Counter
from time import sleep
from pprint import pprint
import xcoin_api_client
from datetime import datetime
from datetime import timedelta
import dateutil.parser
import dateutil.tz
from decimal import Decimal
from collections import defaultdict
import requests
from copy import deepcopy

e = {}

class AssetLedger(object):
    def __init__(self, sym):
        self.sym = sym
        self.balance = Decimal(0)
        self.usd_avg_cost_basis = None
        if sym in ["USD", "USDT"]:
            self.usd_avg_cost_basis = Decimal(1.0)

    def trade(self, amount, usd_unit_price):
        if amount > 0:
            self.buy(amount, usd_unit_price)
        else:
            self.sell(amount, usd_unit_price)

    def buy(self, amount, usd_unit_price):
        """units, usd_price per 1 unit"""
        self.balance += amount
        if self.usd_avg_cost_basis:
            new_usd_price = ((self.usd_avg_cost_basis * self.balance) + (amount * usd_unit_price)) / (self.balance + amount)
            print(f"{new_usd_price} $/{self.sym} = (${self.usd_avg_cost_basis:0.2f} * {self.balance}{self.sym}) + ({usd_unit_price:0.2f} * {amount}{self.sym}) / ({self.balance} + {amount}) {self.sym}")
        else:
            new_usd_price = usd_unit_price
            print(f"{new_usd_price} $/{self.sym} = {usd_unit_price}")
        self.usd_avg_cost_basis = new_usd_price
        print(f"{self.sym} balance: {self.balance:0.2f} new cost basis ${self.usd_avg_cost_basis:0.2f}")

    def sell(self, amount, usd_unit_price):
        if amount > self.balance:
            raise ValueError(f"sell amount {amount} exceeds balance {self.balance}")
        print(f"selling {amount} of {self.sym} for ${usd_unit_price} cost basis ${self.usd_avg_cost_basis}")
        profitloss = (amount * usd_unit_price) - (amount * self.usd_avg_cost_basis)
        print(f"p/l {profitloss}")
        self.balance -= amount

    def transfer(self, amount):
        self.balance += amount

    def __repr__(self):
        return f"AssetLedger {self.balance:0.2f}{self.sym} ${self.usd_avg_cost_basis:0.2f}"

def dp(d):
    try:
        x=dateutil.parser.parse(d)
    except ValueError:
        print(f'bad date: {d}')
        raise
    return addtz(x)

def bithumb_dp(d):
    return addtz(datetime.strptime(d,'%Y-%m-%d%H:%M:%S'))

def addtz(x):
    if not x.tzinfo:
        return x.replace(tzinfo=dateutil.tz.tz.tzlocal())
    return x

def other_transactions():
    transactions = []
    with open("othertx.txt", encoding="utf-8") as f:
        for line in f:
            print("othertx",line)
            date,txtype,exchange,amount,sym = line.rstrip().split(',')
            ts = dp(date)
            transactions.append([ts, exchange, txtype, sym, Decimal(amount)])
    print(transactions)
    return transactions

def kraken_transactions():
    transactions = []
    e['kraken'] = krakenex.API(key=apikeys.kraken['apiKey'], secret=apikeys.kraken['secret'])
    ledgers = e['kraken'].query_private('Ledgers')
    for ledgerid, ledger in ledgers['result']['ledger'].items():
        print(ledger)
        ts = addtz(datetime.fromtimestamp(ledger['time']))
        transactions.append([ts, 'kraken', ledger['type'], ledger['asset'], Decimal(ledger['amount'])])
        if Decimal(ledger['fee']) > 0.0:
            transactions.append([ts, 'kraken', 'fee', ledger['asset'], -Decimal(ledger['fee'])])

    return transactions
 
 
def bithumb_transactions():
    transactions = []
    with open("bithumb.txt", encoding="utf-8") as f:
        header = f.readline().split('\t')
        for line in f:
            rec = line.replace("\"", "").split('\t')
            ts = bithumb_dp(rec[0])
            sym = rec[1]
            order = rec[2]
            qty_coin = Decimal("".join([c for c in rec[3] if c.isdigit() or c == "."]))
            settlement = Decimal("".join([c for c in rec[7] if c.isdigit() or c == "."]))
            if rec[6] == "-":
                fee = Decimal(0)
                fee_sym = "KRW"
            else:
                fee = Decimal("".join([c for c in rec[6] if c.isdigit() or c == "."]))
                fee_sym = rec[6][-3:]

            # still need to check that all of the transaction directions go the right way
            if 'BUY' in order:
                transactions.append([ts, 'bithumb', order, sym, qty_coin, rec])
                transactions.append([ts, 'bithumb', order, "KRW", -settlement, rec])
                transactions.append([ts, 'bithumb', "fee", fee_sym, -fee, rec])
            elif 'SELL' in order:
                transactions.append([ts, 'bithumb', order, sym, -qty_coin, rec])
                transactions.append([ts, 'bithumb', order, "KRW", settlement, rec])
                transactions.append([ts, 'bithumb', "fee", fee_sym, -fee, rec])
            elif 'DEPOSIT' in order:
                transactions.append([ts, 'bithumb', "deposit", sym, qty_coin, rec])
                transactions.append([ts, 'bithumb', "fee", fee_sym, -fee, rec])
            elif 'WITHDRAWAL' in order:
                transactions.append([ts, 'bithumb', "withdrawal", sym, -qty_coin, rec])
                transactions.append([ts, 'bithumb', "fee", fee_sym, -fee, rec])
            else:
                print(f"unknown order type {rec}")
    return transactions
    
def bittrex_transactions():
    transactions = []
    with open("bittrex.txt", encoding="utf-8") as f:
        header = f.readline().split('\t')
        for line in f:
            rec = line.split('\t')
            uuid = rec[0]
            base, quote = rec[1].split('-')
            order = rec[2]
            qty = Decimal(rec[3])
            limit = Decimal(rec[4])
            commission = Decimal(rec[5])
            price = Decimal(rec[6])
            ts = dp(rec[8])
            if 'BUY' in order:
                transactions.append([ts, 'bittrex', order, base, -price, rec])
                transactions.append([ts, 'bittrex', "fee", base, -commission, rec])
                transactions.append([ts, 'bittrex', order, quote, qty, rec])
            elif 'SELL' in order:
                transactions.append([ts, 'bittrex', order, base, price, rec])
                transactions.append([ts, 'bittrex', "fee", base, -commission, rec])
                transactions.append([ts, 'bittrex', order, quote, -qty, rec])
            else:
                print(f"unknown order type {rec}")
    e['bittrex'] = bittrex.Bittrex(apikeys.bittrex['apiKey'], apikeys.bittrex['secret'])
    dh = e['bittrex'].get_deposit_history()
    wh = e['bittrex'].get_withdrawal_history()
    for tx in dh['result']:
       ts = dp(tx['LastUpdated'])
       transactions.append([ts, 'bittrex', 'deposit', tx['Currency'], Decimal(tx['Amount']), tx])
    for tx in wh['result']:
       ts = dp(tx['Opened'])
       transactions.append([ts, 'bittrex', 'withdrawal', tx['Currency'], -Decimal(tx['Amount']), tx])
    return transactions

def binance_transactions():
    transactions = []
    txs = {}
    assets = set()
    e['binance'] = binance.client.Client(apikeys.binance['apiKey'], apikeys.binance['secret'])
    txs['withdraw'] = e['binance'].get_withdraw_history()
    txs['deposit'] = e['binance'].get_deposit_history()
    for d in ['withdraw','deposit']:
        for tx in txs[d][d + 'List']:
            if 'successTime' in tx.keys():
                ts = addtz(datetime.fromtimestamp(tx['successTime']/1000))
            else:
                ts = addtz(datetime.fromtimestamp(tx['insertTime']/1000))
                assets.add(tx['asset'])
            if d == 'withdraw':
                transactions.append([ts, 'binance', d, tx['asset'], -Decimal(tx['amount']), tx])
            else:
                transactions.append([ts, 'binance', d, tx['asset'], Decimal(tx['amount']), tx])
    pr = e['binance'].get_products()
    new_assets = set()
    for p in pr['data']:
        if p['baseAsset'] in assets or p['quoteAsset'] in assets:
            for tx in e['binance'].get_my_trades(symbol=p['symbol']):
                sleep(0.25)
                if p['baseAsset'] not in assets:
                    new_assets.add(p['baseAsset'])
                if p['quoteAsset'] not in assets:
                    new_assets.add(p['quoteAsset'])
                ts = addtz(datetime.fromtimestamp(tx['time']/1000))
                if tx['isBuyer'] is True:
                    transactions.append([ts, 'binance', 'buy', p['quoteAsset'], -Decimal(tx['qty'])*Decimal(tx['price'])])
                    transactions.append([ts, 'binance', 'buy', p['baseAsset'], Decimal(tx['qty'])])
                else:
                    transactions.append([ts, 'binance', 'sell', p['quoteAsset'], Decimal(tx['qty'])*Decimal(tx['price'])])
                    transactions.append([ts, 'binance', 'sell', p['baseAsset'], -Decimal(tx['qty'])])
                transactions.append([ts, 'binance', 'commission', tx['commissionAsset'], -Decimal(tx['commission'])])
                # TODO figure out how to visit and download new_assets
    return transactions

def gdax_transactions():
    transactions = []
    e['gdax'] = gdax.AuthenticatedClient(apikeys.gdax['apiKey'], apikeys.gdax['secret'], apikeys.gdax['password'])
    gdax_accounts = e['gdax'].get_accounts()
    for a in gdax_accounts:
        ah = e['gdax'].get_account_history(account_id=a['id'])
        for txs in ah:
            for tx in txs:
                created = dp(tx['created_at'])
                if tx['type'] in ['fee','match']:
                    transactions.append([created, 'gdax', tx['type'], a['currency'], Decimal(tx['amount']), tx['details']])
                else:
                    transactions.append([created, 'gdax', tx['type'], a['currency'], Decimal(tx['amount']), tx['details']])
    return transactions

def coinbase_transactions():
    transactions = []
    e['coinbase'] = coinbase_client.Client(apikeys.coinbase['apiKey'], apikeys.coinbase['secret'])
    c = e['coinbase']
    for a in c.get_accounts()['data']:
        for tx in c.get_transactions(a['id'])['data']:
            created = dp(tx['created_at'])
            if tx['type'] in ['fiat_deposit', 'fiat_withdrawal']:
                transactions.append([created, 'coinbase', tx['type'],
                    tx['native_amount']['currency'], Decimal(tx['native_amount']['amount']), tx])
                transactions.append([created, 'bofa', tx['type'],
                    tx['native_amount']['currency'], -Decimal(tx['native_amount']['amount']), tx])
            elif tx['type'] == "buy" and "Bank of" in tx['details']['payment_method_name']:
                # for a credit card payment, create 2 ledger entries transferring usd from the bank
                transactions.append([created, 'bofa', "fiat_deposit",
                    tx['native_amount']['currency'], -Decimal(tx['native_amount']['amount']), tx])
                transactions.append([created, 'coinbase', "fiat_deposit",
                    tx['native_amount']['currency'], Decimal(tx['native_amount']['amount']), tx])
                # now debit the USD from the coinbase account as a buy
                transactions.append([created, 'coinbase', tx['type'],
                    tx['native_amount']['currency'], -Decimal(tx['native_amount']['amount']), tx])
                # and credit the cryptocurrency bought as a buy
                transactions.append([created, 'coinbase', tx['type'],
                    tx['amount']['currency'], Decimal(tx['amount']['amount']), tx])
            else:
                transactions.append([created, 'coinbase', tx['type'],
                    tx['amount']['currency'], Decimal(tx['amount']['amount']), tx])

    return transactions

def get_transactions(exchange):
    if exchange == 'gdax':
        return gdax_transactions()
    elif exchange == 'coinbase':
        return coinbase_transactions()
    elif exchange == 'binance':
        return binance_transactions()
    elif exchange == 'kraken':
        return kraken_transactions()
    elif exchange == 'bittrex':
        return bittrex_transactions()
    elif exchange == 'bithumb':
        return bithumb_transactions()
    elif exchange == 'other':
        return other_transactions()

def binance_sym(sym):
    syms = {'BCH': 'BCC'}
    if sym in syms.keys():
        return syms[sym]
    else:
        return sym

def normalize_sym(sym):
    syms = {'XETH': 'ETH', 'XXBT': 'BTC', 'BCC': 'BCH'}
    if sym in syms.keys():
        return syms[sym]
    else:
        return sym

def normalize_txtype(txtype):
    if txtype in ['buy', 'sell', 'match', 'LIMIT_SELL', 'LIMIT_BUY', 'trade', "BUY", "SELL"]:
        return "trade"
    elif txtype in ['deposit', 'transfer', 'send', 'fiat_deposit', 'fiat_withdrawal', 'exchange_deposit', 'withdraw', 'withdrawal', 'exchange_withdrawal']:
        return "transfer"
    elif txtype in ['fee', 'rebate', 'commission', 'stolen']:
        return "fee"
    elif txtype in ['gift', 'spent']:
        return "spent"
    else:
        raise ValueError(f"no such txtype {txtype}")

def get_all_transactions():
    transactions = []
    exchs = ['gdax', 'coinbase', 'binance', 'kraken', 'bittrex', 'bithumb', 'other']
    for ex in exchs:
        if os.path.exists(f'{ex}.pickle'):
            print(f'unpickling {ex}')
            with open(f'{ex}.pickle', 'rb') as f:
                t = pickle.load(f)
        else:
            print(f'loading {ex}')
            t = get_transactions(ex)
            with open(f'{ex}.pickle', 'wb') as f:
                pickle.dump(t, f)
        transactions += t
    return transactions

def gdax_price(market, ts):
    c = gdax.AuthenticatedClient(apikeys.gdax['apiKey'], apikeys.gdax['secret'], apikeys.gdax['password'])
    data = c.get_product_historic_rates(market, start=ts.isoformat(), end=(ts+timedelta(minutes=1)).isoformat())
    sleep(0.5)
    print(f"gdax price {market} {data[0][1]}")
    return data[0][1]

def binance_price(market, ts):
    c = binance.client.Client(apikeys.binance['apiKey'], apikeys.binance['secret'])
    st=ts.replace(second=0, microsecond=0)
    et=st + timedelta(minutes=1)
    sleep(0.5)
    data = c.get_klines(symbol=market, interval='1m', startTime=int(st.timestamp())*1000, endTime=int(et.timestamp())*1000)
    print(f"binance price {market} {data[0][3]}")
    return data[0][3]

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
        usd_price = Decimal(gdax_price(f"{sym1}-USD", ts))
        sym2_price = usd_price * abs(amt1) / abs(amt2)
        print(f"{sym1} ${usd_price} {sym2} ${sym2_price} = {usd_price} * {abs(amt1)} / {abs(amt2)}")
        return usd_price, sym2_price
    if sym2 in ["BTC", "LTC", "ETH"]:
        usd_price = Decimal(gdax_price(f"{sym2}-USD", ts))
        sym1_price = usd_price * abs(amt2) / abs(amt1)
        print(f"{sym1} ${sym1_price} = {usd_price} * {abs(amt1)} / {abs(amt2)} {sym2} ${usd_price}")
        return sym1_price, usd_price
    if sym1 in ["KRW"]:
        return Decimal(1.0) / Decimal(1160.0), Decimal(1160.0) / Decimal(1.0)
    if sym2 in ["KRW"]:
        return Decimal(1160.0) / Decimal(1.0), Decimal(1.0) / Decimal(1160.0)
    raise ValueError(f"can't get exchange rate for {a} {b} {ts}")


def match_trades():
    transactions = get_all_transactions()
    opentx = defaultdict(list)
    trades = []
    assets = {}

    for t in sorted(transactions):
        date = t[0]
        if date.month > 11:
            break
        ledger = t[1]
        txtype = normalize_txtype(t[2])
        sym = normalize_sym(t[3])
        amount = t[4]
        if sym not in assets:
            assets[sym] = AssetLedger(sym)
        if txtype == "transfer":
            assets[sym].transfer(amount)
        if txtype == "trade":
            print(f"{date.ctime()} looking for {amount:0.2f} {sym} from {ledger} in {opentx[sym]}")
            newtxlist = []
            if opentx[ledger]:
                opentxlist = deepcopy(opentx[ledger])
                matched = False
                for p in opentxlist:
                    o_date, o_sym, o_amount = p
                    if matched:
                        newtxlist.append([o_date, o_sym, o_amount])
                        continue
                    opposite = False
                    if (o_amount > 0 and amount < 0) or (o_amount < 0 and amount > 0):
                        opposite = True
                    near_date = False
                    if (max(o_date, date) - min(o_date, date)) < timedelta(seconds=1):
                        near_date = True
                    if o_sym != sym and opposite and near_date:
                        trades.append([o_date, ledger, sym, amount, o_sym, o_amount])
                        sym_usd, o_sym_usd = get_usd_for_pair((sym, amount), (o_sym, o_amount), o_date)
                        print(f"matched: {o_date.ctime()} {ledger} {sym} {amount:0.2f} price {sym_usd:0.2f} <-> {o_sym} {o_amount:0.2f} price {o_sym_usd:0.2f}")

                        assets[sym].trade(amount, sym_usd)
                        assets[o_sym].trade(o_amount, o_sym_usd)
                        matched = True
                    else:
                        newtxlist.append([o_date, o_sym, o_amount])
                if not matched:
                    print(f"no match found, adding to open tx list")
                    newtxlist.append([date, sym, amount])
            else:
                print(f"no open tx of {ledger}")
                newtxlist.append([date, sym, amount])
            opentx[ledger] = newtxlist
    print("unclosed tx:")
    pprint(opentx)
    print("trades")
    pprint(trades)


def calc_transfer_fees(tolerance=0.01):
    tolerance = Decimal(tolerance)
    transactions = get_all_transactions()
    balance = defaultdict(Decimal)
    opentx = defaultdict(list)

    for t in sorted(transactions):
        date = t[0]
        ledger = t[1]
        txtype = normalize_txtype(t[2])
        sym = normalize_sym(t[3])
        amount = t[4]
        if txtype == "transfer":
            print(f"{date.ctime()} looking for {amount:0.2f} {sym} from {ledger} in {opentx[sym]}")
            newtxlist = []
            if opentx[sym]:
                opentxlist = deepcopy(opentx[sym])
                matched = False
                for p in opentxlist:
                    o_date, o_ledger, o_amount = p
                    if matched:
                        newtxlist.append([o_date, o_ledger, o_amount])
                        continue
                    diff = Decimal(abs(amount + o_amount))
                    under1 = Decimal(abs(amount)) * tolerance
                    under2 = Decimal(abs(o_amount)) * tolerance
                    if o_ledger != ledger and diff < under1 and diff < under2:
                        network_fee = abs(abs(o_amount) - abs(amount))
                        print(f"matched: diff {diff:0.4f} {o_date.ctime()} {o_ledger} {o_amount:0.2f} fee {network_fee}")
                        matched = True
                    else:
                        newtxlist.append([o_date, o_ledger, o_amount])
                if not matched:
                    print(f"no match found, adding to open tx list")
                    newtxlist.append([date, ledger, amount])
            else:
                print(f"no open tx of {sym}")
                newtxlist.append([date, ledger, amount])
            opentx[sym] = newtxlist
    print("unclosed tx:")
    pprint(opentx)


def transaction_trial_balance(until=None):
    if not until:
        until=datetime.now().replace(tzinfo=dateutil.tz.tz.tzlocal())
    transactions = get_all_transactions()
    taccounts = defaultdict(lambda: defaultdict(Decimal))
    tbalance = defaultdict(Decimal)
    traccounts = defaultdict(lambda: defaultdict(Decimal))
    trbalance = defaultdict(Decimal)
    accounts = defaultdict(lambda: defaultdict(Decimal))
    balance = defaultdict(Decimal)
    overall_transfer_accounts = defaultdict(lambda: defaultdict(Decimal))
    overall_transfer_balance = defaultdict(Decimal)
    last_date = None
    for t in sorted(transactions):
        date = t[0]
        if date > until:
            break
        if not last_date:
            last_date = date
        ledger = t[1]
        txtype = normalize_txtype(t[2])
        sym = normalize_sym(t[3])
        amount = t[4]

        if date - timedelta(hours=12) > last_date:
            pt = prettytable.PrettyTable(['sym']+sorted(taccounts.keys()))
            bt = prettytable.PrettyTable(["sym","balance"])
            for tsym in sorted(tbalance.keys()):
                pt.add_row([tsym]+[f'{taccounts[exch][tsym]:0.2f}' if tsym in taccounts[exch] else 'None' for exch in sorted(taccounts.keys())])
                bt.add_row([tsym,f'{tbalance[tsym]:0.2f}'])
            print("transfers")
            print(pt)
            print(bt)
            pt = prettytable.PrettyTable(['sym']+sorted(traccounts.keys()))
            for tsym in sorted(trbalance.keys()):
                pt.add_row([tsym]+[f'{traccounts[exch][tsym]:0.2f}' if tsym in traccounts[exch] else 'None' for exch in sorted(traccounts.keys())])
            print("trades")
            print(pt)
            tbalance = defaultdict(Decimal)
            taccounts = defaultdict(lambda: defaultdict(Decimal))
            trbalance = defaultdict(Decimal)
            traccounts = defaultdict(lambda: defaultdict(Decimal))
            pt = prettytable.PrettyTable(['sym']+sorted(accounts.keys()))
            for tsym in sorted(balance.keys()):
                pt.add_row([tsym]+[f'{accounts[exch][tsym]:0.2f}' if tsym in accounts[exch] else 'None' for exch in sorted(accounts.keys())])
            print("balance")
            print(pt)
        last_date = date
        if amount > 0:
            print(f"{date.ctime()} {amount:0.2f} {sym} -> {ledger} {txtype}")
        else:
            print(f"{date.ctime()} {amount:0.2f} {sym} <- {ledger} {txtype}")
        if txtype == "transfer":
            tbalance[sym] += amount
            taccounts[ledger][sym] += amount
            overall_transfer_accounts[ledger][sym] += amount
            overall_transfer_balance[sym] += amount
        else:
            trbalance[sym] += amount
            traccounts[ledger][sym] += amount
        accounts[ledger][sym] += amount
        balance[sym] += amount

    pt = prettytable.PrettyTable(['sym','amt'])
    for tsym in sorted(overall_transfer_balance.keys()):
        pt.add_row([tsym, overall_transfer_balance[tsym]])
    print("overall transfer balance")
    print(pt)

    pt = prettytable.PrettyTable(['sym']+sorted(overall_transfer_accounts.keys()))
    for tsym in sorted(overall_transfer_balance.keys()):
        pt.add_row([tsym]+[f'{overall_transfer_accounts[exch][tsym]:0.2f}' if tsym in overall_transfer_accounts[exch] else 'None' for exch in sorted(overall_transfer_accounts.keys())])
    print("overall transfer balance by account")
    print(pt)

    pt = prettytable.PrettyTable(['sym']+sorted(accounts.keys()))
    for tsym in sorted(balance.keys()):
        pt.add_row([tsym]+[f'{accounts[exch][tsym]:0.2f}' if tsym in accounts[exch] else 'None' for exch in sorted(accounts.keys())])
    print("final balance")
    print(pt)


if __name__ == '__main__':
    if sys.argv[1] == "trades":
        match_trades()
    elif sys.argv[1] == "balance":
        transaction_trial_balance()
    elif sys.argv[1] == "bittrex":
        for t in bittrex_transactions():
            pprint(t)
            input("press enter")
    elif sys.argv[1] == "tx":
        calc_transfer_fees()


