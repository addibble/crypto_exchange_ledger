#!env python

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

e = {}

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
            rec = line.split('\t')
            ts = bithumb_dp(rec[0].replace("\"", ""))
            sym = rec[1]
            order = rec[2]
            qty_coin = Decimal("".join([c for c in rec[3] if c.isdigit() or c == "."]))
            #if krw_price == "-"
            #    krw_price = Decimal("".join([c for c in rec[4] if c.isdigit() or c == "."]))
            qty_krw = Decimal("".join([c for c in rec[5] if c.isdigit() or c == "."]))
            fee = Decimal(0)
            if rec[6] != "-":
                fee = Decimal("".join([c for c in rec[6] if c.isdigit() or c == "."]))
            # still need to check that all of the transaction directions go the right way
            if 'BUY' in order:
                transactions.append([ts, 'bithumb', order, sym, qty_coin, rec])
                transactions.append([ts, 'bithumb', order, "KRW", -qty_krw, rec])
                transactions.append([ts, 'bithumb', "fee", sym, -fee, rec])
            elif 'SELL' in order:
                transactions.append([ts, 'bithumb', order, sym, qty_coin, rec])
                transactions.append([ts, 'bithumb', order, "KRW", -qty_krw, rec])
                transactions.append([ts, 'bithumb', "fee", "KRW", -fee, rec])
            elif 'DEPOSIT' in order:
                transactions.append([ts, 'bithumb', "deposit", sym, qty_coin, rec])
            elif 'WITHDRAWAL' in order:
                transactions.append([ts, 'bithumb', "withdrawal", sym, -qty_coin, rec])
                transactions.append([ts, 'bithumb', "fee", sym, -fee, rec])
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
            ts = dp(rec[7])
            if 'BUY' in order:
                transactions.append([ts, 'bittrex', order, base, -price, rec])
                transactions.append([ts, 'bittrex', order, base, -commission, rec])
                transactions.append([ts, 'bittrex', order, quote, qty, rec])
            elif 'SELL' in order:
                transactions.append([ts, 'bittrex', order, base, price, rec])
                transactions.append([ts, 'bittrex', order, base, -commission, rec])
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
            transactions.append([created, 'coinbase', tx['type'], tx['amount']['currency'], Decimal(tx['amount']['amount']), tx])
            if tx['type'] == "buy" and "Bank of" in tx['details']['payment_method_name']:
                transactions.append([created, 'bofa', tx['type'], tx['native_amount']['currency'], -Decimal(tx['native_amount']['amount']), tx])
            if tx['type'] in ['fiat_deposit', 'fiat_withdrawal']:
                transactions.append([created, 'bofa', tx['type'], tx['native_amount']['currency'], -Decimal(tx['native_amount']['amount']), tx])

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

def normalize_sym(sym):
    syms = {'XETH': 'ETH', 'XXBT': 'BTC', 'BCC': 'BCH'}
    if sym in syms:
        return syms[sym]
    else:
        return sym

def normalize_txtype(txtype):
    if txtype in ['buy', 'sell', 'match', 'LIMIT_SELL', 'LIMIT_BUY', 'trade', "BUY", "SELL"]:
        return "trade"
    elif txtype in ['deposit', 'transfer', 'send', 'fiat_deposit', 'fiat_withdrawal', 'exchange_deposit', 'withdraw', 'withdrawal', 'exchange_withdrawal']:
        return "transfer"
    elif txtype in ['fee', 'rebate', 'commission']:
        return "fee"
    else:
        raise ValueError(f"no such txtype {txtype}")

def get_all_transactions():
    transactions = []
    exchs = ['gdax', 'coinbase', 'binance', 'kraken', 'bittrex', 'bithumb']
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

def transaction_trial_balance():
    transactions = get_all_transactions()
    totals = defaultdict(Decimal)
    accounts = defaultdict(lambda: defaultdict(Decimal))
    taccounts = defaultdict(lambda: defaultdict(Decimal))
    credits = defaultdict(Decimal)
    debits = defaultdict(Decimal)
    balance = defaultdict(Decimal)
    tbalance = defaultdict(Decimal)
    txns = defaultdict(lambda: defaultdict(Decimal))
    credit_ctr = Counter()
    debit_ctr = Counter()
    last_date = None
    for t in sorted(transactions):
        date = t[0]
        if not last_date:
            last_date = date
        if date - timedelta(hours=12) > last_date:
            pt = prettytable.PrettyTable(['sym']+sorted(taccounts.keys()))
            bt = prettytable.PrettyTable(["sym","balance"])
            for sym in sorted(tbalance.keys()):
                pt.add_row([sym]+[f'{taccounts[exch][sym]:0.2f}' if sym in taccounts[exch] else 'None' for exch in sorted(taccounts.keys())])
                bt.add_row([sym,f'{tbalance[sym]:0.2f}'])
            print(pt)
            print(bt)
            tbalance = defaultdict(Decimal)
            taccounts = defaultdict(lambda: defaultdict(Decimal))
            input("press enter")
        last_date = date
        ledger = t[1]
        txtype = normalize_txtype(t[2])
        sym = normalize_sym(t[3])
        amount = t[4]
        balance[sym] += amount
        accounts[ledger][sym] += amount
        txns[txtype][sym] += amount
        if amount > 0:
            credit_ctr[txtype] += 1
            print(f"{date.ctime()} {amount:0.2f} {sym} -> {ledger} {txtype}")
        else:
            debit_ctr[txtype] += 1
            print(f"{date.ctime()} {amount:0.2f} {sym} <- {ledger} {txtype}")
        if txtype == "transfer":
            tbalance[sym] += amount
            taccounts[ledger][sym] += amount
        #print(f'{ledger} {sym} {accounts[ledger][sym]:0.2f}')
    pt = prettytable.PrettyTable(['sym']+sorted(accounts.keys()))
    for sym in sorted(balance.keys()):
        pt.add_row([sym]+[f'{accounts[exch][sym]:0.2f}' if sym in accounts[exch] else 'None' for exch in sorted(accounts.keys())])
    print(pt)
    pprint(credit_ctr)
    pprint(debit_ctr)
    pprint(txns)


if __name__ == '__main__':
    transaction_trial_balance()
