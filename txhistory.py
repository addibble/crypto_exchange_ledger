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
from time import sleep
import xcoin_api_client
from datetime import datetime
import dateutil.parser
import dateutil.tz
from decimal import Decimal
from collections import defaultdict
import requests

e = {}

def dp(d):
    x=dateutil.parser.parse(d)
    return addtz(x)

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
 
def bittrex_transactions():
    e['bittrex'] = bittrex.Bittrex(apikeys.bittrex['apiKey'], apikeys.bittrex['secret'])
    dh = e['bittrex'].get_deposit_history()
    # use same algorithm as binance

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
    elif exchange == 'bithumb':
        return bithumb_transactions()
    elif exchange == 'bittrex':
        return bittrex_transactions()

def normalize_sym(sym):
    syms = {'XETH': 'ETH', 'XXBT': 'BTC', 'BCC': 'BCH'}
    if sym in syms:
        return syms[sym]
    else:
        return sym

if __name__ == '__main__':
    print(bithumb_transactions())
    sys.exit(0)
    transactions = []
    exchs = ['gdax', 'coinbase', 'binance', 'kraken']
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

    totals = defaultdict(Decimal)
    accounts = defaultdict(lambda: defaultdict(Decimal))
    credits = defaultdict(Decimal)
    debits = defaultdict(Decimal)
    balance = defaultdict(Decimal)
    for t in sorted(transactions):
        sym = normalize_sym(t[3])
        ledger = t[1]
        amount = t[4]
        date = t[0]
        balance[sym] += amount
        accounts[ledger][sym] += amount
        if amount > 0:
            print(f"{date.ctime()} {amount:0.2f} {sym} -> {ledger} {t[2]}")
        else:
            print(f"{date.ctime()} {amount:0.2f} {sym} <- {ledger} {t[2]}")
        print(accounts)
    pt = prettytable.PrettyTable(['sym']+sorted(accounts.keys()))
    for sym in sorted(balance.keys()):
        pt.add_row([sym]+[f'{accounts[exch][sym]:0.2f}' if sym in accounts[exch] else 'None' for exch in sorted(accounts.keys())])
    print(pt)
