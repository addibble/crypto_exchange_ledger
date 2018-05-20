#!/usr/bin/env python

import prettytable
import sys
from time import sleep
from pprint import pprint
from datetime import datetime
from datetime import timedelta
import dateutil.tz
from decimal import Decimal
from collections import defaultdict
from copy import deepcopy
from exchanges import get_all_transactions, get_usd_for_pair, normalize_txtype, normalize_sym
from ledger import AssetTradeMatcher, AssetTransferMatcher, AssetCostBasis, AssetLedgerEntry

tradematchers = defaultdict(AssetTradeMatcher)
transferledger = AssetTransferMatcher()
costbasis = defaultdict(AssetCostBasis)

def match_trades():
    transactions = get_all_transactions()
    prev_date = datetime(year=2017, month=1, day=1, tzinfo=dateutil.tz.tz.tzlocal())

    for t in sorted(transactions):
        date = t[0]
        ledger = t[1]
        txtype = normalize_txtype(t[2])
        sym = normalize_sym(t[3])
        amount = t[4]
        if txtype == "gift":
            pass
        elif txtype == "transfer":
            costbasis[sym].transfer(amount, sym)
        elif txtype == "fee":
            pass
        elif txtype == "trade":
            print(f"trade {date.ctime()} {amount:0.2f} {sym} {ledger}")
            tradematchers[ledger].tx.append(AssetLedgerEntry(sym=sym, date=date, amount=amount))

        if date - prev_date > timedelta(seconds=10):
            for lgr, tm in tradematchers.items():
                if tm.can_resolve():
                    print(f"matching trades on {lgr}")
                    tm.resolve(costbasis)
                else:
                    print(f"can't resolve trades on {lgr} yet")
        pprint(tradematchers)
        pprint(costbasis)

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

def alltx():
    transactions = get_all_transactions()
    for t in sorted(transactions):
        print(f"{t[0].ctime()} {t[1]} {normalize_txtype(t[2])} {normalize_sym(t[3])} {t[4]:0.2f}")

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
    elif sys.argv[1] == "alltx":
        alltx()
    elif sys.argv[1] == "fees":
        calc_transfer_fees()


