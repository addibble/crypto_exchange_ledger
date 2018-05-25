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
from exchanges import get_all_transactions, get_usd_for_pair, normalize_txtype, normalize_sym, get_current_usd
from ledger import AssetTradeMatcher, AssetTransferMatcher, AssetCostBasis, AssetLedgerEntry, AssetBalance


class keydefaultdict(defaultdict):
    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        else:
            ret = self[key] = self.default_factory(key)
        return ret


def do_resolve(tradematchers, transfermatchers, costbasis):
    profit_loss = Decimal(0)
    newtradematchers = defaultdict(AssetTradeMatcher)
    for exch, tm in tradematchers.items():
        result, pl = tm.resolve(costbasis)
        profit_loss += pl
        if result > 0:
            newtradematchers[exch] = tm
            #pprint(newtradematchers[exch])
        #print(f"remaining unresolved trades {exch}: {result}")
    newtransfermatchers = defaultdict(AssetTransferMatcher)
    for sym, tm in transfermatchers.items():
        result, pl = tm.resolve()
        profit_loss += pl
        if result > 0:
            newtransfermatchers[sym] = tm
            #pprint(newtransfermatchers[sym])
        #print(f"remaining unresolved transfers {sym}: {result}")
    # input("------press any key-----")
    return newtradematchers, newtransfermatchers, costbasis, profit_loss

def match_trades(cutoff_date):
    if not cutoff_date:
        cutoff_date = datetime.now().replace(tzinfo=dateutil.tz.tz.tzlocal)
    transactions = get_all_transactions()
    prev_date = None
    tradematchers = defaultdict(AssetTradeMatcher)
    transfermatchers = defaultdict(AssetTransferMatcher)
    costbasis = keydefaultdict(AssetCostBasis)
    exch_balance = defaultdict(lambda: keydefaultdict(AssetBalance))
    deposits = Decimal(0)
    profit_loss = Decimal(0)

    for t in sorted(transactions):
        entry = AssetLedgerEntry(date=t[0], exchange=t[1], txtype=normalize_txtype(t[2]), sym=normalize_sym(t[3]), amount=t[4])
        if entry.date > cutoff_date:
            break
        if not prev_date:
            prev_date = entry.date

        exch_balance[entry.exchange][entry.sym].balance += entry.amount
        if entry.txtype == "gift":
            #print(f"{entry.date.ctime()} gift subtracting {entry.amount:0.2f} {entry.sym} from CB ({entry.exchange})")
            costbasis[entry.sym].transfer(entry.amount, entry.date)
            if entry.exchange == "bofa":
                deposits += entry.amount
        elif entry.txtype == "transfer":
            if entry.exchange == "bofa":
                deposits += entry.amount
            else:
                costbasis[entry.sym].transfer(entry.amount, entry.date)
            transfermatchers[entry.sym].tx.append(entry)
        elif entry.txtype in ["fee"]:
            usd_unit_price = Decimal(get_current_usd(entry, ts=entry.date))
            profit_loss += costbasis[entry.sym].trade(entry.amount, usd_unit_price, entry.date, txtype="exchange_fee")
        elif entry.txtype in ["stolen"]:
            usd_price = get_current_usd(entry, ts=entry.date) * entry.amount
            costbasis[entry.sym].transfer(entry.amount, entry.date)
            profit_loss += usd
        elif entry.txtype == "trade":
            tradematchers[entry.exchange].tx.append(entry)
        else:
            print("unknown txtype {entry.txtype} for {entry}")

        if entry.date - prev_date > timedelta(seconds=10):
            tradematchers, transfermatchers, costbasis, pl = do_resolve(tradematchers, transfermatchers, costbasis)
            profit_loss += pl
    tradematchers, transfermatchers, costbasis, pl = do_resolve(tradematchers, transfermatchers, costbasis)
    profit_loss += pl
    #pprint(tradematchers)
    #pprint(transfermatchers)
    #pprint(exch_balance)
    return costbasis, deposits, profit_loss


if __name__ == '__main__':
    # TODO add date range args for date range report
    if sys.argv[1] == "trades":
        costbasis,deposits,profit_loss=match_trades(datetime(year=2018, month=1, day=1, hour=0, minute=0, second=0, tzinfo=dateutil.tz.tz.tzlocal()))
        #costbasis,deposits,profit_loss=match_trades(datetime.now().replace(tzinfo=dateutil.tz.tz.tzlocal()))
        pprint(costbasis)
        if len(sys.argv) > 2:
            totalusd=0
            print(f"total cost basis: {sum(map(lambda x: x.balance, costbasis.values()))}")
            print(f"total deposits: {deposits}")
            for sym, cb in costbasis.items():
                usd = cb.balance * Decimal(get_current_usd(cb))
                totalusd += usd
                print(f"{sym} {cb.balance} ${usd:0.2f}")
            print(f"${totalusd:0.2f} current value")
            print(f"realized p/l: ${profit_loss:0.2f}")

