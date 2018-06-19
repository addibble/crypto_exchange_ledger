#!/usr/bin/env python

import sys
from pprint import pprint
from datetime import datetime
from datetime import timedelta
import dateutil.tz
from decimal import Decimal
from collections import defaultdict
from exchanges import (
    get_all_transactions,
    normalize_txtype,
    normalize_sym,
    get_current_usd,
)
from ledger import (
    AssetTradeMatcher,
    AssetTransferMatcher,
    AssetCostBasis,
    AssetFifoCostBasis,
    AssetLifoCostBasis,
    AssetLedgerEntry,
    AssetBalance,
)


class keydefaultdict(defaultdict):

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        else:
            ret = self[key] = self.default_factory(key)
        return ret


def do_resolve(tradematchers, transfermatchers, costbasis):
    newtradematchers = defaultdict(AssetTradeMatcher)
    for exch, tm in tradematchers.items():
        result = tm.resolve(costbasis)
        if result > 0:
            newtradematchers[exch] = tm
    newtransfermatchers = defaultdict(AssetTransferMatcher)
    for sym, tm in transfermatchers.items():
        result = tm.resolve(costbasis)
        if result > 0:
            newtransfermatchers[sym] = tm
    return newtradematchers, newtransfermatchers, costbasis


def match_trades(cutoff_date=None, reset_pl_date=None, costbasis_class=AssetLifoCostBasis):
    transactions = get_all_transactions()
    prev_date = None
    tradematchers = defaultdict(AssetTradeMatcher)
    transfermatchers = defaultdict(AssetTransferMatcher)
    costbasis = keydefaultdict(costbasis_class)
    exch_balance = defaultdict(lambda: keydefaultdict(AssetBalance))
    deposits = Decimal(0)

    for t in sorted(transactions):
        entry = AssetLedgerEntry(
            date=t[0],
            exchange=t[1],
            txtype=normalize_txtype(t[2]),
            sym=normalize_sym(t[3]),
            amount=t[4],
        )
        if cutoff_date and entry.date > cutoff_date:
            break
        if reset_pl_date and entry.date > reset_pl_date:
            for cbsym, cb in costbasis.items():
                cb.profit_loss = Decimal(0)
            reset_pl_date = None
        if not prev_date:
            prev_date = entry.date

        exch_balance[entry.exchange][entry.sym].balance += entry.amount
        if entry.txtype == "gift":
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
            costbasis[entry.sym].fee(entry.amount, entry.date, txtype="exchange_fee")
        elif entry.txtype in ["loss"]:
            costbasis[entry.sym].loss(entry.amount, entry.date)
        elif entry.txtype == "trade":
            tradematchers[entry.exchange].tx.append(entry)
        else:
            print(f"unknown txtype {entry.txtype} for {entry}")

        if entry.date - prev_date > timedelta(seconds=10):
            tradematchers, transfermatchers, costbasis = do_resolve(
                tradematchers, transfermatchers, costbasis
            )
    tradematchers, transfermatchers, costbasis = do_resolve(
        tradematchers, transfermatchers, costbasis
    )
    if len(tradematchers) > 0:
        print(f"unresolved trades! {tradematchers}")
    if len(transfermatchers) > 0:
        print(f"unresolved transfers! {transfermatchers}")
    return costbasis, deposits


if __name__ == "__main__":
    # TODO add date range args for date range report
    c = None
    if "2017" in sys.argv:
        c = datetime(
                year=2018,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                tzinfo=dateutil.tz.tz.tzlocal(),
            )
    r = None
    if "2018" in sys.argv:
        r = datetime(
                year=2018,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                tzinfo=dateutil.tz.tz.tzlocal(),
                )

    if "fifo" in sys.argv:
        cb_class = AssetFifoCostBasis
    else:
        cb_class = AssetLifoCostBasis

    costbasis, deposits = match_trades(cutoff_date=c, reset_pl_date=r, costbasis_class=cb_class)
    if "detail" in sys.argv:
        totalcb = 0
        currvalue = 0
        profit_loss = 0
        print(f"total deposits: {deposits}")
        for sym, cb in costbasis.items():
            usd = cb.balance * Decimal(get_current_usd(cb, ts=c))
            print(f"{sym} {cb.balance:0.2f} cost_basis ${cb.usd_avg_cost_basis*cb.balance:0.2f} value ${usd:0.2f} P/L ${cb.profit_loss:0.2f}")
            profit_loss += cb.profit_loss
            if sym == "USD":
                continue
            currvalue += usd
            totalcb += cb.usd_avg_cost_basis * cb.balance
        print(f"all crypto ${currvalue:0.2f} current value cost basis ${totalcb:0.2f}")
        print(f"unrealized p/l: ${currvalue-totalcb:0.2f}")
        print(f"realized p/l: ${profit_loss:0.2f}")
