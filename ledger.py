#!/usr/bin/env python

from decimal import Decimal
from datetime import timedelta
from collections import defaultdict
from exchanges import get_usd_for_pair
from itertools import permutations


class AssetLedgerEntry(object):
    def __init__(self, sym=None, amount=None, date=None, exchange=None, txtype=None):
        self.sym = sym
        self.exchange = exchange
        self.amount = amount
        self.date = date
        self.txtype = txtype

    def __repr__(self):
        return f"AssetLedgerEntry {self.date.ctime()} {self.exchange} {self.txtype} {self.amount:0.2f}{self.sym}"

    def __str__(self):
        return self.__repr__()

class AssetLedger(object):
    """base class for AssetTradeLedger and AssetTransferLedger"""
    def __init__(self, amount_tolerance=Decimal(0.05), time_tolerance=timedelta(seconds=2)):
        """tolerance = allowed difference in ratio to detect the
        transaction as the same"""
        self.tx = []
        self.amount_tolerance = amount_tolerance
        self.time_tolerance = time_tolerance

    def can_resolve(self):
        if len(self.tx) % 2 != 0:
            return False
        syms = set([tx.sym for tx in self.tx])

        pos = list(filter(lambda x: x.amount > 0, self.tx))
        neg = list(filter(lambda x: x.amount < 0, self.tx))
        return len(pos) > 0 and len(neg) > 0 and len(pos) == len(neg)

    def order_tx(self):
        return sorted(self.tx, key=lambda x: abs(x.amount))

    def __repr__(self):
        return f"{type(self).__name__}(tx={self.order_tx()})"

    def __str__(self):
        return self.__repr__()


class AssetTransferMatcher(AssetLedger):
    """Attempts to match a list ef transfers to what makes the most sense
    Closest time-wise ordered by ascending value.
    """

    def resolve(self):
        newtx = []
        matched = []
        if len(self.tx) < 2:
            return len(self.tx)
        for a, b in permutations(sorted(self.tx, key=lambda x: abs(x.amount)), r=2):
            amount_delta = abs((a.amount + b.amount) / max(abs(a.amount), abs(b.amount)))
            if a.sym == b.sym and a.exchange != b.exchange and amount_delta < self.amount_tolerance and (b,a) not in matched:
                matched.append((a,b))
                if a.amount > b.amount:
                    src = b
                    dst = a
                else:
                    src = a
                    dst = b

                timediff = dst.date - src.date
                print(f"matched transfer: {src.date.ctime()} {amount_delta*100:0.2f}% {timediff} {src.amount:0.2f} {src.sym} {src.exchange} -> {dst.amount:0.2f} {dst.sym} {dst.exchange}")
                if len(matched) == len(self.tx):
                    break
        for a, b in matched:
            self.tx = list(filter(lambda x: x not in (a,b), self.tx))
        return len(self.tx)

class AssetTradeMatcher(AssetLedger):
    """Attempts to match a list ef trades to what makes the most sense
    Closest time-wise ordered by ascending value (distance from zero)
    Raises an error if the lists are different sizes.
    """
    def resolve(self, costbasis):
        if not self.can_resolve():
            return len(self.tx)
        syms = list(set([tx.sym for tx in self.tx]))
        sd = {}
        for sym in syms:
            sd[sym] = sorted([tx for tx in self.tx if tx.sym == sym], key=lambda x: abs(x.amount))

        newtx = []
        for a, b in zip(sd[syms[0]], sd[syms[1]]):
            time_diff = max(a.date, b.date) - min(a.date, b.date)
            if time_diff < self.time_tolerance:
                a_usd, b_usd = get_usd_for_pair((a.sym, a.amount), (b.sym, b.amount), a.date)
                print(f"matched trade: {a.date.ctime()} {time_diff} {a.amount:0.2f} {a.sym} ${a_usd:0.2f} <-> {b.amount:0.2f} {b.sym} ${b_usd:0.2f}")

                costbasis[a.sym].trade(a.amount, a_usd, a.date)
                costbasis[b.sym].trade(b.amount, b_usd, b.date)
            else:
                newtx.append(a)
                newtx.append(b)
        self.tx = newtx
        return len(self.tx)

class AssetCostBasis(object):
    def __init__(self, sym):
        self.balance = Decimal(0)
        self.usd_avg_cost_basis = Decimal(0)
        self.sym = sym
        if sym == "USD":
            self.usd_avg_cost_basis = Decimal(1.0)

    def trade(self, amount, usd_unit_price, date):
        if amount > 0:
            return self.buy(amount, usd_unit_price, date)
        else:
            return self.sell(amount, usd_unit_price, date)

    def buy(self, amount, usd_unit_price, date):
        """units, usd_price per 1 unit"""
        self.balance += amount
        if self.usd_avg_cost_basis:
            new_usd_price = ((self.usd_avg_cost_basis * self.balance) + (amount * usd_unit_price)) / (self.balance + amount)
        else:
            new_usd_price = usd_unit_price
        print(f"{date.ctime()} buy {abs(amount):0.2f} {self.sym} new c/b ${new_usd_price:0.2f} balance {self.balance:0.2f}")
        self.usd_avg_cost_basis = new_usd_price

    def sell(self, amount, usd_unit_price, date):
        if abs(amount) > self.balance:
            print(f"{date.ctime()} WARNING! sell amount {amount:0.2f} exceeds balance {self.balance:0.2f} of {self.sym}")
        profitloss = (abs(amount) * usd_unit_price) - (abs(amount) * self.usd_avg_cost_basis)
        self.balance += amount
        print(f"{date.ctime()} sell {abs(amount):0.2f} {self.sym} p/l ${profitloss:0.2f} balance {self.balance:0.2f}")

    def transfer(self, amount, date):
        if self.balance > 0 and amount + self.balance < 0:
            print(f"{date.ctime()} WARNING! transfer amount {amount:0.2f} exceeds balance {self.balance:0.2f} of {self.sym}")
        self.balance += amount
        #print(f"{date.ctime()} CostBasisTransfer {self.sym} += {amount:0.2f} -> {self.balance:0.2f}")

    def __str__(self):
        return f"{type(self).__name__}(balance={self.balance:0.2f}, ${self.usd_avg_cost_basis:0.2f})"

    def __repr__(self):
        return self.__str__()
