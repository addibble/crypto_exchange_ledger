#!/usr/bin/env python

from decimal import Decimal
from datetime import timedelta
from collections import defaultdict
from exchanges import get_usd_for_pair
from itertools import permutations
from exchanges import get_current_usd


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
        # TODO delete?
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


class AssetBalance(object):
    def __init__(self, sym):
        self.sym = sym
        self.balance = Decimal(0)

    def __repr__(self):
        return f"AssetBalance({self.sym}, {self.balance:0.2f})"

class AssetTransferMatcher(AssetLedger):
    """Attempts to match a list ef transfers to what makes the most sense
    Closest time-wise ordered by ascending value.
    """

    def resolve(self):
        network_fee = Decimal(0)
        newtx = []
        matched = []
        if len(self.tx) < 2:
            return len(self.tx), network_fee
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
                fee = AssetLedgerEntry(sym=src.sym, amount=(dst.amount + src.amount), date=src.date)
                network_fee += Decimal(get_current_usd(dst, ts=src.date)) * fee.amount
                #print(f"matched transfer: {src.date.ctime()} {amount_delta*100:0.2f}% {timediff} {src.amount:0.2f} {src.sym} {src.exchange} -> {dst.amount:0.2f} {dst.sym} {dst.exchange} fee {fee.amount:0.5f}")

                if len(matched) == len(self.tx):
                    break
        for a, b in matched:
            self.tx = list(filter(lambda x: x not in (a,b), self.tx))
        return len(self.tx), network_fee

class AssetTradeMatcher(AssetLedger):
    """Attempts to match a list ef trades to what makes the most sense
    Closest time-wise ordered by ascending value (distance from zero)
    Raises an error if the lists are different sizes.
    """
    def resolve(self, costbasis):
        profit_loss = Decimal(0)
        if not self.can_resolve():
            return len(self.tx), Decimal(0)
        syms = list(set([tx.sym for tx in self.tx]))
        sd = {}
        for sym in syms:
            sd[sym] = sorted([tx for tx in self.tx if tx.sym == sym], key=lambda x: abs(x.amount))

        newtx = []
        for a, b in zip(sd[syms[0]], sd[syms[1]]):
            time_diff = max(a.date, b.date) - min(a.date, b.date)
            if time_diff < self.time_tolerance:
                a_usd, b_usd = get_usd_for_pair((a.sym, a.amount), (b.sym, b.amount), a.date)
                #print(f"matched trade: {a.date.ctime()} {time_diff} {a.amount:0.2f} {a.sym} ${a_usd:0.2f} <-> {b.amount:0.2f} {b.sym} ${b_usd:0.2f}")

                profit_loss += costbasis[a.sym].trade(a.amount, a_usd, a.date)
                profit_loss += costbasis[b.sym].trade(b.amount, b_usd, b.date)
            else:
                newtx.append(a)
                newtx.append(b)
        self.tx = newtx
        return len(self.tx), profit_loss

class AssetCostBasis(object):
    def __init__(self, sym):
        self.balance = Decimal(0)
        self.usd_avg_cost_basis = Decimal(0)
        self.sym = sym
        self.profit_loss = Decimal(0)
        if sym == "USD":
            self.usd_avg_cost_basis = Decimal(1.0)

    def trade(self, amount, usd_unit_price, date, txtype=None):
        if amount > 0:
            if not txtype:
                txtype="buy"
            return self.buy(amount, usd_unit_price, date, txtype=txtype)
        else:
            if not txtype:
                txtype="sell"
            return self.sell(amount, usd_unit_price, date, txtype=txtype)

    def buy(self, amount, usd_unit_price, date, txtype="buy"):
        """units, usd_price per 1 unit"""
        if self.usd_avg_cost_basis:
            new_cost_basis = ((self.usd_avg_cost_basis * self.balance) + (amount * usd_unit_price)) / (self.balance + amount)
        else:
            new_cost_basis = usd_unit_price
        self.balance += amount
        #print(f"{date.ctime()} buy {abs(amount):0.2f} {self.sym} new c/b ${new_cost_basis:0.2f} balance {self.balance:0.2f}")
        if not self.sym == "USD":
            print(f"{date.ctime()},{txtype},{self.sym},{amount:0.3f},{(new_cost_basis*amount):0.2f},{new_cost_basis:0.2f},0,{self.balance:0.3f}")
        self.usd_avg_cost_basis = new_cost_basis
        return Decimal(0)

    def sell(self, amount, usd_unit_price, date, txtype="sell"):
        # if abs(amount) > self.balance:
            # print(f"{date.ctime()} WARNING! sell amount {amount:0.2f} exceeds balance {self.balance:0.2f} of {self.sym}")
        profitloss = (abs(amount) * usd_unit_price) - (abs(amount) * self.usd_avg_cost_basis)
        self.profit_loss += profitloss
        self.balance += amount
        #print(f"{date.ctime()} sell {abs(amount):0.2f} {self.sym} p/l ${profitloss:0.2f} balance {self.balance:0.2f}")
        if not self.sym == "USD":
            print(f"{date.ctime()},{txtype},{self.sym},{amount:0.3f},{(usd_unit_price*amount):0.2f},{self.usd_avg_cost_basis:0.2f},{profitloss:0.2f},{self.balance:0.3f}")
        return Decimal(profitloss)

    def transfer(self, amount, date):
        # if self.balance > 0 and amount + self.balance < 0:
            # print(f"{date.ctime()} WARNING! transfer amount {amount:0.2f} exceeds balance {self.balance:0.2f} of {self.sym}")
        self.balance += amount
        #print(f"{date.ctime()} CostBasisTransfer {self.sym} += {amount:0.2f} -> {self.balance:0.2f}")

    def __str__(self):
        return f"{type(self).__name__}(balance={self.balance:0.2f}, ${self.usd_avg_cost_basis:0.2f})"

    def __repr__(self):
        return self.__str__()
