#!/usr/bin/env python

from decimal import Decimal
from datetime import timedelta
from exchanges import get_usd_for_pair
from itertools import permutations
from exchanges import get_current_usd
from collections import deque


class AssetLedgerEntry(object):

    def __init__(self, sym=None, amount=None, date=None, exchange=None, txtype=None):
        self.sym = sym
        self.exchange = exchange
        self.amount = amount
        self.date = date
        self.txtype = txtype

    def __repr__(self):
        return (
            f"AssetLedgerEntry {self.date.ctime()} {self.exchange} {self.txtype} {self.amount:0.2f}{self.sym}"
        )

    def __str__(self):
        return self.__repr__()


class AssetLedger(object):
    """base class for AssetTradeLedger and AssetTransferLedger"""

    def __init__(
        self, amount_tolerance=Decimal(0.05), time_tolerance=timedelta(seconds=2)
    ):
        """tolerance = allowed difference in ratio to detect the
        transaction as the same"""
        self.tx = []
        self.amount_tolerance = amount_tolerance
        self.time_tolerance = time_tolerance

    def can_resolve(self):
        # TODO delete?
        if len(self.tx) % 2 != 0:
            return False

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

    def resolve(self, costbasis):
        network_fee = Decimal(0)
        matched = []
        if len(self.tx) < 2:
            return len(self.tx)
        for a, b in permutations(sorted(self.tx, key=lambda x: abs(x.amount)), r=2):
            amount_delta = abs(
                (a.amount + b.amount) / max(abs(a.amount), abs(b.amount))
            )
            if (
                a.sym == b.sym
                and a.exchange != b.exchange
                and amount_delta < self.amount_tolerance
                and (b, a) not in matched
            ):
                matched.append((a, b))
                if a.amount > b.amount:
                    src = b
                    dst = a
                else:
                    src = a
                    dst = b

                costbasis[src.sym].fee(dst.amount+src.amount, src.date, txtype="network_fee")

                if len(matched) == len(self.tx):
                    break
        for a, b in matched:
            self.tx = list(filter(lambda x: x not in (a, b), self.tx))
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
            sd[sym] = sorted(
                [tx for tx in self.tx if tx.sym == sym], key=lambda x: abs(x.amount)
            )

        newtx = []
        for a, b in zip(sd[syms[0]], sd[syms[1]]):
            time_diff = max(a.date, b.date) - min(a.date, b.date)
            if time_diff < self.time_tolerance:
                a_usd, b_usd = get_usd_for_pair(
                    (a.sym, a.amount), (b.sym, b.amount), a.date
                )
                # print(f"matched trade: {a.date.ctime()} {time_diff} {a.amount:0.2f} {a.sym} ${a_usd:0.2f} <-> {b.amount:0.2f} {b.sym} ${b_usd:0.2f}")

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
        self.lots = deque()
        self.sym = sym
        self.profit_loss = Decimal(0)
        self.pending_fees = Decimal(0)
        if sym == "USD":
            self.usd_avg_cost_basis = Decimal(1.0)

    def loss(self, amount, date, txtype="loss"):
        profitloss = Decimal(0)
        loss_remaining = amount
        while loss_remaining < 0 and self.lots:
            try:
                lot_amount, lot_usd_price = self.get_tx()
                loss_amount = min(abs(loss_remaining), lot_amount)
                loss_remaining += loss_amount
                if loss_amount != lot_amount:
                    self.insert_tx((lot_amount-loss_amount,lot_usd_price))
                profitloss -= (loss_amount * lot_usd_price)
            except IndexError:
                print(f"IndexError {self} {amount} {date}")
                raise
            self.balance += amount
            self.profit_loss += profitloss
        if loss_remaining < 0:
            profitloss += loss_remaining
            usd_price = get_current_usd(AssetLedgerEntry(sym=self.sym), ts=date)
            print(f"{date.ctime()},{txtype},{self.sym},{abs(amount):0.3f},{profitloss:0.2f}")
            self.balance += loss_remaining
        if self.lots:
            self.usd_avg_cost_basis = sum([a*u for a,u in self.lots]) / sum([a for a, u in self.lots])
        elif self.sym != "USD":
            self.usd_avg_cost_basis = Decimal(0)

        if profitloss:
            print(f"{date.ctime()},{txtype},{self.sym},{abs(amount):0.3f},{profitloss:0.2f}")

    def fee(self, fee_amount, date, txtype=None):
        self.loss(fee_amount, date, txtype="fee")

    def trade(self, amount, usd_unit_price, date, txtype=None):
        if amount > 0:
            if not txtype:
                txtype = "buy"
            pl = self.buy(amount, usd_unit_price, date, txtype=txtype)
        else:
            if not txtype:
                txtype = "sell"
            pl = self.sell(amount, usd_unit_price, date, txtype=txtype)
            if self.sym != "USD":
                print(f"{date.ctime()},{txtype},{self.sym},{abs(amount):0.3f},{pl:0.2f}")
        self.balance += amount
        if not self.sym == "USD" and self.lots:
            self.usd_avg_cost_basis = sum([a*u for a,u in self.lots]) / sum([a for a, u in self.lots])
        else:
            self.usd_avg_cost_basis = Decimal(1)
        return pl

    def buy(self, amount, usd_unit_price, date, txtype="buy"):
        self.lots.append((amount, usd_unit_price))
        total = sum([a for a,u in self.lots])
        avg_cost = sum([a*u for a,u in self.lots]) / total
        self.lots = deque([(total, avg_cost)])
        return Decimal(0)

    def buy_lot(self, amount, usd_unit_price, date, txtype="buy"):
        self.lots.append((amount, usd_unit_price))
        return Decimal(0)

    def sell(self, amount, usd_unit_price, date, txtype="sell"):
        # if abs(amount) > self.balance:
        # print(f"{date.ctime()} WARNING! sell amount {amount:0.2f} exceeds balance {self.balance:0.2f} of {self.sym}")
        profitloss = (abs(amount) * usd_unit_price) - (
            abs(amount) * self.usd_avg_cost_basis
        )
        self.profit_loss += profitloss
        total = sum([a for a,u in self.lots])
        self.lots = deque([(total + amount, self.usd_avg_cost_basis)])
        # print(f"{date.ctime()} sell {abs(amount):0.2f} {self.sym} p/l ${profitloss:0.2f} balance {self.balance:0.2f}")
        return Decimal(profitloss)

    def sell_from_lot(self, amount, usd_unit_price, date, txtype="sell"):
        # if abs(amount) > self.balance:
        # print(f"{date.ctime()} WARNING! sell amount {amount:0.2f} exceeds balance {self.balance:0.2f} of {self.sym}")
        profitloss = Decimal(0)
        if self.sym == "USD":
            return profitloss
        sell_remaining = amount
        while sell_remaining < 0 and self.lots:
            try:
                lot_amount, lot_usd_price = self.get_tx()
                sell_amount = min(abs(sell_remaining), lot_amount)
                sell_remaining += sell_amount
                if sell_amount != lot_amount:
                    self.insert_tx((lot_amount-sell_amount,lot_usd_price))
                profitloss += (sell_amount * usd_unit_price) - (sell_amount * lot_usd_price)
            except IndexError:
                print(f"IndexError {self} {amount} {date}")
                raise

        if sell_remaining < 0:
            usd_price = get_current_usd(AssetLedgerEntry(sym=self.sym), ts=date)
            print(f"WARNING deducting unmatched sale of {sell_remaining:0.2f} {self.sym} from P/L")
            profitloss += Decimal(usd_price) * Decimal(sell_remaining)
            self.balance += sell_remaining

        self.profit_loss += profitloss
        return profitloss

    def transfer(self, amount, date):
        # if self.balance > 0 and amount + self.balance < 0:
        # print(f"{date.ctime()} WARNING! transfer amount {amount:0.2f} exceeds balance {self.balance:0.2f} of {self.sym}")
        self.balance += amount

    def __str__(self):
        return (
            f"{type(self).__name__}(sym={self.sym}, balance={self.balance:0.2f}, CB ${self.usd_avg_cost_basis:0.2f}, profit_loss={self.profit_loss}, lots={self.lots})"
        )

    def __repr__(self):
        return self.__str__()


class AssetFifoCostBasis(AssetCostBasis):
    def insert_tx(self, tx):
        self.lots.appendleft(tx)

    def get_tx(self):
        return self.lots.popleft()

    def sell(self, amount, usd_unit_price, date, txtype="sell"):
        return self.sell_from_lot(amount, usd_unit_price, date, txtype=txtype)

    def buy(self, amount, usd_unit_price, date, txtype="buy"):
        return self.buy_lot(amount, usd_unit_price, date, txtype=txtype)
    

class AssetLifoCostBasis(AssetCostBasis):
    def insert_tx(self, tx):
        self.lots.append(tx)

    def get_tx(self):
        return self.lots.pop()

    def sell(self, amount, usd_unit_price, date, txtype="sell"):
        return self.sell_from_lot(amount, usd_unit_price, date, txtype=txtype)

    def buy(self, amount, usd_unit_price, date, txtype="buy"):
        return self.buy_lot(amount, usd_unit_price, date, txtype=txtype)
