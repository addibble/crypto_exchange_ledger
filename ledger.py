from decimal import Decimal
from datetime import timedelta
from collections import defaultdict
from exchanges import get_usd_for_pair

class AssetLedgerEntry(object):
    def __init__(self, sym=None, amount=None, date=None, exchange=None):
        self.sym = sym
        self.exchange = exchange
        self.amount = amount
        self.date = date

    def __repr__(self):
        return f"{self.sym} {self.exchange} {self.amount:0.2f} {self.date.ctime()}"
    def __str__(self):
        return self.__repr__()

class AssetLedger(object):
    """base class for AssetTradeLedger and AssetTransferLedger"""
    def __init__(self, amount_tolerance=Decimal(0.01), date_tolerance=timedelta(seconds=1)):
        """tolerance = allowed difference in ratio to detect the
        transaction as the same"""
        self.tx = []
        self.amount_tolerance = amount_tolerance
        self.date_tolerance = date_tolerance

    def can_resolve(self):
        syms = set([tx.sym for tx in self.tx])
        if len(syms) == 2 and len(self.tx) % 2 == 0:
            pos = list(filter(lambda x: x.amount > 0, self.tx))
            neg = list(filter(lambda x: x.amount < 0, self.tx))
            if len(pos) == len(neg):
                return True
        print(f"tx {self.tx}")
        return False

class AssetTransferMatcher(object):
    """Attempts to match a list ef transfers to what makes the most sense
    Closest time-wise ordered by ascending value.
    """

    def resolve(self):
        if not self.can_resolve():
            return False
        syms = list(set([tx.sym for tx in self.tx]))
        sd = {}
        for sym in syms:
            sd[sym] = sorted([tx for tx in self.tx if tx.sym == sym], key=lambda x: abs(x.amount))

        for p in zip(sd[syms[0]], sd[syms[1]]):
            a,b=p
            near_date = False
            td = max(a.date, b.date) - min(a.date, b.date)
            if td < timedelta(seconds=1):
                near_date = True
                a_usd, b_usd = get_usd_for_pair((a.sym, a.amount), (b.sym, b.amount), a.date)
                print(f"matched: {a.date.ctime()} {td} {a.amount:0.2f} {a.sym} ${a_usd:0.2f} <-> {b.amount:0.2f} {b.sym} ${b_usd:0.2f}")

                costbasis[a.sym].trade(a.amount, a_usd, a.sym)
                costbasis[b.sym].trade(b.amount, b_usd, b.sym)
        self.tx = []

    def __repr__(self):
        return "AssetTradeMatcher("+",".join([str(x) for x in self.tx])+")"

    def __str__(self):
        return self.__repr__()

class AssetTradeMatcher(object):
    """Attempts to match a list ef trades to what makes the most sense
    Closest time-wise ordered by ascending value (distance from zero)
    Raises an error if the lists are different sizes.
    """
    def __init__(self):
        self.tx = []

    def can_resolve(self):
        syms = set([tx.sym for tx in self.tx])
        if len(syms) > 2:
            raise ValueError("can't resolve trades, more than 2 symbols: {self.tx}")
        if len(syms) == 2 and len(self.tx) % 2 == 0:
            pos = list(filter(lambda x: x.amount > 0, self.tx))
            neg = list(filter(lambda x: x.amount < 0, self.tx))
            if len(pos) == len(neg):
                return True
        print(f"tx {self.tx}")
        return False

    def resolve(self, costbasis):
        if not self.can_resolve():
            return False
        syms = list(set([tx.sym for tx in self.tx]))
        sd = {}
        for sym in syms:
            sd[sym] = sorted([tx for tx in self.tx if tx.sym == sym], key=lambda x: abs(x.amount))

        for p in zip(sd[syms[0]], sd[syms[1]]):
            a,b=p
            near_date = False
            td = max(a.date, b.date) - min(a.date, b.date)
            if td < timedelta(seconds=1):
                near_date = True
                a_usd, b_usd = get_usd_for_pair((a.sym, a.amount), (b.sym, b.amount), a.date)
                print(f"matched: {a.date.ctime()} {td} {a.amount:0.2f} {a.sym} ${a_usd:0.2f} <-> {b.amount:0.2f} {b.sym} ${b_usd:0.2f}")

                costbasis[a.sym].trade(a.amount, a_usd, a.sym)
                costbasis[b.sym].trade(b.amount, b_usd, b.sym)
        self.tx = []

    def __repr__(self):
        return "AssetTradeMatcher("+",".join([str(x) for x in self.tx])+")"

    def __str__(self):
        return self.__repr__()

class AssetCostBasis(object):
    def __init__(self):
        self.balance = Decimal(0)
        self.usd_avg_cost_basis = Decimal(0)

    def trade(self, amount, usd_unit_price, sym):
        if amount > 0:
            self.buy(amount, usd_unit_price, sym)
        else:
            self.sell(amount, usd_unit_price, sym)

    def buy(self, amount, usd_unit_price, sym):
        """units, usd_price per 1 unit"""
        self.balance += amount
        if self.usd_avg_cost_basis:
            new_usd_price = ((self.usd_avg_cost_basis * self.balance) + (amount * usd_unit_price)) / (self.balance + amount)
        else:
            new_usd_price = usd_unit_price
        print(f'{sym} new cost basis ${new_usd_price:0.2f}')
        self.usd_avg_cost_basis = new_usd_price

    def sell(self, amount, usd_unit_price, sym):
        if abs(amount) > self.balance:
            # raise ValueError(f"sell amount {amount:0.2f} exceeds balance {self.balance:0.2f} of {sym}")
            print(f"sell amount {amount:0.2f} exceeds balance {self.balance:0.2f} of {sym}")
        profitloss = (abs(amount) * usd_unit_price) - (abs(amount) * self.usd_avg_cost_basis)
        self.balance += amount
        print(f"sell {abs(amount):0.2f} {sym} p/l ${profitloss:0.2f} balance {self.balance:0.2f}")

    def transfer(self, amount, sym):
        self.balance += amount
        print(f"Transfer {sym} += {amount:0.2f} -> {self.balance:0.2f}")

    def __repr__(self):
        return f"AssetCostBasis balance={self.balance:0.2f} ${self.usd_avg_cost_basis:0.2f}"

tradematchers = defaultdict(AssetTradeMatcher)
transferledger = AssetTransferMatcher()
