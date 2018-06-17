import unittest
from datetime import datetime
from decimal import Decimal
import ledger

class TestAssetFifoCostBasis(unittest.TestCase):
    def test_tx(self):
        cb = ledger.AssetFifoCostBasis("BTC")
        cb.trade(Decimal(2.0), Decimal(10000), datetime.now())
        assert cb.profit_loss == 0
        cb.trade(Decimal(-1.0), Decimal(20000), datetime.now())
        assert cb.profit_loss == Decimal(10000)
        cb.trade(Decimal(1.0), Decimal(20000), datetime.now())
        assert cb.usd_avg_cost_basis == Decimal(15000)
        cb.trade(Decimal(-1.0), Decimal(20000), datetime.now())
        assert cb.usd_avg_cost_basis == Decimal(20000)
        assert cb.profit_loss == Decimal(20000)

class TestAssetLifoCostBasis(unittest.TestCase):
    def test_tx(self):
        cb = ledger.AssetLifoCostBasis("BTC")
        cb.trade(Decimal(2.0), Decimal(10000), datetime.now())
        assert cb.profit_loss == 0
        cb.trade(Decimal(-1.0), Decimal(20000), datetime.now())
        assert cb.profit_loss == Decimal(10000)
        cb.trade(Decimal(1.0), Decimal(20000), datetime.now())
        assert cb.usd_avg_cost_basis == Decimal(15000)
        cb.trade(Decimal(-1.0), Decimal(20000), datetime.now())
        assert cb.usd_avg_cost_basis == Decimal(10000)
        assert cb.profit_loss == Decimal(10000)

