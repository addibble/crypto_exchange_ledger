import unittest
from datetime import datetime
from decimal import Decimal


class TestAssetCostBasis(unittest.TestCase):

    def test_tx(self):
        import ledger

        cb = ledger.AssetCostBasis("BTC")
        cb.trade(Decimal(1.5), Decimal(10000), datetime.now())
        assert cb.balance == Decimal(1.5)
        assert cb.usd_avg_cost_basis == Decimal(10000)
        cb.trade(Decimal(-0.5), Decimal(15000), datetime.now())
        assert cb.balance == Decimal(1.0)
        assert cb.usd_avg_cost_basis == Decimal(10000)
        assert cb.profit_loss == Decimal(2500)
        cb.trade(Decimal(1.0), Decimal(20000), datetime.now())
        assert cb.usd_avg_cost_basis == Decimal(15000)

    def test_tx2(self):
        import ledger

        cb = ledger.AssetCostBasis("BTC")
        cb.trade(Decimal(1.5), Decimal(10000), datetime.now())
        assert cb.balance == Decimal(1.5)
