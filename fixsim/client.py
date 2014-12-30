import quickfix
import copy
import uuid
import random
import datetime
import yaml
from twisted.internet import task

from sim import (FixSimError, FixSimApplication, create_fix_version,
                 instance_safe_call, create_logger, IncrementID, load_yaml)


class Subscription(object):
    def __init__(self, symbol):
        super(Subscription, self).__init__()
        self.symbol = symbol
        self.currency = self.symbol.split("/")[0]

    def __repr__(self):
        return "<Subscription %s>" % self.symbol


class Subscriptions(object):
    def __init__(self):
        self.subscriptions = {}

    def add(self, subscription):
        if subscription.symbol in self.subscriptions:
            raise KeyError("Subscription for symbol has already exist")
        self.subscriptions[subscription.symbol] = subscription

    def get(self, symbol):
        subscription = self.subscriptions.get(symbol, None)
        return subscription

    def __iter__(self):
        return self.subscriptions.values().__iter__()


class OrderBook(object):
    def __init__(self):
        self.quotes = []

    def setSnapshot(self, snaphot):
        raise NotImplementedError()

    def __iter__(self):
        return self.quotes.__iter__()

    def get(self, quoteID):
        for quote in self.quotes:
            if quote.id == quoteID:
                return quote

        return None


class IDGenerator(object):
    def __init__(self):
        self._orderID = IncrementID()
        self._reqID = IncrementID()

    def orderID(self):
        return self._orderID.generate()

    def reqID(self):
        return self._reqID.generate()


def create_initiator(client_config, simulation_config):
    def create_subscriptions(instruments):
        result = Subscriptions()

        for instrument in instruments:
            subscription = Subscription(instrument['symbol'])
            result.add(subscription)

        return result

    settings = quickfix.SessionSettings(client_config)
    config = load_yaml(simulation_config)

    fix_version = create_fix_version(config)

    subscriptions = create_subscriptions(config['instruments'])

    logger = create_logger(config)
    subscribe_interval = config.get('subscribe_interval', 1)
    skip_snapshot_chance= config.get('skip_snapshot_chance', 0)
    application = Client(fix_version, logger, skip_snapshot_chance, subscribe_interval, subscriptions)
    storeFactory = quickfix.FileStoreFactory(settings)
    logFactory = quickfix.ScreenLogFactory(settings)
    initiator = quickfix.SocketInitiator(application, storeFactory, settings, logFactory)
    return initiator


class Snapshot(object):
    def __init__(self, symbol):
        self.symbol = symbol
        self.bid = []
        self.ask = []

    def addBid(self, quote):
        self.bid.append(quote)

    def addAsk(self, quote):
        self.ask.append(quote)

    def __repr__(self):
        return "Snapshot %s\n    BID: %s\n    ASK: %s" % (self.symbol, self.bid, self.ask)

class Quote(object):
    BID = '0'
    ASK = '1'

    def __init__(self):
        super(Quote, self).__init__()
        self.side = None
        self.symbol = None
        self.price = None
        self.size = None
        self.id = None

    def __repr__(self):
        return "(%s %s, %s)" % (self.side, str(self.price), str(self.size))


class Client(FixSimApplication):
    MKD_TOKEN = "MKD"

    def __init__(self, fixVersion, logger, skipSnapshotChance, subscribeInterval, subscriptions):
        super(Client, self).__init__(fixVersion, logger)
        # TODO DELETE AFTER COMPLETE
        import quickfix44

        self.fixVersion = quickfix44

        self.skipSnapshotChance = skipSnapshotChance
        self.subscribeInterval = subscribeInterval
        self.subscriptions = subscriptions
        self.orderSession = None
        self.marketSession = None
        self.idGen = IDGenerator()
        self.loop = task.LoopingCall(self.subscribe)
        self.loop.start(self.subscribeInterval, True)

    def onCreate(self, sessionID):
        pass

    def onLogon(self, sessionID):
        sid = str(sessionID)
        # print "ON LOGON sid", sid
        if sid.find(self.MKD_TOKEN) != -1:
            self.marketSession = sessionID
            self.logger.info("MARKET SESSION %s", self.marketSession)
        else:
            self.orderSession = sessionID
            self.logger.info("ORDER SESSION %s", self.orderSession)

    def onLogout(self, sessionID):
        # print "ON LOGOUT"
        return

    def toAdmin(self, sessionID, message):
        # print "TO ADMIN", message
        return

    def fromAdmin(self, sessionID, message):
        # print "FROM ADMIN"
        return

    def toApp(self, sessionID, message):
        # print "TO APP"
        return

    def subscribe(self):
        if self.marketSession is None:
            self.logger.info("Market session is none, skip subscribing")
            return

        for subscription in self.subscriptions:
            message = self.fixVersion.MarketDataRequest()
            message.setField(quickfix.MDReqID(self.idGen.reqID()))
            message.setField(quickfix.SubscriptionRequestType(quickfix.SubscriptionRequestType_SNAPSHOT_PLUS_UPDATES))
            message.setField(quickfix.MDUpdateType(quickfix.MDUpdateType_FULL_REFRESH))
            message.setField(quickfix.MarketDepth(0))
            message.setField(quickfix.MDReqID(self.idGen.reqID()))

            relatedSym = self.fixVersion.MarketDataRequest.NoRelatedSym()
            relatedSym.setField(quickfix.Product(quickfix.Product_CURRENCY))
            relatedSym.setField(quickfix.SecurityType(quickfix.SecurityType_FOREIGN_EXCHANGE_CONTRACT))
            relatedSym.setField(quickfix.Symbol(subscription.symbol))
            message.addGroup(relatedSym)

            group = self.fixVersion.MarketDataRequest.NoMDEntryTypes()
            group.setField(quickfix.MDEntryType(quickfix.MDEntryType_BID))
            message.addGroup(group)
            group.setField(quickfix.MDEntryType(quickfix.MDEntryType_BID))
            message.addGroup(group)

            self.sendToTarget(message, self.marketSession)

    def getSettlementDate(self):
        tomorrow = datetime.date.today() + datetime.timedelta(days=1)
        return tomorrow.strftime('%Y%m%d')

    def onNewOrderSingle(self, message, beginString, sessionID):
        symbol = quickfix.Symbol()
        side = quickfix.Side()
        ordType = quickfix.OrdType()
        orderQty = quickfix.OrderQty()
        price = quickfix.Price()
        clOrdID = quickfix.ClOrdID()
        quoteID = quickfix.QuoteID()
        currency = quickfix.Currency()

        message.getField(ordType)
        if ordType.getValue() != quickfix.OrdType_PREVIOUSLY_QUOTED:
            raise quickfix.IncorrectTagValue(ordType.getField())

        message.getField(symbol)
        message.getField(side)
        message.getField(orderQty)
        message.getField(price)
        message.getField(clOrdID)
        message.getField(quoteID)
        message.getField(currency)

        executionReport = quickfix.Message()
        executionReport.getHeader().setField(beginString)
        executionReport.getHeader().setField(quickfix.MsgType(quickfix.MsgType_ExecutionReport))
        executionReport.setField(quickfix.OrderID(self.idGen.orderID()))
        executionReport.setField(quickfix.ExecID(self.idGen.execID()))

        try:
            reject_chance = random.choice(range(1, 101))
            if self.rejectRate > reject_chance:
                raise FixSimError("Rejected by cruel destiny %s" % str((reject_chance, self.rejectRate)))

            subscription = self.subscriptions.get(symbol.getValue())
            quote = subscription.orderbook.get(quoteID.getValue())

            execPrice = price.getValue()
            execSize = orderQty.getValue()
            if execSize > quote.size:
                raise FixSimError("size to large for quote")

            if abs(execPrice - quote.price) > 0.0000001:
                raise FixSimError("Trade price not equal to quote")

            executionReport.setField(quickfix.SettlDate(self.getSettlementDate()))
            executionReport.setField(quickfix.Currency(subscription.currency))

            executionReport.setField(quickfix.OrdStatus(quickfix.OrdStatus_FILLED))
            executionReport.setField(symbol)
            executionReport.setField(side)
            executionReport.setField(clOrdID)

            executionReport.setField(quickfix.Price(price.getValue()))
            executionReport.setField(quickfix.AvgPx(execPrice))
            executionReport.setField(quickfix.LastPx(execPrice))

            executionReport.setField(quickfix.LastShares(execSize))
            executionReport.setField(quickfix.CumQty(execSize))
            executionReport.setField(quickfix.OrderQty(execSize))

            executionReport.setField(quickfix.ExecType(quickfix.ExecType_FILL))
            executionReport.setField(quickfix.LeavesQty(0))

        except Exception as e:
            self.logger.exception("FixServer:Close order error")
            executionReport.setField(quickfix.SettlDate(''))
            executionReport.setField(currency)

            executionReport.setField(quickfix.OrdStatus(quickfix.OrdStatus_REJECTED))
            executionReport.setField(symbol)
            executionReport.setField(side)
            executionReport.setField(clOrdID)

            executionReport.setField(quickfix.Price(0))
            executionReport.setField(quickfix.AvgPx(0))
            executionReport.setField(quickfix.LastPx(0))

            executionReport.setField(quickfix.LastShares(0))
            executionReport.setField(quickfix.CumQty(0))
            executionReport.setField(quickfix.OrderQty(0))

            executionReport.setField(quickfix.ExecType(quickfix.ExecType_REJECTED))
            executionReport.setField(quickfix.LeavesQty(0))

        self.sendToTarget(executionReport, sessionID)

    def makeOrder(self, quoteID):
        pass


    def onMarketDataSnapshotFullRefresh(self, message, sessionID):
        skip_chance = random.choice(range(1, 101))
        if self.skipSnapshotChance > skip_chance:
            self.logger.info("onMarketDataSnapshotFullRefresh skip making trade with random choice %d", skip_chance)
            return

        fix_symbol = quickfix.Symbol()
        message.getField(fix_symbol)
        symbol = fix_symbol.getValue()
        snapshot = Snapshot(symbol)

        group = self.fixVersion.MarketDataSnapshotFullRefresh.NoMDEntries()
        fix_no_entries = quickfix.NoMDEntries()
        message.getField(fix_no_entries)
        no_entries = fix_no_entries.getValue()

        for i in range(1, no_entries + 1):
            message.getGroup(i, group)
            price = quickfix.MDEntryPx()
            size = quickfix.MDEntrySize()
            quote_id = quickfix.QuoteEntryID()
            group.getField(quote_id)
            group.getField(price)
            group.getField(size)

            print price, size, quote_id
            quote = Quote()
            quote.price = price.getValue()
            quote.size = size.getValue()
            quote.id = quote_id.getValue()

            fix_entry_type = quickfix.MDEntryType()
            group.getField(fix_entry_type)
            entry_type = fix_entry_type.getValue()

            if entry_type == quickfix.MDEntryType_BID:
                snapshot.addBid(quote)
            elif entry_type == quickfix.MDEntryType_OFFER:
                snapshot.addAsk(quote)
            else:
                raise RuntimeError("Unknown entry type %s" % str(entry_type))

        print "SNAPSHOT RECEIVED !!!!!!"
        print snapshot

    def onExecutionReport(self, message, sessionID):
        print "!!!!! EXECUTIION REPORT ", message
        pass


    def dispatchFromApp(self, msgType, message, beginString, sessionID):
        if msgType == '8':
            self.onExecutionReport(message, sessionID)
        elif msgType == 'W':
            self.onMarketDataSnapshotFullRefresh(message, sessionID)

