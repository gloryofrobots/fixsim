import sys
import quickfix
import yaml
from twisted.internet import task
from twisted.internet import reactor
import uuid

class MarketDataError(quickfix.Exception):
    pass

class FixSimError(Exception):
    pass

class Subscription(object):
    def __init__(self, symbol):
        super(Subscription, self).__init__()
        self.symbol = symbol
        self.sessions = set()
        self.snapshot = None

    def hasSessions(self):
        return len(self.sessions) > 0

    def removeSession(self, sessionID):
        self.sessions.remove(sessionID)

    def addSession(self, sessionID):
        self.sessions.add(sessionID)

    def setSnapshot(self, snapshot):
        self.snapshot = snapshot

    def __iter__(self):
        return self.sessions.__iter__()

    def __repr__(self):
        return "<Subscription %s>" % self.symbol

    def __len__(self):
        return self.sessions.__len__()

class Quote(object):
    def __init__(self, price, size):
        super(Quote, self).__init__()

        self.price = price
        self.size = size


class Snapshot(object):
    def __init__(self):
        super(Snapshot, self).__init__()
        self.bids = []
        self.asks = []

    def addBid(self, price, size):
        self.bids.append(Quote(price, size))

    def addAsk(self, price, size):
        self.asks.append(Quote(price, size))


class Application(quickfix.Application):
    orderID = 0
    execID = 0

    def __init__(self, config):
        super(Application, self).__init__()
        self.config = config
        self.subscriptions = {}

        #YOU CAN ADD OPTION FOR FIX VERSIONS HERE!
        import quickfix44
        self.fix_version = quickfix44

        self.loop = task.LoopingCall(self.publishMarketData)
        self.loop.start(1, True)

        symbols = self.config['symbols']
        for symbol in symbols:
            name = symbol['name']
            subscription = Subscription(name)
            snapshot = Snapshot()
            for quote in symbol['bid']:
                snapshot.addBid(quote['price'], quote['size'])

            for quote in symbol['ask']:
                snapshot.addAsk(quote['price'], quote['size'])

            subscription.setSnapshot(snapshot)
            self.subscriptions[name] = subscription

    def publishMarketData(self):
        print "publishMarketData", self.subscriptions
        for subscription in self.subscriptions.values():

            if not subscription.hasSessions():
                continue

            message = self.fix_version.MarketDataSnapshotFullRefresh()
            group = self.fix_version.MarketDataSnapshotFullRefresh().NoMDEntries()

            snapshot = subscription.snapshot
            for quote in snapshot.bids:
                group.setField(quickfix.MDEntryType('0'))
                group.setField(quickfix.MDEntryPx(quote.price))
                group.setField(quickfix.MDEntrySize(quote.size))
                group.setField(quickfix.QuoteEntryID(self.generateQuoteID()))
                message.addGroup( group )

            for quote in snapshot.asks:
                group.setField(quickfix.MDEntryType('1'))
                group.setField(quickfix.MDEntryPx(quote.price))
                group.setField(quickfix.MDEntrySize(quote.size))
                group.setField(quickfix.QuoteEntryID(self.generateQuoteID()))
                message.addGroup( group )

            for sessionID in subscription:
                self.sendToTarget(message, sessionID)

    def onCreate(self, sessionID):
        pass

    def onLogon(self, sessionID):
        return

    def onLogout(self, sessionID):
        return

    def toAdmin(self, sessionID, message):
        return

    def fromAdmin(self, sessionID, message):
        return

    def toApp(self, sessionID, message):
        return

    def sendToTarget(self, message, sessionID):
        print "SEND TO SESSION", sessionID
        quickfix.Session.sendToTarget(message, sessionID)

    def createMarketDataFullRefresh(self):
        message = self.fix_version.MarketDataSnapshotFullRefresh()
        group = self.fix_version.MarketDataSnapshotFullRefresh().NoMDEntries()

        group.setField(quickfix.MDEntryType('0'))
        group.setField(quickfix.MDEntryPx(12.32))
        group.setField(quickfix.MDEntrySize(100))
        group.setField(quickfix.OrderID("ORDERID"))
        message.addGroup( group )

        group.setField(quickfix.MDEntryType('1'))
        group.setField(quickfix.MDEntryPx(12.32))
        group.setField(quickfix.MDEntrySize(100))
        group.setField(quickfix.OrderID("ORDERID"))
        message.addGroup( group )
        return message


    def _parse(self, message):
        noMDEntries = quickfix.NoMDEntries()
        message.getField(noMDEntries)

        group = self.fix_version.MarketDataSnapshotFullRefresh.NoMDEntries()
        MDEntryType = quickfix.MDEntryType()
        MDEntryPx = quickfix.MDEntryPx()
        MDEntrySize = quickfix.MDEntrySize()
        orderID = quickfix.OrderID()

        message.getGroup(1, group)
        group.getField(MDEntryType)
        group.getField(MDEntryPx)
        group.getField(MDEntrySize)
        group.getField(orderID)

        message.getGroup(2, group)
        group.getField(MDEntryType)
        group.getField(MDEntryPx)
        group.getField(MDEntrySize)
        group.getField(orderID)


    def onMarketDataRequest(self, message, sessionID):
        print 'onMarketDataRequest', message, sessionID
        requestID = quickfix.MDReqID()
        message.getField(requestID)
        print "Request id ", requestID.getValue()

        relatedSym = self.fix_version.MarketDataRequest.NoRelatedSym()
        symbolFix = quickfix.Symbol()
        product = quickfix.Product()

        message.getGroup(1, relatedSym)
        relatedSym.getField(symbolFix)
        relatedSym.getField(product)
        print "onMarketDataRequest symbol", symbolFix, product
        if product.getValue() != quickfix.Product_CURRENCY:
            self.sendMarketDataReject(requestID, " product.getValue() != quickfix.Product_CURRENCY:", sessionID)
            return

        #bid
        entryType = self.fix_version.MarketDataRequest.NoMDEntryTypes()
        message.getGroup(1, entryType)
        #print "onMarketDataRequest bid", entryType

        #ask
        message.getGroup(2, entryType)
        #print "onMarketDataRequest ask", entryType

        symbol = symbolFix.getValue()

        if not self.isSymbolSupported(symbol):
            self.sendMarketDataReject(requestID, "Symbol %s does not supported" % symbol , sessionID)

        self.addSessionToSubscription(symbol, sessionID)

    def isSymbolSupported(self, symbol):
        return symbol in self.subscriptions

    def getSubscription(self, symbol):
        return self.subscriptions[symbol]

    def addSessionToSubscription(self, symbol, sessionID):
        if not self.isSymbolSupported(symbol):
            raise MarketDataError("Symbol %s does not supported" % symbol )

        subscription = self.getSubscription(symbol)
        subscription.addSession(sessionID)

    def sendMarketDataReject(self, requestID, reason, sessionID):
         reject = self.fix_version.MarketDataRequestReject()
         reject.setField(requestID)
         text = quickfix.Text(reason)
         reject.setField(text)
         self.sendToTarget(reject, sessionID)

    def onNewOrderSingle(self, message, beginString, sessionID):
        symbol = quickfix.Symbol()
        side = quickfix.Side()
        ordType = quickfix.OrdType()
        orderQty = quickfix.OrderQty()
        price = quickfix.Price()
        clOrdID = quickfix.ClOrdID()
        quoteID = quickfix.QuoteID()

        message.getField(ordType)
        if ordType.getValue() != quickfix.OrdType_LIMIT:
            raise quickfix.IncorrectTagValue(ordType.getField())

        message.getField(symbol)
        message.getField(side)
        message.getField(orderQty)
        message.getField(price)
        message.getField(clOrdID)
        message.getField(quoteID)

        executionReport = quickfix.Message()
        executionReport.getHeader().setField(beginString)
        executionReport.getHeader().setField(quickfix.MsgType(quickfix.MsgType_ExecutionReport))

        executionReport.setField(quickfix.OrderID(self.genOrderID()))
        executionReport.setField(quickfix.ExecID(self.genExecID()))
        executionReport.setField(quickfix.OrdStatus(quickfix.OrdStatus_FILLED))
        executionReport.setField(symbol)
        executionReport.setField(side)
        executionReport.setField(quickfix.CumQty(orderQty.getValue()))
        executionReport.setField(quickfix.AvgPx(price.getValue()))
        executionReport.setField(quickfix.LastShares(orderQty.getValue()))
        executionReport.setField(quickfix.LastPx(price.getValue()))
        executionReport.setField(clOrdID)
        executionReport.setField(orderQty)

        if beginString.getValue() == quickfix.BeginString_FIX40 \
                or beginString.getValue() == quickfix.BeginString_FIX41 \
                or beginString.getValue() == quickfix.BeginString_FIX42:
            executionReport.setField(quickfix.ExecTransType(quickfix.ExecTransType_NEW))

        if beginString.getValue() >= quickfix.BeginString_FIX41:
            executionReport.setField(quickfix.ExecType(quickfix.ExecType_FILL))
            executionReport.setField(quickfix.LeavesQty(0))

        try:
            self.sendToTarget(executionReport, sessionID)
        except quickfix.SessionNotFound as e:
            return

    def fromApp(self, message, sessionID):
        fixMsgType = quickfix.MsgType()
        beginString = quickfix.BeginString()
        message.getHeader().getField(beginString)
        message.getHeader().getField(fixMsgType)

        print fixMsgType

        msgType = fixMsgType.getValue()
        print "MESSAGE TYPE:", msgType
        if msgType == 'D':
            self.onNewOrderSingle(message, beginString, sessionID)
        elif msgType == 'V':
            self.onMarketDataRequest(message, sessionID)

    def genOrderID(self):
        self.orderID += 1
        return self.orderID

    def genExecID(self):
        self.execID += 1
        return self.execID

    def generateQuoteID(self):
        return str(uuid.uuid4())

def load_simulator_config(path):
    with open(path,'r') as stream:
        cfg = yaml.load(stream)

    return cfg
try:
    fix_config = sys.argv[1]
    settings = quickfix.SessionSettings(fix_config)

    simulator_config = load_simulator_config(sys.argv[2])

    application = Application(simulator_config)
    storeFactory = quickfix.FileStoreFactory(settings)
    logFactory = quickfix.ScreenLogFactory(settings)
    acceptor = quickfix.SocketAcceptor(application, storeFactory, settings, logFactory)
    acceptor.start()

    reactor.run()
except (quickfix.ConfigError, quickfix.RuntimeError) as e:
    print(e)

