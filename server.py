import quickfix
import copy
import uuid
import random
import datetime
import yaml
from twisted.internet import task

def rangef(first, last, step):
    if last < 0:
        raise ValueError("last float is negative")

    result = []
    while True:
        if first >= last:
            return result

        result.append(first)
        first+=step


class MarketDataError(quickfix.Exception):
    pass

class FixSimError(Exception):
    pass

class Quote(object):
    BID = '0'
    ASK = '1'

    def __init__(self, side, price, size):
        super(Quote, self).__init__()
        self.side = side
        self.price = price
        self.size = size
        self.id = None

    def __repr__(self):
        return "(%s %s, %s)" % (self.side, str(self.price), str(self.size))

class Subscription(object):
    def __init__(self, symbol, generator):
        super(Subscription, self).__init__()
        self.symbol = symbol
        self.currency = self.symbol.split("/")[0]
        self.sessions = set()
        self.generator = generator

    def createOrderBook(self):
        self.orderbook = OrderBook(self.generator.generate())

    def getFirstCurrency(self):
        return self.currency

    def hasSessions(self):
        return len(self.sessions) > 0

    def removeSession(self, sessionID):
        self.sessions.remove(sessionID)

    def addSession(self, sessionID):
        self.sessions.add(sessionID)

    def __iter__(self):
        return self.sessions.__iter__()

    def __repr__(self):
        return "<Subscription %s>" % self.symbol

    def __len__(self):
        return self.sessions.__len__()

class Subscriptions(object):
    def __init__(self):
        self.subscriptions = {}

    def add(self, subscription):
        if subscription.symbol in self.subscriptions:
            raise KeyError("Subscription for symbol has already exist")
        self.subscriptions[subscription.symbol] = subscription

    def get(self, symbol):
        return self.subscriptions.get(symbol, None)

    def __iter__(self):
        return self.subscriptions.values().__iter__()

class OrderBook(object):
    def __init__(self, quotes):
        self.quotes = []

        bid = []
        ask = []
        for quote in quotes:
            if quote.side == Quote.BID:
                bid.append(quote)
            else:
                ask.append(quote)

        if len(ask) != len(bid):
            raise FixSimError("len(ask) != len(bid)")

        bid,ask = self._sort(bid, ask)
        self._normalise(bid, ask)
        # print "BID", bid
        # print "ASK", ask
        result = bid+ask
        # print "result", result
        self.quotes = result

    def _sort(self, bid, ask):
        bid_new = sorted(bid, key=lambda x: x.price, reverse=True)
        ask_new = sorted(ask, key=lambda x: x.price)
        return bid_new,ask_new

    def _normalise(self, bid, ask):
        for b,a in zip(bid, ask):
            if b.price > a.price:
                b.price = a.price

    def __iter__(self):
        return self.quotes.__iter__()

    def get(self, quoteID):
        for quote in self.quotes:
            if quote.id == quoteID:
                return quote

        return None

class SnapshotGenerator(object):
    def __init__(self, step, limit):
        self.sources = []
        self.deltas = rangef(0, limit, step)
        self.orderBook = None

    def addQuote(self, quote):
        self.sources.append(quote)

    def modify(self, quote):
        quote.id = str(uuid.uuid4())
        delta = random.choice(self.deltas)
        quote.price += delta

    def generate(self):
        for source in self.sources:
            quote = copy.copy(source)
            self.modify(quote)
            yield quote

class IDGenerator(object):
    def __init__(self):
        self._orderID = 0
        self._execID = 0
        self._reqID = 0

    def orderID(self):
        self._orderID += 1
        return str(self._orderID)

    def execID(self):
        self._execID += 1
        return str(self._execID)

    def reqID(self):
        self._reqID += 1
        return str(self._reqID)


def load_yaml(path):
    with open(path,'r') as stream:
        cfg = yaml.load(stream)

    return cfg



def create_acceptor(server_config, simulation_config):
    def create_subscriptions(sources):
        subscriptions = Subscriptions()

        for source in sources:
            variation = source.get('variation', None)
            if variation:
                step,limit = variation.get('step', 0), variation.get('limit', 0)
            else:
                step,limit = 0,0

            subscription = Subscription(source['name'], SnapshotGenerator(step, limit))

            for quote in source['bid']:
                subscription.generator.addQuote(Quote(Quote.BID, quote['price'], quote['size']))

            for quote in source['ask']:
                subscription.generator.addQuote(Quote(Quote.ASK, quote['price'], quote['size']))

            subscriptions.add(subscription)
            return subscriptions

    settings = quickfix.SessionSettings(server_config)
    config = load_yaml(simulation_config)

    #ONLY FIX44 FOR NOW
    fix_version = config.get('fix_version','FIX44')
    if fix_version != 'FIX44':
        raise FixSimError("Unsupported fix version %s" % str(fix_version))

    import quickfix44
    publish_interval = config.get("publish_interval", 1)
    subscriptions = create_subscriptions(config['symbols'])

    application = Server(quickfix44, publish_interval, subscriptions)
    storeFactory = quickfix.FileStoreFactory(settings)
    logFactory = quickfix.ScreenLogFactory(settings)
    acceptor = quickfix.SocketAcceptor(application, storeFactory, settings, logFactory)
    return acceptor

class Server(quickfix.Application):
    def __init__(self, fix_version, interval, subscriptions):
        super(Server, self).__init__()
        self.idGen = IDGenerator()
        self.subscriptions = subscriptions
        self.fixVersion = fix_version
        self.publishInterval = interval
        self.loop = task.LoopingCall(self.publishMarketData)
        self.loop.start(self.publishInterval, True)





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
        try:
            print "SEND TO SESSION", sessionID
            quickfix.Session.sendToTarget(message, sessionID)
        except quickfix.SessionNotFound as e:
            print str(e)


    def publishMarketData(self):
        print "publishMarketData", self.subscriptions
        for subscription in self.subscriptions.values():
            if not subscription.hasSessions():
                continue

            message = self.fixVersion.MarketDataSnapshotFullRefresh()
            message.setField(quickfix.Symbol(subscription.symbol))
            message.setField(quickfix.MDReqID(self.idGen.reqID()))

            group = self.fixVersion.MarketDataSnapshotFullRefresh().NoMDEntries()

            subscription.createOrderBook()

            for quote in subscription.orderbook:
                group.setField(quickfix.MDEntryType(quote.side))
                group.setField(quickfix.MDEntryPx(quote.price))
                group.setField(quickfix.MDEntrySize(quote.size))
                group.setField(quickfix.QuoteEntryID(quote.id))
                group.setField(quickfix.Currency(subscription.currency))
                group.setField(quickfix.QuoteCondition(quickfix.QuoteCondition_OPEN_ACTIVE))
                message.addGroup( group )

            for sessionID in subscription:
                self.sendToTarget(message, sessionID)


    def onMarketDataRequest(self, message, sessionID):
        print 'onMarketDataRequest', message, sessionID
        requestID = quickfix.MDReqID()
        message.getField(requestID)
        print "Request id ", requestID.getValue()

        relatedSym = self.fixVersion.MarketDataRequest.NoRelatedSym()
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
        entryType = self.fixVersion.MarketDataRequest.NoMDEntryTypes()
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
         reject = self.fixVersion.MarketDataRequestReject()
         reject.setField(requestID)
         text = quickfix.Text(reason)
         reject.setField(text)
         self.sendToTarget(reject, sessionID)

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

        message.getField(ordType)
        if ordType.getValue() != quickfix.OrdType_PREVIOUSLY_QUOTED:
            raise quickfix.IncorrectTagValue(ordType.getField())

        message.getField(symbol)
        message.getField(side)
        message.getField(orderQty)
        message.getField(price)
        message.getField(clOrdID)
        message.getField(quoteID)

        subscription = self.getSubscription(symbol.getValue())
        quote = subscription.orderbook.get(quoteID.getValue())

        execPrice = price.getValue()
        execSize = orderQty.getValue()
        if execSize > quote.size:
            raise quickfix.IncorrectMessageStructure("size to large for quote")

        if abs(execPrice - quote.price) > 0.0000001:
            raise quickfix.IncorrectMessageStructure("Trade price not equal to quote")

        executionReport = quickfix.Message()
        executionReport.getHeader().setField(beginString)
        executionReport.getHeader().setField(quickfix.MsgType(quickfix.MsgType_ExecutionReport))

        executionReport.setField(quickfix.SettlDate(self.getSettlementDate()))
        executionReport.setField(quickfix.Currency(subscription.currency))
        executionReport.setField(quickfix.OrderID(self.idGen.orderID()))
        executionReport.setField(quickfix.ExecID(self.idGen.execID()))
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


        #TODO VERSION ADAPTERS
        if beginString.getValue() == quickfix.BeginString_FIX40 \
                or beginString.getValue() == quickfix.BeginString_FIX41 \
                or beginString.getValue() == quickfix.BeginString_FIX42:
            executionReport.setField(quickfix.ExecTransType(quickfix.ExecTransType_NEW))

        if beginString.getValue() >= quickfix.BeginString_FIX41:
            executionReport.setField(quickfix.ExecType(quickfix.ExecType_FILL))
            executionReport.setField(quickfix.LeavesQty(0))


        self.sendToTarget(executionReport, sessionID)


    def fromApp(self, message, sessionID):
        print "********FROM APP", message, sessionID
        fixMsgType = quickfix.MsgType()
        beginString = quickfix.BeginString()
        message.getHeader().getField(beginString)
        message.getHeader().getField(fixMsgType)

        print fixMsgType

        msgType = fixMsgType.getValue()
        print "************************MESSAGE TYPE:", msgType
        if msgType == 'D':
            self.onNewOrderSingle(message, beginString, sessionID)
        elif msgType == 'V':
            self.onMarketDataRequest(message, sessionID)

