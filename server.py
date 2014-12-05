import quickfix
import copy
import uuid
import random
import datetime
import yaml
from twisted.internet import task

def float_range(first, last, step):
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
        subscription = self.subscriptions.get(symbol, None)
        return subscription

    def __iter__(self):
        return self.subscriptions.values().__iter__()

    def __repr__(self):
        return self.subscriptions.__repr__()

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
        self.deltas = float_range(0, limit, step)
        if len(self.deltas) == 0:
            self.deltas = [0]

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
    import logging
    import logging.handlers

    def create_logger(config):
        def syslog_logger():
            logger = logging.getLogger('FixServer')
            logger.setLevel(logging.DEBUG)
            handler = logging.handlers.SysLogHandler()
            logger.addHandler(handler)
            return logger

        def file_logger(fname):
            logger = logging.getLogger('FixServer')
            logger.setLevel(logging.DEBUG)
            handler = logging.handlers.RotatingFileHandler(fname)
            logger.addHandler(handler)
            return logger

        logcfg = config.get('logging', None)
        if not logging:
            return syslog_logger()

        target = logcfg['target']
        if target == 'syslog':
            logger =syslog_logger()
        elif target == 'file':
            filename = logcfg['filename']
            logger = file_logger(filename)
        else:
            raise FixSimError("invalid logger " + str(target))

        logger.addHandler(logging.StreamHandler())
        return logger

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


    publish_interval = config.get("publish_interval", 1)
    subscriptions = create_subscriptions(config['symbols'])

    logger = create_logger(config)
    rejectRate = config.get("reject_rate", 0)

    application = ServerFIX44(logger, publish_interval, rejectRate,  subscriptions)
    storeFactory = quickfix.FileStoreFactory(settings)
    logFactory = quickfix.ScreenLogFactory(settings)
    acceptor = quickfix.SocketAcceptor(application, storeFactory, settings, logFactory)
    return acceptor


def instance_safe_call(fn):
    def wrapper(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except quickfix.Exception as e:
            raise e
        except Exception as e:
            self.logger.exception(str(e))
    return wrapper


class Server(quickfix.Application):
    def __init__(self, fixVersion, logger, interval, rejectRate, subscriptions):
        super(Server, self).__init__()
        self.rejectRate = rejectRate
        self.idGen = IDGenerator()
        self.subscriptions = subscriptions
        self.fixVersion = fixVersion
        self.logger = logger
        self.publishInterval = interval
        self._onInit()

    def _onInit(self):
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

    @instance_safe_call
    def sendToTarget(self, message, sessionID):
        self.logger.info("SEND TO SESSION %s", message)
        quickfix.Session.sendToTarget(message, sessionID)

    @instance_safe_call
    def publishMarketData(self):
        self.logger.info("publishMarketData %s", self.subscriptions)
        for subscription in self.subscriptions:
            if not subscription.hasSessions():
                self.logger.info("No session subscribed, skip publish symbol %s", subscription.symbol)
                continue

            message = self.fixVersion.MarketDataSnapshotFullRefresh()
            message.setField(quickfix.Symbol(subscription.symbol))
            message.setField(quickfix.MDReqID(self.idGen.reqID()))

            group = self.fixVersion.MarketDataSnapshotFullRefresh().NoMDEntries()

            subscription.createOrderBook()
            for quote in subscription.orderbook:
                self.logger.info('add quote to fix message %s', str(quote))

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
        requestID = quickfix.MDReqID()
        try:
            message.getField(requestID)
        except Exception as e:
            raise quickfix.IncorrectTagValue(requestID)

        try:
            relatedSym = self.fixVersion.MarketDataRequest.NoRelatedSym()
            symbolFix = quickfix.Symbol()
            product = quickfix.Product()

            message.getGroup(1, relatedSym)
            relatedSym.getField(symbolFix)
            relatedSym.getField(product)
            if product.getValue() != quickfix.Product_CURRENCY:
                self.sendMarketDataReject(requestID, " product.getValue() != quickfix.Product_CURRENCY:", sessionID)
                return

            #bid
            entryType = self.fixVersion.MarketDataRequest.NoMDEntryTypes()
            message.getGroup(1, entryType)

            #ask
            message.getGroup(2, entryType)

            symbol = symbolFix.getValue()
            subscription = self.subscriptions.get(symbol)
            if subscription is None:
                self.sendMarketDataReject(requestID, "Unknown symbol: %s" % str(symbol), sessionID)
                return

            subscription.addSession(sessionID)
        except Exception as e:
            self.sendMarketDataReject(requestID, str(e), sessionID)

    @instance_safe_call
    def sendMarketDataReject(self, requestID, reason, sessionID):
         self.logger.error("SEND REJECT %s", reason)
         reject = self.fixVersion.MarketDataRequestReject()
         reject.setField(requestID)
         text = quickfix.Text(reason)
         reject.setField(quickfix.MDReqRejReason("0"))
         reject.setField(text)
         self.sendToTarget(reject, sessionID)

    def getSettlementDate(self):
        tomorrow = datetime.date.today() + datetime.timedelta(days=1)
        return tomorrow.strftime('%Y%m%d')

    def onNewOrderSingle(self, message, beginString, sessionID):
        pass

    @instance_safe_call
    def fromApp(self, message, sessionID):
        fixMsgType = quickfix.MsgType()
        beginString = quickfix.BeginString()
        message.getHeader().getField(beginString)
        message.getHeader().getField(fixMsgType)
        msgType = fixMsgType.getValue()

        self.logger.info("Message type %s", str(msgType))

        if msgType == 'D':
            self.onNewOrderSingle(message, beginString, sessionID)
        elif msgType == 'V':
            self.onMarketDataRequest(message, sessionID)


class ServerFIX44(Server):
    def __init__(self, logger, interval, reject_rate, subscriptions):
        import quickfix44
        super(ServerFIX44, self).__init__(quickfix44, logger, interval, reject_rate, subscriptions)

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
            reject_chance = random.choice(range(1,101))
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
            self.logger.exception("Close order error")
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