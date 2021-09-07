import decimal
import hashlib
import hmac
import json
import time
from copy import copy
from datetime import datetime, timedelta
import base64
import uuid
import pytz
from decimal import Decimal

from typing import List, Sequence, Dict, Any, Set

from requests import ConnectionError

from vnpy.event import Event, EventEngine
from vnpy_rest import Request, RestClient, Response
from vnpy_websocket import WebsocketClient
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.constant import (
    Direction,
    Exchange,
    OrderType,
    Product,
    Status,
    Interval
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    AccountData,
    ContractData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)

# UTC时区
UTC_TZ = pytz.utc

# 实盘REST API地址
REST_HOST: str = "https://api.pro.coinbase.com"

# 实盘Websocket API地址
WEBSOCKET_HOST: str = "wss://ws-feed.pro.coinbase.com"

# 模拟盘REST API地址
SANDBOX_REST_HOST: str = "https://api-public.sandbox.pro.coinbase.com"

# 模拟盘Websocket API地址
SANDBOX_WEBSOCKET_HOST: str = "wss://ws-feed-public.sandbox.pro.coinbase.com"

# 委托类型映射
ORDERTYPE_VT2COINBASE: Dict[OrderType, str] = {
    OrderType.LIMIT: "limit",
    OrderType.MARKET: "market",
}
ORDERTYPE_COINBASE2VT: Dict[str, OrderType] = {v: k for k, v in ORDERTYPE_VT2COINBASE.items()}

# 买卖方向映射
DIRECTION_VT2COINBASE: Dict[Direction, str] = {
    Direction.LONG: "buy",
    Direction.SHORT: "sell"
}
DIRECTION_COINBASE2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2COINBASE.items()}

# 数据频率映射
INTERVAL_VT2COINBASE: Dict[Interval, int] = {
    Interval.MINUTE: 60,
    Interval.HOUR: 3600,
    Interval.DAILY: 86400,
}

# 时间间隔映射
TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}

# 委托信息全局缓存字典
sys_order_map: Dict[str, OrderData] = {}

# 合约名称全局缓存字典
symbol_name_map: Dict[str, str] = {}


class CoinbaseGateway(BaseGateway):
    """
    vn.py用于对接Coinbase交易所的交易接口。
    """

    default_setting: Dict[str, Any] = {
        "ID": "",
        "Secret": "",
        "passphrase": "",
        "server": ["REAL", "SANDBOX"],
        "代理地址": "",
        "代理端口": 0,
    }

    exchanges: Exchange = [Exchange.COINBASE]

    def __init__(self, event_engine: EventEngine, gateway_name: str = "COINBASE") -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.rest_api: "CoinbaseRestApi" = CoinbaseRestApi(self)
        self.ws_api: "CoinbaseWebsocketApi" = CoinbaseWebsocketApi(self)

    def connect(self, setting: dict):
        """连接交易接口"""
        key: str = setting["ID"]
        secret: str = setting["Secret"]
        proxy_host: str = setting["代理地址"]
        proxy_port: int = setting["代理端口"]
        server: str = setting["server"]
        passphrase: str = setting["passphrase"]

        self.rest_api.connect(
            key,
            secret,
            passphrase,
            server,
            proxy_host,
            proxy_port
        )

        self.ws_api.connect(
            key,
            secret,
            passphrase,
            server,
            proxy_host,
            proxy_port
        )

        self.init_query()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.rest_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        self.rest_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        pass

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """关闭连接"""
        self.rest_api.stop()
        self.ws_api.stop()

    def process_timer_event(self, event: Event) -> None:
        """定时事件处理"""
        self.rest_api.query_account()

    def init_query(self) -> None:
        """初始化查询任务"""
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    
class CoinbaseRestApi(RestClient):
    """Coinbase的REST接口"""

    def __init__(self, gateway: CoinbaseGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: CoinbaseGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""
        self.passphrase: str = ""

        self.accounts: Dict[str, AccountData] = {}

    def sign(self, request: Request) -> Request:
        """生成Coinbase签名"""
        timestamp: str = str(time.time())
        message: str = "".join([timestamp, request.method,
                           request.path, request.data or ""])
        request.headers = (get_auth_header(timestamp, message,
                                           self.key,
                                           self.secret,
                                           self.passphrase))
        return request

    def connect(
        self,
        key: str,
        secret: str,
        passphrase: str,
        server: str,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """连接REST服务器"""
        self.key = key
        self.secret = secret.encode()
        self.passphrase = passphrase

        if server == "REAL":
            self.init(REST_HOST, proxy_host, proxy_port)
        else:
            self.init(SANDBOX_REST_HOST, proxy_host, proxy_port)

        self.start()

        self.query_instrument()
        self.query_order()

        self.gateway.write_log("REST API启动成功")

    def query_instrument(self) -> None:
        """查询合约信息"""
        self.add_request(
            "GET",
            "/products",
            callback=self.on_query_instrument
        )

    def query_order(self) -> None:
        """查询未成交委托"""
        self.add_request(
            "GET",
            "/orders?status=open",
            callback=self.on_query_order
        )

    def query_account(self) -> None:
        """查询资金"""
        self.add_request(
            "GET",
            "/accounts",
            callback=self.on_query_account,
        )

    def on_query_account(self, data: dict, request: Request) -> None:
        """资金查询回报"""
        for acc in data:
            account_id: str = str(acc["currency"])

            account: AccountData = self.accounts.get(account_id, None)
            if not account:
                account: AccountData = AccountData(
                    accountid=account_id,
                    gateway_name=self.gateway_name
                )
                self.accounts[account_id] = account

            account.balance = float(acc.get("balance", account.balance))
            account.available = float(acc.get("available", account.available))
            account.frozen = float(acc.get("hold", account.frozen))

            self.gateway.on_account(copy(account))

    def on_query_order(self, data: dict, request: Request):
        """未成交委托查询回报"""
        for d in data:
            if d["status"] == "open":
                if not float(d["filled_size"]):
                    status: Status = Status.NOTTRADED
                else:
                    status: Status = Status.PARTTRADED
            else:
                if d["size"] == d["filled_size"]:
                    status: Status = Status.ALLTRADED
                else:
                    status: Status = Status.CANCELLED

            order: OrderData = OrderData(
                symbol=d["product_id"],
                gateway_name=self.gateway_name,
                exchange=Exchange.COINBASE,
                orderid=d["id"],
                direction=DIRECTION_COINBASE2VT[d["side"]],
                price=float(d["price"]),
                volume=float(d["size"]),
                traded=float(d["filled_size"]),
                datetime=generate_datetime(d["created_at"]),
                status=status,
            )
            self.gateway.on_order(copy(order))

            sys_order_map[order.orderid] = order

        self.gateway.write_log(u"委托信息查询成功")

    def on_query_instrument(self, data: dict, request: Request) -> None:
        """合约信息查询回报"""
        for d in data:
            contract: ContractData = ContractData(
                symbol=d["id"],
                exchange=Exchange.COINBASE,
                name=d["display_name"],
                product=Product.SPOT,
                pricetick=float(d["quote_increment"]),
                size=1,
                min_volume=float(d["base_min_size"]),
                history_data=True,
                gateway_name=self.gateway_name,
            )

            self.gateway.on_contract(contract)
            symbol_name_map[contract.symbol] = contract.name

        self.gateway.write_log("合约信息查询成功")

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        orderid: str = str(uuid.uuid1())
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)

        data: dict = {
            "size": req.volume,
            "product_id": req.symbol,
            "side": DIRECTION_VT2COINBASE[req.direction],
            "type": ORDERTYPE_VT2COINBASE[req.type],
            "client_oid": orderid,
        }

        if req.type == OrderType.LIMIT:
            data["price"] = req.price

        self.add_request(
            "POST",
            "/orders",
            callback=self.on_send_order,
            data=json.dumps(data),
            params={},
            extra=order,
            on_failed=self.on_send_order_failed,
            on_error=self.on_send_order_error,
        )

        return order.vt_orderid

    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """委托下单失败服务器报错回报"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        if request.response.text:
            data: dict = request.response.json()
            error: str = data["message"]
            msg: str = f"委托失败，状态码：{status_code}，信息：{error}"
        else:
            msg: str = f"委托失败，状态码：{status_code}"

        self.gateway.write_log(msg)

    def on_send_order_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb,
        request: Request
    ) -> None:
        """委托下单回报函数报错回报"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_send_order(self, data: dict, request: Request) -> None:
        """委托下单回报"""
        pass

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        orderid: str = req.orderid

        if orderid in sys_order_map:
            path: str = f"/orders/{orderid}"
        else:
            path: str = f"/orders/client:{orderid}"

        self.add_request(
            "DELETE",
            path,
            callback=self.on_cancel_order,
            on_failed=self.on_cancel_order_failed,
        )

    def on_cancel_order(self, data: str, request: Request) -> None:
        """委托撤单回报"""
        sysid: str = data
        order: OrderData = sys_order_map[sysid]

        if order.status != Status.CANCELLED:
            order.status = Status.CANCELLED
            self.gateway.on_order(copy(order))

    def on_cancel_order_failed(self, status_code: str, request: Request) -> None:
        """撤单回报函数报错回报"""
        if request.response.text:
            data: dict = request.response.json()
            error: str = data["message"]
            msg: str = f"撤单失败，状态码：{status_code}，信息：{error}"
        else:
            msg: str = f"撤单失败，状态码：{status_code}"

        self.gateway.write_log(msg)

    def on_failed(self, status_code: int, request: Request) -> None:
        """请求失败的回调"""
        data: dict = request.response.json()
        error: str = data["message"]
        msg: str = f"请求失败，状态码：{status_code},信息：{error}"
        self.gateway.write_log(msg)

    def on_error(
        self,
        exception_type: type,
        exception_value: Exception,
        tb,
        request: Request
    ) -> None:
        """请求触发异常的回调"""
        msg: str = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        super().on_error(exception_type, exception_value, tb, request)

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        history: List[BarData] = []
        count: int = 300
        start: datetime = req.start
        path: str = f"/products/{req.symbol}/candles"
        time_delta: timedelta = TIMEDELTA_MAP[req.interval]

        while True:
            # 如果开始时间晚于结束时间则终止循环
            if start > req.end:
                break

            # 计算查询的开始时间和结束时间
            start_time: str = start.isoformat()

            end: datetime = start + time_delta * count
            end: datetime = min(end, req.end)
            end_time: str = end.isoformat()

            # 创建查询参数
            params: dict = {
                "start": start_time,
                "end": end_time,
                "granularity": INTERVAL_VT2COINBASE[req.interval],
            }

            resp: Response = self.request(
                "GET",
                path,
                params=params
            )

            if resp.status_code // 100 != 2:
                msg: str = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data: list = resp.json()
                if not data:
                    msg: str = f"获取历史数据为空，开始时间：{start_time}，数量：{count}"
                    break

                data.reverse()
                buf: List[BarData] = []

                for row in data[1:]:
                    dt: datetime = datetime.fromtimestamp(row[0])
                    dt: datetime = UTC_TZ.localize(dt)

                    bar: BarData = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        interval=req.interval,
                        volume=row[5],
                        open_price=row[3],
                        high_price=row[2],
                        low_price=row[1],
                        close_price=row[4],
                        gateway_name=self.gateway_name
                    )
                    buf.append(bar)

                history.extend(buf)

                begin: datetime = buf[0].datetime
                end: datetime = buf[-1].datetime
                msg: str = f"获取历史数据成功，{req.symbol} - {req.interval.value}，{begin} - {end}"
                self.gateway.write_log(msg)

                # 更新开始时间
                start: datetime = bar.datetime + TIMEDELTA_MAP[req.interval]

        return history


class CoinbaseWebsocketApi(WebsocketClient):
    """Coinbase的Websocket接口"""

    def __init__(self, gateway: CoinbaseGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: CoinbaseGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""
        self.passphrase: str = ""

        self.callbacks: dict = {
            "ticker": self.on_orderbook,
            "l2update": self.on_orderbook,
            "snapshot": self.on_orderbook,
            "received": self.on_order_received,
            "open": self.on_order_open,
            "done": self.on_order_done,
            "match": self.on_order_match,
        }

        self.orderbooks: Dict[str, "OrderBook"] = {}
        self.subscribed: Dict[str, SubscribeRequest] = {}
        self.tradeids: Set[str] = set()

    def connect(
        self,
        key: str,
        secret: str,
        passphrase: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """连接Websocket服务器"""
        self.gateway.write_log("开始连接ws接口")
        self.key = key
        self.secret = secret.encode()
        self.passphrase = passphrase

        if server == "REAL":
            self.init(WEBSOCKET_HOST, proxy_host, proxy_port)
        else:
            self.init(SANDBOX_WEBSOCKET_HOST, proxy_host, proxy_port)

        self.start()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        # 缓存订阅记录
        self.subscribed[req.symbol] = req
        if not self._active:
            return

        symbol: str = req.symbol
        exchange: Exchange = req.exchange

        orderbook: OrderBook = OrderBook(symbol, exchange, self.gateway)
        self.orderbooks[symbol] = orderbook

        sub_req: dict = {
            "type": "subscribe",
            "product_ids": [symbol],
            "channels": ["user", "level2", "ticker"],
        }

        timestamp: str = str(time.time())
        message: str = timestamp + "GET" + "/users/self/verify"

        auth_headers: dict = get_auth_header(
            timestamp,
            message,
            self.key,
            self.secret,
            self.passphrase
        )

        sub_req["signature"] = auth_headers["CB-ACCESS-SIGN"]
        sub_req["key"] = auth_headers["CB-ACCESS-KEY"]
        sub_req["passphrase"] = auth_headers["CB-ACCESS-PASSPHRASE"]
        sub_req["timestamp"] = auth_headers["CB-ACCESS-TIMESTAMP"]

        self.send_packet(sub_req)

    def on_connected(self) -> None:
        """连接成功回报"""
        self.gateway.write_log("Websocket API连接成功")

        for req in self.subscribed.values():
            self.subscribe(req)

    def on_disconnected(self) -> None:
        """连接断开回报"""
        self.gateway.write_log("Websocket API连接断开")

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        if packet["type"] == "error":
            self.gateway.write_log(
                "Websocket API报错： %s" % packet["message"])
            self.gateway.write_log(
                "Websocket API报错原因是： %s" % packet["reason"])
            self.active = False

        else:
            callback: callable = self.callbacks.get(packet["type"], None)
            if callback:
                if packet["type"] not in ["ticker", "snapshot", "l2update"]:
                    callback(packet)
                else:
                    product_id: str = packet["product_id"]
                    callback(packet, product_id)

    def on_orderbook(self, packet: dict, product_id: str) -> None:
        """订单簿更新推送"""
        orderbook: OrderBook = self.orderbooks[product_id]
        orderbook.on_message(packet)

    def on_order_received(self, packet: dict) -> None:
        """交易所接收委托回报"""
        sysid: str = packet["order_id"]

        order: OrderData = OrderData(
            symbol=packet["product_id"],
            exchange=Exchange.COINBASE,
            type=ORDERTYPE_COINBASE2VT[packet["order_type"]],
            orderid=packet["client_oid"],
            direction=DIRECTION_COINBASE2VT[packet["side"]],
            price=float(packet["price"]),
            volume=float(packet["size"]),
            datetime=generate_datetime(packet["time"]),
            status=Status.NOTTRADED,
            gateway_name=self.gateway_name,
        )

        sys_order_map[sysid] = order

        self.gateway.on_order(copy(order))

    def on_order_open(self, packet: dict) -> None:
        """委托成功回报"""
        sysid: str = packet["order_id"]
        order: OrderData = sys_order_map[sysid]

        order.traded = order.volume - float(packet["remaining_size"])
        if order.traded:
            order.status = Status.PARTTRADED

        self.gateway.on_order(copy(order))

    def on_order_done(self, packet: dict) -> None:
        """委托完成回报"""
        sysid: str = packet["order_id"]
        order: OrderData = sys_order_map[sysid]

        if order.status == Status.CANCELLED:
            return

        order.traded = order.volume - float(packet["remaining_size"])

        if packet["reason"] == "filled":
            order.status = Status.ALLTRADED
        else:
            order.status = Status.CANCELLED

        self.gateway.on_order(copy(order))

    def on_order_match(self, packet: dict) -> None:
        """委托成交回报"""
        if packet["maker_order_id"] in sys_order_map:
            order: OrderData = sys_order_map[packet["maker_order_id"]]
        else:
            order: OrderData = sys_order_map[packet["taker_order_id"]]

        tradeid: str = packet["trade_id"]
        if tradeid in self.tradeids:
            return
        self.tradeids.add(tradeid)

        trade: TradeData = TradeData(
            symbol=packet["product_id"],
            exchange=Exchange.COINBASE,
            orderid=order.orderid,
            tradeid=packet["trade_id"],
            direction=order.direction,
            price=float(packet["price"]),
            volume=float(packet["size"]),
            datetime=generate_datetime(packet["time"]),
            gateway_name=self.gateway_name,
        )
        self.gateway.on_trade(trade)


class OrderBook():
    """储存Coinbase订单簿数据"""

    def __init__(self, symbol: str, exchange: Exchange, gateway: BaseGateway) -> None:
        """构造函数"""

        self.asks: Dict[Decimal, Decimal] = dict()
        self.bids: Dict[Decimal, Decimal] = dict()
        self.gateway: CoinbaseGateway = gateway

        self.tick: TickData = TickData(
            symbol=symbol,
            exchange=exchange,
            name=symbol_name_map.get(symbol, ""),
            datetime=datetime.now(UTC_TZ),
            gateway_name=gateway.gateway_name,
        )

    def on_message(self, d: dict) -> None:
        """Websocket订单簿更新推送"""
        if d["type"] == "l2update":
            dt: datetime = generate_datetime(d["time"])
            self.on_update(d["changes"][0], dt)
        elif d["type"] == "snapshot":
            self.on_snapshot(d["asks"], d["bids"])
        else:
            self.on_ticker(d)

    def on_update(self, d: list, dt) -> None:
        """盘口更新推送"""
        size: decimal = Decimal(d[2])
        price: decimal = Decimal(d[1])
        side: str = d[0]

        if side == "buy":
            if price in self.bids:
                if not size:
                    del self.bids[price]
                else:
                    self.bids[price] = size
            else:
                self.bids[price] = size
        else:
            if price in self.asks:
                if not size:
                    del self.asks[price]
                else:
                    self.asks[price] = size
            else:
                self.asks[price] = size

        self.generate_tick(dt)

    def on_ticker(self, d: dict) -> None:
        """行情推送回报"""
        tick: TickData = self.tick

        tick.open_price = float(d["open_24h"])
        tick.high_price = float(d["high_24h"])
        tick.low_price = float(d["low_24h"])
        tick.last_price = float(d["price"])
        tick.volume = float(d["volume_24h"])
        tick.localtime = datetime.now()

        self.gateway.on_tick(copy(tick))

    def on_snapshot(self, asks: Sequence[List], bids: Sequence[List]) -> None:
        """盘口推送回报"""
        for price, size in asks:
            self.asks[Decimal(price)] = Decimal(size)

        for price, size in bids:
            self.bids[Decimal(price)] = Decimal(size)

    def generate_tick(self, dt: datetime) -> None:
        """合成tick"""
        tick = self.tick

        bids_keys: list = self.bids.keys()
        bids_keys: list = sorted(bids_keys, reverse=True)

        for i in range(min(5, len(bids_keys))):
            price: float = float(bids_keys[i])
            volume: float = float(self.bids[bids_keys[i]])
            setattr(tick, f"bid_price_{i + 1}", price)
            setattr(tick, f"bid_volume_{i + 1}", volume)

        asks_keys: list = self.asks.keys()
        asks_keys: list = sorted(asks_keys)

        for i in range(min(5, len(asks_keys))):
            price: float = float(asks_keys[i])
            volume: float = float(self.asks[asks_keys[i]])
            setattr(tick, f"ask_price_{i + 1}", price)
            setattr(tick, f"ask_volume_{i + 1}", volume)

        tick.datetime = dt
        tick.localtime = datetime.now()
        self.gateway.on_tick(copy(tick))


def get_auth_header(
    timestamp: str,
    message: str,
    api_key: str,
    secret_key: str,
    passphrase: str
) -> dict:
    """生成请求头"""
    message: bytes = message.encode("ascii")
    hmac_key: bytes = base64.b64decode(secret_key)
    signature: str = hmac.new(hmac_key, message, hashlib.sha256)
    signature_b64: bytes = base64.b64encode(signature.digest()).decode("utf-8")

    return {
        "Content-Type": "Application/JSON",
        "CB-ACCESS-SIGN": signature_b64,
        "CB-ACCESS-TIMESTAMP": timestamp,
        "CB-ACCESS-KEY": api_key,
        "CB-ACCESS-PASSPHRASE": passphrase
    }


def generate_datetime(timestamp: str) -> datetime:
    """生成时间"""
    dt: datetime = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    dt: datetime = UTC_TZ.localize(dt)
    return dt
