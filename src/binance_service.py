"""
封装与 Binance API 交互的逻辑。

本模块负责：
- 创建 Binance API 客户端
- 从 Binance 获取 ETH/USDT 交易对的 K 线数据
"""

from __future__ import annotations

from datetime import datetime
import os
from typing import Iterable, List

from binance.client import Client

# ETH/USDT 交易对符号，Binance API 使用此符号标识交易对
SYMBOL = "ETHUSDT"


def create_client(api_key: str | None = None, api_secret: str | None = None) -> Client:
    """
    创建 Binance API 客户端实例。

    参数优先级：
    1. 如果传入了 api_key 和 api_secret 参数，则使用传入的值
    2. 如果参数为 None，则从环境变量中读取：
       - BINANCE_API_KEY: API 密钥
       - BINANCE_API_SECRET: API 密钥对应的密钥

    注意：对于公开数据（如 K 线数据），通常不需要 API Key。
         但如果需要访问私有数据或提高请求频率限制，则需要配置 API Key。

    Args:
        api_key: Binance API Key，可选。如果为 None，则从环境变量读取
        api_secret: Binance API Secret，可选。如果为 None，则从环境变量读取

    Returns:
        Client: 配置好的 Binance 客户端实例

    Example:
        >>> # 使用环境变量
        >>> client = create_client()
        >>> 
        >>> # 使用传入参数
        >>> client = create_client(api_key="your_key", api_secret="your_secret")
    """

    # 优先使用传入的参数，如果为 None 则从环境变量读取
    key = api_key or os.getenv("BINANCE_API_KEY", "")
    secret = api_secret or os.getenv("BINANCE_API_SECRET", "")
    return Client(api_key=key, api_secret=secret)


def fetch_eth_klines(
    client: Client, start: datetime, end: datetime, interval: str = Client.KLINE_INTERVAL_1HOUR
) -> List[List[str]]:
    """
    从 Binance API 获取 ETH/USDT 交易对在指定时间范围内的 K 线数据。

    K 线数据包含以下字段（按顺序）：
    - open_time: 开盘时间（毫秒时间戳）
    - open: 开盘价
    - high: 最高价
    - low: 最低价
    - close: 收盘价
    - volume: 成交量（基础货币，即 ETH）
    - close_time: 收盘时间（毫秒时间戳）
    - quote_asset_volume: 成交额（计价货币，即 USDT）
    - number_of_trades: 成交笔数
    - taker_buy_base_volume: 主动买入成交量（基础货币）
    - taker_buy_quote_volume: 主动买入成交额（计价货币）
    - ignore: 忽略字段

    Args:
        client: Binance 客户端实例
        start: 开始时间（datetime 对象）
        end: 结束时间（datetime 对象）
        interval: K 线周期，默认为 1 小时。可选值包括：
                  - Client.KLINE_INTERVAL_1MINUTE: 1 分钟
                  - Client.KLINE_INTERVAL_5MINUTE: 5 分钟
                  - Client.KLINE_INTERVAL_15MINUTE: 15 分钟
                  - Client.KLINE_INTERVAL_1HOUR: 1 小时
                  - Client.KLINE_INTERVAL_1DAY: 1 天
                  等等

    Returns:
        List[List[str]]: K 线数据列表，每个元素是一个包含 12 个字段的列表（字符串格式）

    Raises:
        ValueError: 如果开始时间大于或等于结束时间

    Example:
        >>> from datetime import datetime
        >>> client = create_client()
        >>> start = datetime(2025, 10, 1)
        >>> end = datetime(2025, 10, 2)
        >>> klines = fetch_eth_klines(client, start, end, Client.KLINE_INTERVAL_1HOUR)
        >>> print(f"获取到 {len(klines)} 条 K 线数据")
    """

    # 验证时间范围的有效性
    if start >= end:
        raise ValueError("开始时间必须小于结束时间。")

    # Binance API 需要毫秒级时间戳
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    # 调用 Binance API 获取历史 K 线数据
    return client.get_historical_klines(
        symbol=SYMBOL,  # 交易对符号
        interval=interval,  # K 线周期
        start_str=start_ms,  # 开始时间（毫秒时间戳）
        end_str=end_ms,  # 结束时间（毫秒时间戳）
    )


