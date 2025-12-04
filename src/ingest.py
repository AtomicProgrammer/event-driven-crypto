"""
数据拉取和入库的业务逻辑模块。

本模块负责：
- 解析用户输入的时间字符串
- 将 Binance API 返回的字符串格式数据转换为数据库所需的数值格式
- 协调 binance_service 和 database 模块，完成从 API 拉取到数据库存储的完整流程
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Sequence

from binance.client import Client

from . import database
from . import binance_service


def _parse_datetime(value: str) -> datetime:
    """
    解析时间字符串，支持多种常见格式。

    支持的时间格式（按优先级顺序）：
    1. "%Y-%m-%d %H:%M:%S" - 例如：2025-10-01 12:30:45
    2. "%Y-%m-%d %H:%M" - 例如：2025-10-01 12:30
    3. "%Y-%m-%d" - 例如：2025-10-01

    Args:
        value: 时间字符串

    Returns:
        datetime: 解析后的 datetime 对象

    Raises:
        ValueError: 如果时间字符串无法匹配任何支持的格式

    Example:
        >>> _parse_datetime("2025-10-01")
        datetime.datetime(2025, 10, 1, 0, 0)
        >>> _parse_datetime("2025-10-01 14:30")
        datetime.datetime(2025, 10, 1, 14, 30)
    """
    # 按顺序尝试不同的时间格式
    formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d")
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            # 如果当前格式不匹配，尝试下一个格式
            continue
    # 如果所有格式都不匹配，抛出异常
    raise ValueError(f"无法解析时间字符串: {value}")


def _transform_kline_row(kline: Sequence[str]) -> Sequence[float | int]:
    """
    将 Binance API 返回的字符串格式 K 线数据转换为数据库所需的数值格式。

    Binance API 返回的 K 线数据是字符串列表，需要转换为适当的数据类型：
    - 时间戳字段（open_time, close_time）转换为整数
    - 价格和成交量字段转换为浮点数
    - 成交笔数转换为整数

    Args:
        kline: Binance API 返回的 K 线数据行，是一个包含 12 个字符串元素的序列
               索引对应关系：
               0: open_time (开盘时间，毫秒时间戳)
               1: open (开盘价)
               2: high (最高价)
               3: low (最低价)
               4: close (收盘价)
               5: volume (成交量)
               6: close_time (收盘时间，毫秒时间戳)
               7: quote_asset_volume (成交额)
               8: number_of_trades (成交笔数)
               9: taker_buy_base_volume (主动买入成交量)
               10: taker_buy_quote_volume (主动买入成交额)
               11: ignore (忽略字段，不处理)

    Returns:
        Sequence[float | int]: 转换后的数据行，包含 11 个元素（忽略最后一个字段）

    Example:
        >>> kline = ["1696118400000", "2500.0", "2510.0", "2490.0", "2505.0",
        ...          "100.5", "1696122000000", "251000.0", "1500", "50.0", "125000.0", "0"]
        >>> row = _transform_kline_row(kline)
        >>> print(row)
        (1696118400000, 2500.0, 2510.0, 2490.0, 2505.0, 100.5, 1696122000000, 251000.0, 1500, 50.0, 125000.0)
    """
    return (
        int(kline[0]),      # open_time: 开盘时间（毫秒时间戳）
        float(kline[1]),    # open: 开盘价
        float(kline[2]),    # high: 最高价
        float(kline[3]),    # low: 最低价
        float(kline[4]),    # close: 收盘价
        float(kline[5]),    # volume: 成交量
        int(kline[6]),      # close_time: 收盘时间（毫秒时间戳）
        float(kline[7]),    # quote_asset_volume: 成交额
        int(kline[8]),      # number_of_trades: 成交笔数
        float(kline[9]),    # taker_buy_base_volume: 主动买入成交量
        float(kline[10]),   # taker_buy_quote_volume: 主动买入成交额
        # 注意：kline[11] 是忽略字段，不包含在返回结果中
    )


def ingest_eth_data(
    start: str,
    end: str,
    interval: str,
    db_path: str,
    client: Client | None = None,
) -> int:
    """
    拉取 ETH K 线数据并写入数据库的完整流程。

    工作流程：
    1. 解析开始和结束时间字符串
    2. 创建或使用传入的 Binance 客户端
    3. 从 Binance API 获取指定时间范围内的 ETH K 线数据
    4. 将 API 返回的字符串格式数据转换为数据库所需的数值格式
    5. 初始化数据库（如果不存在则创建）
    6. 批量写入数据库（使用 upsert 语义，避免重复数据）

    Args:
        start: 开始时间字符串，支持格式：
               - "YYYY-MM-DD"（例如："2025-10-01"）
               - "YYYY-MM-DD HH:MM"（例如："2025-10-01 12:30"）
               - "YYYY-MM-DD HH:MM:SS"（例如："2025-10-01 12:30:45"）
        end: 结束时间字符串，格式同 start
        interval: K 线周期，例如：
                  - Client.KLINE_INTERVAL_1HOUR: 1 小时
                  - Client.KLINE_INTERVAL_1DAY: 1 天
                  等等
        db_path: SQLite 数据库文件路径（例如："data/market.db"）
        client: Binance 客户端实例，可选。如果为 None，则使用默认配置创建客户端

    Returns:
        int: 写入数据库的记录数（插入或更新的条数）

    Raises:
        ValueError: 如果时间字符串无法解析，或开始时间大于等于结束时间

    Example:
        >>> count = ingest_eth_data(
        ...     start="2025-10-01",
        ...     end="2025-10-02",
        ...     interval=Client.KLINE_INTERVAL_1HOUR,
        ...     db_path="data/market.db"
        ... )
        >>> print(f"已写入 {count} 条 K 线数据")
    """

    # 步骤 1: 解析时间字符串为 datetime 对象
    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)

    # 步骤 2: 如果没有传入客户端，则创建默认客户端
    client = client or binance_service.create_client()

    # 步骤 3: 从 Binance API 获取 K 线数据
    klines = binance_service.fetch_eth_klines(client, start_dt, end_dt, interval)

    # 步骤 4: 将字符串格式的数据转换为数值格式，准备写入数据库
    rows = [_transform_kline_row(kline) for kline in klines]

    # 步骤 5: 初始化数据库（确保数据库和表存在）
    database.init_database(db_path)

    # 步骤 6: 批量写入数据库，返回写入的记录数
    return database.bulk_upsert_klines(db_path, rows)


