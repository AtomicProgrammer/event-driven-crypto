"""
SQLite 数据库封装模块，负责 ETH K 线数据的持久化存储。

本模块提供：
- 数据库和表的初始化
- K 线数据的批量插入/更新（使用 INSERT OR REPLACE 实现 upsert）
"""

from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Iterable, Sequence

# ETH K 线数据表的 SQL 建表语句
# 使用 IF NOT EXISTS 确保表不存在时才创建，避免重复创建报错
# PRIMARY KEY 设置为 open_time（开盘时间），确保每条 K 线数据唯一
KLINE_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS eth_klines (
    open_time INTEGER PRIMARY KEY,              -- 开盘时间（毫秒时间戳），主键
    open REAL NOT NULL,                         -- 开盘价（USDT）
    high REAL NOT NULL,                         -- 最高价（USDT）
    low REAL NOT NULL,                          -- 最低价（USDT）
    close REAL NOT NULL,                        -- 收盘价（USDT）
    volume REAL NOT NULL,                       -- 成交量（ETH，基础货币）
    close_time INTEGER NOT NULL,                -- 收盘时间（毫秒时间戳）
    quote_asset_volume REAL NOT NULL,           -- 成交额（USDT，计价货币）
    number_of_trades INTEGER NOT NULL,          -- 成交笔数
    taker_buy_base_volume REAL NOT NULL,        -- 主动买入成交量（ETH）
    taker_buy_quote_volume REAL NOT NULL        -- 主动买入成交额（USDT）
);
"""

# 插入/更新 K 线数据的 SQL 语句
# 使用 INSERT OR REPLACE 实现 upsert 语义：
# - 如果 open_time 已存在，则更新该条记录
# - 如果 open_time 不存在，则插入新记录
# 使用参数化查询（? 占位符）防止 SQL 注入
INSERT_STATEMENT = """
INSERT OR REPLACE INTO eth_klines (
    open_time,
    open,
    high,
    low,
    close,
    volume,
    close_time,
    quote_asset_volume,
    number_of_trades,
    taker_buy_base_volume,
    taker_buy_quote_volume
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def init_database(db_path: str) -> None:
    """
    初始化数据库文件及表结构。

    如果数据库文件不存在，会自动创建。
    如果数据库目录不存在，会自动创建目录。
    如果表不存在，会创建 eth_klines 表。

    Args:
        db_path: SQLite 数据库文件路径（例如：'data/market.db'）

    Example:
        >>> init_database('data/market.db')
        >>> # 数据库和表已创建（如果不存在）
    """

    path = Path(db_path)
    # 确保数据库文件所在的目录存在，如果不存在则创建
    # parents=True: 创建所有父目录
    # exist_ok=True: 如果目录已存在则不报错
    path.parent.mkdir(parents=True, exist_ok=True)

    # 使用上下文管理器自动处理数据库连接的打开和关闭
    with sqlite3.connect(path) as conn:
        # 执行建表语句
        conn.execute(KLINE_TABLE_SCHEMA)
        # 提交事务，确保表结构被持久化
        conn.commit()


def bulk_upsert_klines(db_path: str, rows: Iterable[Sequence[float | int]]) -> int:
    """
    将 K 线记录批量写入数据库，使用 upsert 语义（存在则更新，不存在则插入）。

    使用 executemany 进行批量插入，比逐条插入效率更高。
    由于使用 INSERT OR REPLACE，如果同一条 K 线数据（相同的 open_time）已存在，
    则会更新该记录；如果不存在，则插入新记录。

    Args:
        db_path: SQLite 数据库文件路径
        rows: K 线数据行的可迭代对象。每行数据应该是一个序列，包含 11 个元素：
              [open_time, open, high, low, close, volume, close_time,
               quote_asset_volume, number_of_trades, taker_buy_base_volume,
               taker_buy_quote_volume]
              其中 open_time 和 close_time 为整数（毫秒时间戳），
              number_of_trades 为整数，其余为浮点数

    Returns:
        int: 受影响的行数（插入或更新的记录数）

    Example:
        >>> rows = [
        ...     (1696118400000, 2500.0, 2510.0, 2490.0, 2505.0, 100.5,
        ...      1696122000000, 251000.0, 1500, 50.0, 125000.0)
        ... ]
        >>> count = bulk_upsert_klines('data/market.db', rows)
        >>> print(f"已写入 {count} 条记录")
    """

    with sqlite3.connect(db_path) as conn:
        # 使用 executemany 批量执行插入语句
        # 将 rows 转换为列表，因为 executemany 需要可迭代对象
        cursor = conn.executemany(INSERT_STATEMENT, list(rows))
        # 提交事务，确保数据被持久化
        conn.commit()
        # 返回受影响的行数
        return cursor.rowcount


