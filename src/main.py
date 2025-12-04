"""
命令行接口（CLI）入口模块。

本模块负责：
- 解析命令行参数
- 创建 Binance 客户端
- 调用数据拉取和入库流程
- 输出执行结果

使用示例：
    python -m src.main --start 2025-10-01 --end 2025-10-02 --interval 1h --db data/market.db
"""

from __future__ import annotations

import argparse
from pathlib import Path

from binance.client import Client

from . import ingest


def build_parser() -> argparse.ArgumentParser:
    """
    构建命令行参数解析器。

    定义所有支持的命令行参数及其说明。

    Returns:
        argparse.ArgumentParser: 配置好的参数解析器

    Args 说明:
        --start: 必需参数，数据拉取的开始时间
        --end: 必需参数，数据拉取的结束时间
        --interval: 可选参数，K 线周期，默认为 1 小时
        --db: 可选参数，数据库文件路径，默认为 data/market.db
        --api-key: 可选参数，Binance API Key，如果不提供则从环境变量读取
        --api-secret: 可选参数，Binance API Secret，如果不提供则从环境变量读取
    """
    parser = argparse.ArgumentParser(
        description="拉取 Binance ETH K 线数据并写入数据库。",
        epilog="示例：python -m src.main --start 2025-10-01 --end 2025-10-02"
    )

    # 必需参数：开始时间
    parser.add_argument(
        "--start",
        required=True,
        help="开始时间，格式支持：YYYY-MM-DD 或 YYYY-MM-DD HH:MM[:SS]（例如：2025-10-01 或 2025-10-01 12:30）"
    )

    # 必需参数：结束时间
    parser.add_argument(
        "--end",
        required=True,
        help="结束时间，格式同 --start（例如：2025-10-02 或 2025-10-02 23:59:59）"
    )

    # 可选参数：K 线周期，默认为 1 小时
    parser.add_argument(
        "--interval",
        default=Client.KLINE_INTERVAL_1HOUR,
        help="K 线周期，默认 1h。可选值：1m, 5m, 15m, 30m, 1h, 4h, 1d 等"
    )

    # 可选参数：数据库路径，默认为 data/market.db
    parser.add_argument(
        "--db",
        default=str(Path("data") / "market.db"),
        help="SQLite 数据库文件路径，默认为 data/market.db"
    )

    # 可选参数：API Key，如果不提供则从环境变量 BINANCE_API_KEY 读取
    parser.add_argument(
        "--api-key",
        default=None,
        help="Binance API Key。如果不提供，则从环境变量 BINANCE_API_KEY 读取"
    )

    # 可选参数：API Secret，如果不提供则从环境变量 BINANCE_API_SECRET 读取
    parser.add_argument(
        "--api-secret",
        default=None,
        help="Binance API Secret。如果不提供，则从环境变量 BINANCE_API_SECRET 读取"
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """
    主函数，程序的入口点。

    工作流程：
    1. 解析命令行参数
    2. 根据参数创建 Binance 客户端
    3. 调用数据拉取和入库流程
    4. 输出执行结果

    Args:
        argv: 命令行参数列表，可选。如果为 None，则从 sys.argv 读取。
              主要用于测试时传入模拟参数。

    Returns:
        int: 写入数据库的记录数。如果出错，可能会抛出异常。

    Example:
        >>> # 从命令行运行
        >>> # python -m src.main --start 2025-10-01 --end 2025-10-02
        >>> 
        >>> # 在代码中调用
        >>> count = main(["--start", "2025-10-01", "--end", "2025-10-02"])
    """
    # 步骤 1: 构建并解析命令行参数
    parser = build_parser()
    args = parser.parse_args(argv)

    # 步骤 2: 创建 Binance 客户端
    # 如果命令行提供了 API Key 和 Secret，则使用命令行参数
    # 否则使用环境变量（在 create_client 函数内部处理）
    client = ingest.binance_service.create_client(
        api_key=args.api_key,
        api_secret=args.api_secret
    )

    # 步骤 3: 执行数据拉取和入库流程
    inserted = ingest.ingest_eth_data(
        start=args.start,
        end=args.end,
        interval=args.interval,
        db_path=args.db,
        client=client
    )

    # 步骤 4: 输出执行结果
    print(f"已将 {inserted} 条 ETH K 线写入 {args.db}")
    return inserted


if __name__ == "__main__":
    # 使用 SystemExit 退出，这样可以在脚本被调用时返回正确的退出码
    # 如果 main() 返回 0，表示成功；非 0 表示失败
    raise SystemExit(main())

