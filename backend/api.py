"""
FastAPI 后端 API，提供数据同步接口。

本模块提供 RESTful API 接口，供前端调用以触发数据同步。
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from src import ingest

# 创建 FastAPI 应用实例
app = FastAPI(
    title="Event-Driven Crypto API",
    description="加密货币数据同步 API",
    version="0.1.0"
)

# 配置 CORS，允许前端跨域访问
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境应该限制为具体的前端域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 默认数据库路径
DEFAULT_DB_PATH = str(Path("data") / "market.db")


class SyncRequest(BaseModel):
    """数据同步请求模型"""
    start: str = Field(..., description="开始时间，格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM[:SS]")
    end: str = Field(..., description="结束时间，格式同 start")
    interval: str = Field(default="1h", description="K 线周期，默认 1h。可选值：1m, 5m, 15m, 30m, 1h, 4h, 1d 等")
    db_path: str = Field(default=DEFAULT_DB_PATH, description="数据库文件路径")


class SyncResponse(BaseModel):
    """数据同步响应模型"""
    success: bool = Field(..., description="是否成功")
    message: str = Field(..., description="响应消息")
    records_count: int | None = Field(default=None, description="写入的记录数")
    error: str | None = Field(default=None, description="错误信息（如果有）")


@app.get("/")
async def root():
    """根路径，返回 API 信息"""
    return {
        "message": "Event-Driven Crypto API",
        "version": "0.1.0",
        "endpoints": {
            "sync": "/api/sync - 数据同步接口",
            "health": "/api/health - 健康检查"
        }
    }


@app.get("/api/health")
async def health_check():
    """健康检查接口"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.post("/api/sync", response_model=SyncResponse)
async def sync_data(request: SyncRequest) -> SyncResponse:
    """
    数据同步接口。

    接收前端传来的时间范围和参数，调用数据拉取和入库流程。

    Args:
        request: 同步请求，包含开始时间、结束时间、K 线周期等参数

    Returns:
        SyncResponse: 同步结果，包含成功状态、消息和记录数

    Raises:
        HTTPException: 如果同步过程中出现错误
    """
    try:
        # 调用数据拉取和入库流程
        records_count = ingest.ingest_eth_data(
            start=request.start,
            end=request.end,
            interval=request.interval,
            db_path=request.db_path
        )

        return SyncResponse(
            success=True,
            message=f"数据同步成功，已写入 {records_count} 条记录",
            records_count=records_count
        )

    except ValueError as e:
        # 参数错误（如时间格式错误）
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

    except Exception as e:
        # 其他错误（如网络错误、数据库错误等）
        return SyncResponse(
            success=False,
            message="数据同步失败",
            error=str(e)
        )


if __name__ == "__main__":
    import uvicorn
    # 启动开发服务器，监听 8000 端口
    uvicorn.run(app, host="0.0.0.0", port=8000)

