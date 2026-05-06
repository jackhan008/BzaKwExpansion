import logging
import warnings
import sys
from typing import Dict, Optional
from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine, text

# 忽略警告信息
warnings.filterwarnings("ignore")
# ============================================================================
# 数据库配置常量 - 请勿修改连接方式，ClickHouse 使用 clickhouse+http 驱动
# ============================================================================
CLICKHOUSE_CONFIG = {
    "host": "159.27.42.206",
    "port": 8123,
    "database": "default",
    "username": "default",
}


# ============================================================================
# 日志配置 logging setup
# ============================================================================
def setup_logging():
    """设置日志配置"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger(__name__)


logger = setup_logging()


# ============================================================================
# DB connection: do not change the connection method, as clickhouse+http is required for ClickHouse
# ============================================================================
def create_clickhouse_engine(password: str):
    """创建 ClickHouse 连接引擎

    使用 ClickHouse HTTP 驱动
    """
    try:
        password_encoded = quote_plus(password)
        conn_str = (
            f"clickhouse+http://{CLICKHOUSE_CONFIG['username']}:{password_encoded}"
            f"@{CLICKHOUSE_CONFIG['host']}:{CLICKHOUSE_CONFIG['port']}"
            f"/{CLICKHOUSE_CONFIG['database']}"
        )
        engine = create_engine(conn_str)

        # 验证连接
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        logger.info(
            f"✓ ClickHouse 连接成功: {CLICKHOUSE_CONFIG['host']}:{CLICKHOUSE_CONFIG['port']}"
        )
        return engine
    except Exception as e:
        logger.error(f"✗ ClickHouse 连接失败: {e}")
        raise


# ============================================================================
# 数据查询函数
# ============================================================================
def fetch_data_from_clickhouse(
    engine, sql: str, params: Optional[Dict] = None
) -> pd.DataFrame:
    """从 ClickHouse 执行查询并返回 DataFrame

    Args:
        engine: SQLAlchemy 引擎 (由 create_clickhouse_engine 创建)
        sql: SQL 查询语句
        params: 可选的查询参数字典，例如 {"id": 123}

    Returns:
        查询结果的 DataFrame
    """
    try:
        logger.info(f"正在从 ClickHouse 查询数据...")
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params=params)
        logger.info(f"✓ ClickHouse 查询成功，返回 {len(df)} 行数据")
        return df
    except Exception as e:
        logger.error(f"✗ ClickHouse 查询失败: {e}")
        raise
