
import json
import os
from typing import List
from mcp.server.fastmcp import FastMCP
import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

# 加载.env文件
load_dotenv()

# Initialize FastMCP server
mcp = FastMCP("knowledge", host='0.0.0.0', port=8001)


# PostgreSQL 连接配置（请根据实际情况修改）
PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT"))
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")

@mcp.tool()
def query_postgresql(sql: str) -> str:
    """
    连接 PostgreSQL 并执行传入的 SQL 查询，返回查询结果。

    Args:
        sql: 需要执行的 SQL 查询语句

    Returns:
        查询结果的 JSON 字符串，或错误信息
    """
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE
        )
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        result = [dict(zip(columns, row)) for row in rows]
        cur.close()
        conn.close()
        return json.dumps(result, ensure_ascii=False, indent=2)
    except Exception as e:
        return f"查询出错: {str(e)}"

@mcp.tool()
def get_table_metadata() -> str:
    """
    获取 PostgreSQL 表 product 的元数据信息（字段名、类型等），并查询 product_parameter_metadata 表的全部数据。
    Returns:
        表的元数据信息、product_parameter_metadata表数据和说明文字 JSON 字符串，或错误信息
    """
    table_name = "product"  # 只查一张表
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE
        )
        cur = conn.cursor()
        all_meta = {}
        # 查询 product 表元数据
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            """,
            (table_name,)
        )
        columns = cur.fetchall()
        meta = [
            {
                "column_name": col[0],
                "data_type": col[1]
            }
            for col in columns
        ]
        all_meta[table_name] = meta
        # 查询 product_parameter_metadata 表全部数据
        cur.execute("SELECT * FROM product_parameter_metadata")
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        param_data = [dict(zip(columns, row)) for row in rows]
        all_meta["product_parameter_metadata_data"] = param_data
        # 增加一段说明文字
        all_meta["description"] = "这里可以写字"
        cur.close()
        conn.close()
        return json.dumps(all_meta, ensure_ascii=False, indent=2)
    except Exception as e:
        return f"获取元数据出错: {str(e)}"

if __name__ == "__main__":

    # Initialize and run the server
    mcp.run(transport='sse')
