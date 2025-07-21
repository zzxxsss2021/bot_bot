
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
mcp = FastMCP("knowledge", port=8001)


# PostgreSQL 连接配置（请根据实际情况修改）
PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT"))
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_SCHEMA = os.getenv("PG_SCHEMA")

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
    获取固定三张 PostgreSQL 表的元数据信息（字段名、类型等），
    并对 Can_Shu_BiaokKKtzpA1l5 表的 Can_Shu_Ming_Cheng 和 Ying_Wen_Can_Shu_Ming_Cheng 字段做 group by 查询。

    Returns:
        所有表的元数据信息和参数组合 JSON 字符串，或错误信息
    """
    table_names = ["production", "Mai_Dian_BiaoQGCqj7xkbM", "Can_Shu_BiaokKKtzpA1l5"]
    schema = PG_SCHEMA if PG_SCHEMA else "public"
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
        for table_name in table_names:
            cur.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table_name))
            columns = cur.fetchall()
            meta = [
                {
                    "column_name": col[0],
                    "data_type": col[1],
                    # "is_nullable": col[2],
                    # "column_default": col[3]
                }
                for col in columns
            ]
            all_meta[table_name] = meta

        # 对 Can_Shu_BiaokKKtzpA1l5 表做 group by 查询（带 schema，安全拼接）
        query = sql.SQL("""
            SELECT "Can_Shu_Ming_Cheng", "Ying_Wen_Can_Shu_Ming_Cheng"
            FROM {}.{}
            GROUP BY "Can_Shu_Ming_Cheng", "Ying_Wen_Can_Shu_Ming_Cheng"
        """).format(
            sql.Identifier(schema),
            sql.Identifier("Can_Shu_BiaokKKtzpA1l5")
        )
        cur.execute(query)
        param_combinations = cur.fetchall()
        param_list = [
            {
                "Can_Shu_Ming_Cheng": row[0],
                "Ying_Wen_Can_Shu_Ming_Cheng": row[1]
            }
            for row in param_combinations
        ]
        # 结果中增加参数组合信息
        all_meta["Can_Shu_BiaokKKtzpA1l5_param_combinations"] = param_list

        cur.close()
        conn.close()
        return json.dumps(all_meta, ensure_ascii=False, indent=2)
    except Exception as e:
        return f"获取元数据出错: {str(e)}"

@mcp.tool()
def test_pg_schema() -> str:
    """
    测试数据库连接并输出当前PG_SCHEMA的值。

    Returns:
        PG_SCHEMA的具体值
    """
    return f"当前PG_SCHEMA的值为: {PG_SCHEMA}"

if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='sse')
