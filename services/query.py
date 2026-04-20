import math
import pandas as pd
from sqlalchemy import text


def run_query(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def run_query_chat(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql.replace("%", "%%")), conn)


def safe(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    return v.item() if hasattr(v, "item") else v


def df_to_response(df: pd.DataFrame) -> dict:
    return {
        "columns": list(df.columns),
        "rows": [[safe(v) for v in row] for row in df.values.tolist()],
    }


def clean_records(records: list) -> list:
    cleaned = []
    for row in records:
        new_row = {}
        for k, v in row.items():
            new_row[k] = safe(v)
        cleaned.append(new_row)
    return cleaned