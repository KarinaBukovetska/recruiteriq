import json
from datetime import datetime, date
from sqlalchemy import text


_engine = None

def init(engine) -> None:
    global _engine
    _engine = engine
    _create_tables()


def _conn():
    if _engine is None:
        raise RuntimeError("storage.init(engine) не було викликано")
    return _engine.connect()


def _json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def _dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, default=_json_serial)


def _create_tables() -> None:
    with _conn() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS riq_sessions (
                id          INT AUTO_INCREMENT PRIMARY KEY,
                session_id  VARCHAR(64) NOT NULL,
                title       VARCHAR(255),
                last_active DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_session_id (session_id),
                INDEX idx_last_active (last_active)
            ) CHARACTER SET utf8mb4
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS riq_messages (
                id          INT AUTO_INCREMENT PRIMARY KEY,
                session_id  VARCHAR(64) NOT NULL,
                role        VARCHAR(16) NOT NULL,
                content     TEXT,
                sql_query   TEXT,
                columns_json TEXT,
                rows_json   MEDIUMTEXT,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_session_id (session_id)
            ) CHARACTER SET utf8mb4
        """))
        conn.commit()

def get_all_sessions() -> list:
    with _conn() as conn:
        rows = conn.execute(text("""
            SELECT s.session_id, s.title, s.last_active,
                   COUNT(m.id) as msg_count
            FROM riq_sessions s
            LEFT JOIN riq_messages m ON m.session_id = s.session_id
            GROUP BY s.session_id, s.title, s.last_active
            ORDER BY s.last_active DESC
            LIMIT 50
        """)).fetchall()
    return [
        {"session_id": r[0], "title": r[1], "last_active": str(r[2]), "msg_count": r[3]}
        for r in rows
    ]


def get_session_messages(session_id: str) -> list:
    with _conn() as conn:
        rows = conn.execute(text("""
            SELECT role, content, sql_query, columns_json, rows_json
            FROM riq_messages
            WHERE session_id = :sid
            ORDER BY created_at ASC
            LIMIT 200
        """), {"sid": session_id}).fetchall()

    result = []
    for r in rows:
        msg = {"role": r[0], "content": r[1]}
        if r[2]: msg["sql"]     = r[2]
        if r[3]:
            try: msg["columns"] = json.loads(r[3])
            except Exception: pass
        if r[4]:
            try: msg["rows"]    = json.loads(r[4])
            except Exception: pass
        result.append(msg)
    return result


def save_session_message(session_id: str, role: str, content: str,
                         sql: str | None = None,
                         columns: list | None = None,
                         rows: list | None = None) -> None:
    now = datetime.now()
    with _conn() as conn:
        existing = conn.execute(text(
            "SELECT id FROM riq_sessions WHERE session_id = :sid"
        ), {"sid": session_id}).fetchone()

        if not existing:
            title = content[:60] if role == "user" else "Нова сесія"
            conn.execute(text(
                "INSERT INTO riq_sessions (session_id, title, last_active) VALUES (:sid, :t, :la)"
            ), {"sid": session_id, "t": title, "la": now})
        else:
            conn.execute(text(
                "UPDATE riq_sessions SET last_active = :la WHERE session_id = :sid"
            ), {"la": now, "sid": session_id})

        conn.execute(text("""
            INSERT INTO riq_messages (session_id, role, content, sql_query, columns_json, rows_json)
            VALUES (:sid, :role, :content, :sql, :cols, :rows)
        """), {
            "sid":     session_id,
            "role":    role,
            "content": content,
            "sql":     sql,
            "cols":    _dumps(columns) if columns else None,
            "rows":    _dumps(rows[:50]) if rows else None,
        })

        conn.execute(text("""
            DELETE FROM riq_messages
            WHERE session_id = :sid
            AND id NOT IN (
                SELECT id FROM (
                    SELECT id FROM riq_messages
                    WHERE session_id = :sid
                    ORDER BY created_at DESC
                    LIMIT 200
                ) tmp
            )
        """), {"sid": session_id})

        conn.commit()

    _cleanup_old_sessions()


def _cleanup_old_sessions() -> None:
    try:
        with _conn() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM riq_sessions")).scalar()
            if count > 200:
                conn.execute(text("""
                    DELETE FROM riq_sessions
                    WHERE session_id NOT IN (
                        SELECT session_id FROM (
                            SELECT session_id FROM riq_sessions
                            ORDER BY last_active DESC LIMIT 200
                        ) tmp
                    )
                """))
                conn.commit()
    except Exception:
        pass


def delete_session(session_id: str) -> bool:
    with _conn() as conn:
        conn.execute(text("DELETE FROM riq_messages WHERE session_id = :sid"), {"sid": session_id})
        r = conn.execute(text("DELETE FROM riq_sessions WHERE session_id = :sid"), {"sid": session_id})
        conn.commit()
    return r.rowcount > 0
