import re
import time
import openai
from services.query import run_query, clean_records

_PROMPT_HEADER = (
    "You are an expert MySQL analyst for a recruitment CRM.\n"
    "Output ONLY a valid MySQL SELECT statement. No explanations, no markdown, no backticks.\n"
    "Column aliases must be in Ukrainian.\n\n"
    "━━━ SCHEMA ━━━\n"
)

_SCHEMA_DYNAMIC = ""

_SCHEMA_HINTS = """
-- ВАЖЛИВІ ПОЯСНЕННЯ:
-- requests_benches.author_id = РЕКРУТЕР (alias: rec)
-- requests.author_id = HIRING MANAGER (alias: hm)
-- requests_bs_statuses: кожна подача має КІЛЬКА рядків. Фінальний = MAX(id)
-- requests.duration: тривалість проєкту ('1_3_mo') — НЕ стаж рекрутера
-- requests.status: active | closed | lost | won | on_hold | cvs_are_not_accepted
-- requests_bs_statuses.status: NEWLY_ATTACHED_TO_REQUEST → CV_SENT_TO_THE_CLIENT → FIRST_INTERVIEW → INTERVIEW_WITH_CLIENT → SECOND_INTERVIEW → FINAL_INTERVIEW → WON | LOST
-- УВАГА: використовуй ТІЛЬКИ повні назви статусів. НЕ 'CV_SENT' — а 'CV_SENT_TO_THE_CLIENT'. НЕ 'NEWLY_ATTACHED' — а 'NEWLY_ATTACHED_TO_REQUEST'.
-- bench_technologies.level: junior | middle | senior | lead
-- requests_bs_status_reasons.status_id → requests_bs_statuses.id (тільки для LOST)
"""

_PROMPT_RULES = """
━━━ ОБОВ'ЯЗКОВІ ПРАВИЛА ━━━

## 1. ЗАВЖДИ фільтруй видалені записи через JOIN, не через NOT IN
  JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
  JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL  -- ОБОВ'ЯЗКОВО завжди
  НЕ ВИКОРИСТОВУЙ: rb.bench_id NOT IN (SELECT id FROM benches WHERE deleted_at IS NOT NULL)

## 2. ЗАВЖДИ використовуй COUNT(DISTINCT rb.id) — не COUNT(*)

## 3. ОСТАННІЙ СТАТУС — для будь-якого аналізу результатів (won/lost/кореляції)
Кожна подача має КІЛЬКА записів статусів. Брати треба ТІЛЬКИ останній.
  LEFT JOIN (
      SELECT requests_bs_id, status, created_at
      FROM requests_bs_statuses
      WHERE id IN (SELECT MAX(id) FROM requests_bs_statuses GROUP BY requests_bs_id)
  ) last_s ON last_s.requests_bs_id = rb.id

## 4. КОРЕЛЯЦІЇ — тільки завершені подачі
ТІЛЬКИ для кореляційних запитів: AND last_s.status IN ('WON', 'LOST') -- у WHERE
УВАГА: для загальної статистики рекрутерів — НЕ фільтруй по статусу.

## 5. ВІДСОТОК — завжди від завершених
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='LOST' THEN rb.id END)
      / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END), 0), 1)

## 6. НІКОЛИ не фільтруй статус у WHERE для кореляцій
## 7. НІКОЛИ не використовуй SUM(CASE WHEN rbs.status=...) — тільки COUNT(DISTINCT CASE WHEN)
## 8. JOIN admins — використовуй LEFT JOIN щоб не втрачати подачі без автора

━━━ ПАТЕРНИ ДЛЯ ТИПОВИХ ПИТАНЬ ━━━

## Статистика рекрутерів (топ/ефективність/порівняння)
  ОБОВ'ЯЗКОВО для будь-якого питання про рекрутерів (топ, ефективність, порівняння, хто найкращий):
  Завжди включай ВСІ ці колонки — НЕ скорочуй до однієї-двох:
    rec.name AS рекрутер,
    COUNT(DISTINCT rb.id) AS всього,
    COUNT(DISTINCT CASE WHEN last_s.status='WON'  THEN rb.id END) AS won,
    COUNT(DISTINCT CASE WHEN last_s.status='LOST' THEN rb.id END) AS lost,
    COUNT(DISTINCT CASE WHEN last_s.status NOT IN ('WON','LOST') OR last_s.status IS NULL THEN rb.id END) AS в_процесі,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END)
        / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END),0), 1) AS конверсія,
    ROUND(AVG(CASE WHEN last_s.status='WON'
        THEN TIMESTAMPDIFF(HOUR, rb.created_at, won_s.created_at)/24 END), 1) AS сер_днів_до_won
  Конверсія ЗАВЖДИ від завершених (WON+LOST), не від всіх подач.
  Для сер_днів_до_won використовуй підзапит won_s:
    LEFT JOIN (
        SELECT requests_bs_id, MIN(created_at) as created_at
        FROM requests_bs_statuses WHERE status = 'WON'
        GROUP BY requests_bs_id
    ) won_s ON won_s.requests_bs_id = rb.id
  НЕ фільтруй по статусу в WHERE — рахуй всі подачі, won/lost/в_процесі через CASE WHEN.
  ЗАВЖДИ додавай: AND rec.name IS NOT NULL -- але використовуй COALESCE для відображення
  Якщо питання "хто найгірший/найкращий" — показуй ВСІХ рекрутерів з сортуванням, НЕ LIMIT 1.
  GROUP BY rec.id, rec.name

## Воронка найму / конверсія між етапами
  Рахувати скільки подач досягли кожного етапу (хоч раз були на ньому):
  SELECT rbs.status AS етап,
      COUNT(DISTINCT rbs.requests_bs_id) AS кількість
  FROM requests_bs_statuses rbs
  JOIN requests_benches rb ON rb.id = rbs.requests_bs_id
  JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
  JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL
  GROUP BY rbs.status
  ORDER BY FIELD(rbs.status,
      'NEWLY_ATTACHED_TO_REQUEST','CV_SENT_TO_THE_CLIENT',
      'FIRST_INTERVIEW','INTERVIEW_WITH_CLIENT',
      'SECOND_INTERVIEW','FINAL_INTERVIEW','WON','LOST')


  По технологіях: JOIN bench_technologies bt + technologies t, WHERE last_s.status='WON'
  По вакансіях: додай r.title, r.client_country, r.duration, WHERE last_s.status='WON'

## Швидкість подачі кандидата
  AVG(TIMESTAMPDIFF(HOUR, r.created_at, rb.created_at)) / 24
  Якщо по рекрутеру: GROUP BY rec.id, rec.name

## Швидкість закриття
  AVG(TIMESTAMPDIFF(HOUR, rb.created_at, last_s.created_at)) / 24
  Де last_s.status IN ('WON','LOST') — окремо для won і lost

## Кореляція X і результатом (reject/won)
  Перша колонка = X (технологія / тривалість / рейт діапазон / рекрутер)
  Колонки ЗАВЖДИ у такому порядку: X, всього, won, lost, відсоток_won, відсоток_lost
  ОБОВ'ЯЗКОВО: WHERE last_s.status IN ('WON', 'LOST')
  НЕ ДОДАВАЙ JOIN admins якщо рекрутер/HM не є частиною аналізу — це відфільтровує дані.
  Сортування ЗАВЖДИ тільки: ORDER BY відсоток_won DESC — НЕ по назві технології чи іншому полю.
  Відсоток ЗАВЖДИ від завершених (WON+LOST), не від всіх:
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END)
        / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END), 0), 1) AS відсоток_won,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='LOST' THEN rb.id END)
        / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END), 0), 1) AS відсоток_lost
  Рейт діапазони ЗАВЖДИ через символи '<20', '20-40', '40-60', '60+' — НЕ 'менше 20', 'більше 60':
    CASE WHEN rb.rate < 20 THEN '<20'
         WHEN rb.rate < 40 THEN '20-40'
         WHEN rb.rate < 60 THEN '40-60'
         ELSE '60+' END AS діапазон_рейту
  Технологія: JOIN bench_technologies bt ON bt.bench_id = rb.bench_id
              JOIN technologies t ON t.id = bt.technology_id GROUP BY t.title

## Кореляція Рекрутер × Hiring Manager
  SELECT rec.name AS рекрутер, hm.name AS hiring_manager,
      COUNT(DISTINCT rb.id) AS всього,
      COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END) AS won,
      COUNT(DISTINCT CASE WHEN last_s.status='LOST' THEN rb.id END) AS lost,
      ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END)
          / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END), 0), 1) AS відсоток_won
  FROM requests_benches rb
  LEFT JOIN admins rec ON rec.id = rb.author_id
  JOIN requests r ON r.id = rb.request_id
  LEFT JOIN admins hm ON hm.id = r.author_id
  LEFT JOIN (
      SELECT requests_bs_id, status, created_at
      FROM requests_bs_statuses
      WHERE id IN (SELECT MAX(id) FROM requests_bs_statuses GROUP BY requests_bs_id)
  ) last_s ON last_s.requests_bs_id = rb.id
  WHERE r.deleted_at IS NULL AND rec.name IS NOT NULL AND hm.name IS NOT NULL
  GROUP BY rec.id, rec.name, hm.id, hm.name
  ORDER BY рекрутер, відсоток_won DESC

## Причини відмов (загальні або по рекрутеру)
  ОБОВ'ЯЗКОВО використовуй last_s підзапит + окремий JOIN на rbs зі статусом LOST:
  LEFT JOIN (
      SELECT requests_bs_id, status
      FROM requests_bs_statuses
      WHERE id IN (SELECT MAX(id) FROM requests_bs_statuses GROUP BY requests_bs_id)
  ) last_s ON last_s.requests_bs_id = rb.id
  JOIN requests_bs_statuses rbs ON rbs.requests_bs_id = rb.id AND rbs.status = 'LOST'
  JOIN requests_bs_status_reasons rbsr ON rbsr.status_id = rbs.id
  WHERE last_s.status = 'LOST'
    AND rbsr.reason IS NOT NULL
    AND rec.name IS NOT NULL
  Якщо по рекрутеру: GROUP BY rec.id, rec.name, rbsr.reason ORDER BY рекрутер, кількість DESC

## Фільтр по рекрутеру
  WHERE rec.name = 'Alla Maksymiv' -- або LIKE '%Alla%'

━━━ NULLIF завжди на знаменнику ━━━
  / NULLIF(COUNT(DISTINCT rb.id), 0)

━━━ LIMIT — тільки коли явно просять ━━━
  Використовуй LIMIT тільки якщо питання містить конкретну кількість: "топ 5", "перші 3".
  Якщо питання "порівняй всіх", "покажи всіх" — НЕ використовуй LIMIT.
  Якщо питання "хто найбільше/найменше/найгірший" — LIMIT 1 доречний.
"""


def load_schema_from_db(engine) -> None:
    global _SCHEMA_DYNAMIC, SYSTEM_PROMPT
    try:
        from sqlalchemy import inspect
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        lines = []
        for table in sorted(table_names):
            try:
                cols = inspector.get_columns(table)
                col_names = ", ".join(c["name"] for c in cols)
                lines.append(f"{table}({col_names})")
            except Exception:
                lines.append(f"{table}(...)")
        _SCHEMA_DYNAMIC = "\n".join(lines)
        print(f"[llm] Схема завантажена: {len(table_names)} таблиць")
    except Exception as e:
        print(f"[llm] Не вдалось завантажити схему: {e} — fallback")
        _SCHEMA_DYNAMIC = ""
    _rebuild_prompt()


def _rebuild_prompt() -> None:
    global SYSTEM_PROMPT
    SYSTEM_PROMPT = (
        _PROMPT_HEADER
        + _SCHEMA_DYNAMIC
        + _SCHEMA_HINTS
        + _PROMPT_RULES
    )


_rebuild_prompt()

def clean_sql(raw: str) -> str:
    m = re.search(r"```sql\s*(.*?)\s*```", raw, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r"(SELECT\s+.*)", raw, re.DOTALL | re.IGNORECASE)
    return m.group(1).strip() if m else raw.strip()


def validate_tables(sql: str):
    return None


def generate_sql(client, model, engine, question: str, history: list, run_fn=None):
    if run_fn is None:
        run_fn = run_query
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in history[-20:]:
        messages.append({"role": "user", "content": h["question"]})
        if h.get("sql"):
            messages.append({"role": "assistant", "content": h["sql"]})
    messages.append({"role": "user", "content": question})
    last_sql = ""

    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model=model, messages=messages,
                temperature=0.0, max_tokens=3000
            )
            sql = clean_sql(resp.choices[0].message.content)
            last_sql = sql
            bad = validate_tables(sql)
            if bad:
                raise Exception(f"Table '{bad}' does not exist.")
            df = run_fn(engine, sql)
            return sql, df, None
        except Exception as e:
            err = str(e)
            print(f"[attempt {attempt+1}/3] ERR: {err[:300]}")
            print(f"[attempt {attempt+1}/3] SQL: {last_sql[:400]}")
            if "429" in err or "rate_limit" in err.lower():
                return last_sql, None, "⚠️ Ліміт запитів вичерпано. Зачекай кілька хвилин."
            time.sleep(2)
            messages += [
                {"role": "assistant", "content": last_sql},
                {"role": "user", "content": (
                    f"SQL Error: {err}\n"
                    "Fix the SQL. Key reminders:\n"
                    "- Use last_s subquery: WHERE id IN (SELECT MAX(id) FROM requests_bs_statuses GROUP BY requests_bs_id)\n"
                    "- WHERE last_s.status IN ('WON','LOST') only for correlations\n"
                    "- COUNT(DISTINCT rb.id) not COUNT(*)\n"
                    "- JOIN benches b AND b.deleted_at IS NULL, r.deleted_at IS NULL\n"
                    "- LEFT JOIN admins to avoid losing submissions\n"
                    "- For loss reasons: always add AND rec.name IS NOT NULL to WHERE\n"
                )}
            ]
    return last_sql, None, "Не вдалося згенерувати коректний SQL після 3 спроб"


def analyze(client, model, question: str, df, history: list) -> str:
    if len(df) <= 150:
        summary = df.to_string(index=False)
        summary = summary[:3000]
    else:
        summary = f"Рядків у результаті: {len(df)}\n"
        for col in df.columns:
            if df[col].dtype in ["int64", "float64"]:
                summary += f"{col}: avg={df[col].mean():.1f}, min={df[col].min():.0f}, max={df[col].max():.0f}\n"
            else:
                top = df[col].value_counts().head(8)
                summary += f"{col}: {', '.join(f'{k}={v}' for k, v in top.items())}\n"
        summary += f"\nПерші рядки:\n{df.head(15).to_string(index=False)}"
        summary = summary[:3000]

    context = ""
    if history:
        last = history[-1]
        context = f'\nПопереднє питання: "{last["question"]}". Враховуй контекст.'

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": (
                f"ВІДПОВІДАЙ ТІЛЬКИ УКРАЇНСЬКОЮ МОВОЮ. НЕ використовуй англійську.\n"
                f"Ти Senior Recruitment Analyst. Питання: \"{question}\"{context}\n"
                f"Дані:\n{summary}\n\n"
                "Напиши аналіз українською. Максимум 150 слів. Суцільний текст без заголовків і markdown. "
                "Конкретні цифри — так. Пиши як колега на мітингу — просто і по суті. "
                "Якщо дані про рекрутерів — назви конкретно хто лідер і чому, хто відстає і що саме не так. "
                "Якщо є середній час до WON/LOST — прокоментуй швидкість роботи. "
                "Якщо є подачі в процесі — скажи скільки потенціалу ще не реалізовано. "
                "Якщо дані по причинах відмов — говори про топ-причину кожного рекрутера окремо. "
                "Замість 'win rate' кажи 'конверсія'."
            )}],
            temperature=0.3,
            max_tokens=4096,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        err = str(e)
        if "429" in err or "rate_limit" in err.lower():
            return "⚠️ Ліміт запитів вичерпано. Зачекай кілька хвилин і спробуй знову."
        if "503" in err or "unavailable" in err.lower():
            return "⚠️ Модель тимчасово недоступна. Спробуй ще раз."
        return "⚠️ Не вдалося згенерувати аналіз."


def _date_filter(date_from: str | None, date_to: str | None, alias: str = "r") -> str:
    parts = []
    if date_from:
        parts.append(f"{alias}.created_at >= '{date_from}'")
    if date_to:
        parts.append(f"{alias}.created_at <= '{date_to} 23:59:59'")
    return (" AND " + " AND ".join(parts)) if parts else ""


LAST_S = """
    LEFT JOIN (
        SELECT requests_bs_id, status, created_at
        FROM requests_bs_statuses
        WHERE id IN (SELECT MAX(id) FROM requests_bs_statuses GROUP BY requests_bs_id)
    ) last_s ON last_s.requests_bs_id = rb.id
"""


def get_kpi_data(engine, run_query_fn,
                 date_from: str | None = None,
                 date_to: str | None = None) -> dict:
    df = _date_filter(date_from, date_to, "r")

    queries = {
        "total_candidates": "SELECT COUNT(*) as n FROM benches WHERE deleted_at IS NULL",
        "total_requests":   f"SELECT COUNT(*) as n FROM requests r WHERE deleted_at IS NULL{df}",
        "active_requests":  f"SELECT COUNT(*) as n FROM requests r WHERE status='active' AND deleted_at IS NULL{df}",
        "won_requests":     f"SELECT COUNT(*) as n FROM requests r WHERE status='won' AND deleted_at IS NULL{df}",

        "overall_conversion": f"""
            SELECT COUNT(DISTINCT rb.id) as total,
                COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END) as won,
                ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END)
                    / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END),0), 1) as pct
            FROM requests_benches rb
            JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL{df}
            JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
            {LAST_S}
        """,

        "avg_time_to_won": f"""
            SELECT ROUND(AVG(TIMESTAMPDIFF(HOUR, r.created_at, last_s.created_at)) / 24, 1) as days
            FROM requests_benches rb
            JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL{df}
            JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
            {LAST_S}
            WHERE last_s.status = 'WON'
        """,

        "top_recruiters": f"""
            SELECT a.name,
                COUNT(DISTINCT rb.id) as total,
                COUNT(DISTINCT CASE WHEN last_s.status='WON'  THEN rb.id END) as won,
                COUNT(DISTINCT CASE WHEN last_s.status='LOST' THEN rb.id END) as lost,
                ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END)
                    / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END),0), 1) as win_rate
            FROM requests_benches rb
            JOIN admins a ON a.id = rb.author_id
            JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL{df}
            JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
            {LAST_S}
            GROUP BY a.id, a.name ORDER BY total DESC LIMIT 10
        """,

        "funnel": f"""
            SELECT rbs.status, COUNT(DISTINCT rbs.requests_bs_id) as cnt
            FROM requests_bs_statuses rbs
            JOIN requests_benches rb ON rb.id = rbs.requests_bs_id
            JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL{df}
            JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
            GROUP BY rbs.status
        """,

        "requests_by_status": f"""
            SELECT status, COUNT(*) as cnt FROM requests r
            WHERE deleted_at IS NULL{df} GROUP BY status ORDER BY cnt DESC
        """,

        "top_loss_reasons": f"""
            SELECT reason, COUNT(*) as cnt
            FROM requests_bs_status_reasons rbsr
            JOIN requests_bs_statuses rbs ON rbs.id = rbsr.status_id
            JOIN requests_benches rb ON rb.id = rbs.requests_bs_id
            JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL{df}
            WHERE reason IS NOT NULL 
            GROUP BY reason ORDER BY cnt DESC LIMIT 8
        """,

        "monthly_activity": f"""
            SELECT DATE_FORMAT(rb.created_at, '%Y-%m') as month,
                COUNT(DISTINCT rb.id) as submissions,
                COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END) as won
            FROM requests_benches rb
            JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL{df}
            JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
            {LAST_S}
            WHERE rb.created_at IS NOT NULL
            GROUP BY month ORDER BY month DESC LIMIT 12
        """,

        "conversion_by_country": f"""
            SELECT r.client_country as country,
                COUNT(DISTINCT rb.id) as total,
                COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END) as won,
                ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END)
                    / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END),0), 1) as win_rate
            FROM requests_benches rb
            JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL{df}
            JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
            {LAST_S}
            WHERE r.client_country IS NOT NULL AND r.client_country != ''
            GROUP BY r.client_country ORDER BY total DESC LIMIT 10
        """,

        "top_technologies": f"""
            SELECT t.title as tech,
                COUNT(DISTINCT rb.id) as submissions,
                COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END) as won,
                ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END)
                    / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END),0), 1) as win_rate
            FROM bench_technologies bt
            JOIN technologies t ON t.id = bt.technology_id
            JOIN benches b ON b.id = bt.bench_id AND b.deleted_at IS NULL
            JOIN requests_benches rb ON rb.bench_id = b.id
            JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL{df}
            {LAST_S}
            GROUP BY t.id, t.title ORDER BY submissions DESC LIMIT 10
        """,
    }

    result = {}
    for key, sql in queries.items():
        try:
            result[key] = clean_records(run_query_fn(engine, sql).to_dict(orient="records"))
        except Exception as e:
            result[key] = {"error": str(e)}
    return result


def get_recruiter_comparison(engine, run_query_fn,
                             recruiters: list,
                             date_from: str | None = None,
                             date_to: str | None = None) -> dict:
    df = _date_filter(date_from, date_to, "r")
    placeholders = ', '.join(f"'{r}'" for r in recruiters)
    names = f"({placeholders})"

    stats_sql = f"""
        SELECT a.name as recruiter,
            COUNT(DISTINCT rb.id) as total,
            COUNT(DISTINCT CASE WHEN last_s.status='WON'  THEN rb.id END) as won,
            COUNT(DISTINCT CASE WHEN last_s.status='LOST' THEN rb.id END) as lost,
            COUNT(DISTINCT CASE WHEN last_s.status='CV_SENT_TO_THE_CLIENT' THEN rb.id END) as cv_sent,
            COUNT(DISTINCT CASE WHEN last_s.status='FIRST_INTERVIEW' THEN rb.id END) as interviews,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END)
                / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END),0), 1) as win_rate,
            ROUND(AVG(CASE WHEN last_s.status='WON'
                THEN TIMESTAMPDIFF(HOUR, r.created_at, won_s.created_at)/24 END), 1) as avg_days_to_won
        FROM requests_benches rb
        JOIN admins a ON a.id = rb.author_id
        JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL{df}
        JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
        {LAST_S}
        LEFT JOIN (
            SELECT requests_bs_id, MIN(created_at) as created_at
            FROM requests_bs_statuses WHERE status = 'WON'
            GROUP BY requests_bs_id
        ) won_s ON won_s.requests_bs_id = rb.id
        WHERE a.name IN {names}
        GROUP BY a.id, a.name
    """

    funnel_sql = f"""
        SELECT a.name as recruiter, last_s.status, COUNT(DISTINCT rb.id) as cnt
        FROM requests_benches rb
        JOIN admins a ON a.id = rb.author_id
        JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL{df}
        JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
        {LAST_S}
        WHERE a.name IN {names} AND last_s.status IS NOT NULL
        GROUP BY a.name, last_s.status
    """

    reasons_sql = f"""
        SELECT a.name as recruiter, rbsr.reason, COUNT(*) as cnt
        FROM requests_bs_status_reasons rbsr
        JOIN requests_bs_statuses rbs ON rbs.id = rbsr.status_id
        JOIN requests_benches rb ON rb.id = rbs.requests_bs_id
        JOIN admins a ON a.id = rb.author_id
        JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL{df}
        WHERE a.name IN {names} AND rbsr.reason IS NOT NULL 
        GROUP BY a.name, rbsr.reason ORDER BY cnt DESC
    """

    result = {}
    for key, sql in [("stats", stats_sql), ("funnel", funnel_sql)]:
        try:
            result[key] = clean_records(run_query_fn(engine, sql).to_dict(orient="records"))
        except Exception as e:
            result[key] = {"error": str(e)}
    try:
        result["reasons"] = clean_records(run_query_fn(engine, reasons_sql).to_dict(orient="records"))
    except Exception:
        result["reasons"] = []

    return result


def get_recruiter_names(engine, run_query_fn) -> list:
    try:
        df = run_query_fn(engine, """
            SELECT DISTINCT a.name FROM admins a
            JOIN requests_benches rb ON rb.author_id = a.id
            ORDER BY a.name
        """)
        return df["name"].tolist()
    except Exception:
        return []


def get_group_comparison(engine, run_query_fn,
                         groups: list,
                         date_from: str | None = None,
                         date_to: str | None = None) -> dict:
    df = _date_filter(date_from, date_to, "r")
    all_names = list(set(r for g in groups for r in g["recruiters"]))
    if not all_names:
        return {"groups": groups, "individual": [], "group_stats": []}

    placeholders = ', '.join(f"'{r}'" for r in all_names)
    names = f"({placeholders})"

    individual_sql = f"""
        SELECT a.name as recruiter,
            COUNT(DISTINCT rb.id) as total,
            COUNT(DISTINCT CASE WHEN last_s.status='WON'  THEN rb.id END) as won,
            COUNT(DISTINCT CASE WHEN last_s.status='LOST' THEN rb.id END) as lost,
            COUNT(DISTINCT CASE WHEN last_s.status='CV_SENT_TO_THE_CLIENT' THEN rb.id END) as cv_sent,
            COUNT(DISTINCT CASE WHEN last_s.status='FIRST_INTERVIEW' THEN rb.id END) as interviews,
            ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END)
                / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END),0), 1) as win_rate
        FROM requests_benches rb
        JOIN admins a ON a.id = rb.author_id
        JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL{df}
        JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
        {LAST_S}
        WHERE a.name IN {names}
        GROUP BY a.id, a.name
    """

    try:
        individual = clean_records(
            run_query_fn(engine, individual_sql).to_dict(orient="records")
        )
    except Exception:
        individual = []

    group_stats = []
    for group in groups:
        recs = group.get("recruiters", [])
        if not recs:
            group_stats.append({"name": group["name"], "total": 0, "won": 0, "lost": 0, "win_rate": 0})
            continue
        ph = ', '.join(f"'{r}'" for r in recs)
        sql = f"""
            SELECT
                COUNT(DISTINCT rb.id) as total,
                COUNT(DISTINCT CASE WHEN last_s.status='WON'  THEN rb.id END) as won,
                COUNT(DISTINCT CASE WHEN last_s.status='LOST' THEN rb.id END) as lost,
                COUNT(DISTINCT CASE WHEN last_s.status='CV_SENT_TO_THE_CLIENT' THEN rb.id END) as cv_sent,
                ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END)
                    / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END),0), 1) as win_rate
            FROM requests_benches rb
            JOIN admins a ON a.id = rb.author_id
            JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL{df}
            JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
            {LAST_S}
            WHERE a.name IN ({ph})
        """
        try:
            rows = run_query_fn(engine, sql).to_dict(orient="records")
            stats = clean_records(rows)[0] if rows else {}
            stats["name"] = group["name"]
            stats["recruiters"] = recs
            group_stats.append(stats)
        except Exception:
            group_stats.append({"name": group["name"], "recruiters": recs, "total": 0, "won": 0, "lost": 0, "win_rate": 0})

    return {
        "groups": groups,
        "individual": individual,
        "group_stats": group_stats,
    }


def get_details(engine, run_query_fn, df, context_filter: str = "") -> dict:
    cols_lower = [c.lower() for c in df.columns]
    details = {}

    has_recruiter  = any(c in cols_lower for c in ['рекрутер', 'recruiter'])
    has_technology = any(c in cols_lower for c in ['технологія', 'technology', 'tech'])
    has_rate       = any(c in cols_lower for c in ['діапазон_рейту', 'рейт', 'rate'])
    has_vacancy    = any(c in cols_lower for c in ['вакансія', 'vacancy', 'title'])
    has_won        = any(c in cols_lower for c in ['won', 'кількість_won'])

    if has_won and not has_recruiter and not has_technology:
        has_recruiter = False

    rec_col  = next((c for c in df.columns if c.lower() in ['рекрутер','recruiter']), None)
    tech_col = next((c for c in df.columns if c.lower() in ['технологія','technology','tech']), None)

    rec_filter  = ""
    tech_filter = ""

    if rec_col:
        names = list(df[rec_col].dropna().unique())[:10]
        if names:
            ph = ', '.join(f"'{n}'" for n in names)
            rec_filter = f"AND rec.name IN ({ph})"

    if tech_col:
        techs = list(df[tech_col].dropna().unique())[:10]
        if techs:
            ph = ', '.join(f"'{t}'" for t in techs)
            tech_filter = f"AND t.title IN ({ph})"

    LAST_S_SUB = """LEFT JOIN (
                    SELECT requests_bs_id, status FROM requests_bs_statuses
                    WHERE id IN (SELECT MAX(id) FROM requests_bs_statuses GROUP BY requests_bs_id)
                ) last_s ON last_s.requests_bs_id = rb.id"""

    if has_won and not has_recruiter and not has_technology:
        try:
            sql = f"""
                SELECT rec.name AS рекрутер,
                    COUNT(DISTINCT rb.id) AS всього,
                    COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END) AS won,
                    COUNT(DISTINCT CASE WHEN last_s.status='LOST' THEN rb.id END) AS lost,
                    ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END)
                        / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END),0),1) AS конверсія
                FROM requests_benches rb
                LEFT JOIN admins rec ON rec.id = rb.author_id
                JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL
                JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
                {LAST_S_SUB}
                WHERE last_s.status IN ('WON','LOST') AND rec.name IS NOT NULL
                GROUP BY rec.id, rec.name ORDER BY won DESC LIMIT 8
            """
            rows = run_query_fn(engine, sql)
            if not rows.empty:
                details["по_рекрутерах"] = {
                    "title": "По рекрутерах",
                    "columns": list(rows.columns),
                    "rows": clean_records(rows.values.tolist())
                }
        except Exception:
            pass

    if not has_technology:
        try:
            sql = f"""
                SELECT t.title AS технологія,
                    COUNT(DISTINCT rb.id) AS всього,
                    COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END) AS won,
                    COUNT(DISTINCT CASE WHEN last_s.status='LOST' THEN rb.id END) AS lost,
                    ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END)
                        / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END),0),1) AS конверсія
                FROM requests_benches rb
                LEFT JOIN admins rec ON rec.id = rb.author_id
                JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL
                JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
                JOIN bench_technologies bt ON bt.bench_id = rb.bench_id
                JOIN technologies t ON t.id = bt.technology_id
                {LAST_S_SUB}
                WHERE last_s.status IN ('WON','LOST') {rec_filter}
                GROUP BY t.id, t.title ORDER BY won DESC LIMIT 8
            """
            rows = run_query_fn(engine, sql)
            if not rows.empty:
                details["топ_технологій"] = {
                    "title": "Топ технологій",
                    "columns": list(rows.columns),
                    "rows": clean_records(rows.values.tolist())
                }
        except Exception:
            pass

    if not has_recruiter:
        try:
            sql = f"""
                SELECT rec.name AS рекрутер,
                    COUNT(DISTINCT rb.id) AS всього,
                    COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END) AS won,
                    COUNT(DISTINCT CASE WHEN last_s.status='LOST' THEN rb.id END) AS lost,
                    ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END)
                        / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END),0),1) AS конверсія
                FROM requests_benches rb
                LEFT JOIN admins rec ON rec.id = rb.author_id
                JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL
                JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
                {LAST_S_SUB}
                WHERE last_s.status IN ('WON','LOST') AND rec.name IS NOT NULL {tech_filter}
                GROUP BY rec.id, rec.name ORDER BY won DESC LIMIT 8
            """
            rows = run_query_fn(engine, sql)
            if not rows.empty:
                details["по_рекрутерах"] = {
                    "title": "По рекрутерах",
                    "columns": list(rows.columns),
                    "rows": clean_records(rows.values.tolist())
                }
        except Exception:
            pass

    if not has_rate:
        try:
            sql = f"""
                SELECT CASE WHEN rb.rate < 20 THEN '<20'
                            WHEN rb.rate < 40 THEN '20-40'
                            WHEN rb.rate < 60 THEN '40-60'
                            ELSE '60+' END AS діапазон_рейту,
                    COUNT(DISTINCT rb.id) AS всього,
                    COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END) AS won,
                    ROUND(100.0 * COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END)
                        / NULLIF(COUNT(DISTINCT CASE WHEN last_s.status IN ('WON','LOST') THEN rb.id END),0),1) AS конверсія
                FROM requests_benches rb
                LEFT JOIN admins rec ON rec.id = rb.author_id
                JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL
                JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
                {LAST_S_SUB}
                WHERE last_s.status IN ('WON','LOST') {rec_filter} {tech_filter}
                GROUP BY діапазон_рейту ORDER BY конверсія DESC
            """
            rows = run_query_fn(engine, sql)
            if not rows.empty:
                details["по_рейту"] = {
                    "title": "По діапазону рейту",
                    "columns": list(rows.columns),
                    "rows": clean_records(rows.values.tolist())
                }
        except Exception:
            pass

    if not has_vacancy and has_recruiter:
        try:
            sql = f"""
                SELECT r.title AS вакансія,
                    r.client_country AS країна,
                    COUNT(DISTINCT CASE WHEN last_s.status='WON' THEN rb.id END) AS won,
                    COUNT(DISTINCT rb.id) AS всього
                FROM requests_benches rb
                LEFT JOIN admins rec ON rec.id = rb.author_id
                JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL
                JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
                {LAST_S_SUB}
                WHERE last_s.status IN ('WON','LOST') {rec_filter}
                GROUP BY r.id, r.title, r.client_country
                HAVING won > 0 ORDER BY won DESC LIMIT 8
            """
            rows = run_query_fn(engine, sql)
            if not rows.empty:
                details["топ_вакансій"] = {
                    "title": "Топ вакансій (WON)",
                    "columns": list(rows.columns),
                    "rows": clean_records(rows.values.tolist())
                }
        except Exception:
            pass

    if has_recruiter or has_technology:
        try:
            sql = f"""
                SELECT rbsr.reason AS причина,
                    COUNT(*) AS кількість
                FROM requests_bs_status_reasons rbsr
                JOIN requests_bs_statuses rbs ON rbs.id = rbsr.status_id
                JOIN requests_benches rb ON rb.id = rbs.requests_bs_id
                LEFT JOIN admins rec ON rec.id = rb.author_id
                JOIN requests r ON r.id = rb.request_id AND r.deleted_at IS NULL
                JOIN benches b ON b.id = rb.bench_id AND b.deleted_at IS NULL
                JOIN bench_technologies bt ON bt.bench_id = rb.bench_id
                JOIN technologies t ON t.id = bt.technology_id
                WHERE rbsr.reason IS NOT NULL {rec_filter} {tech_filter}
                GROUP BY rbsr.reason ORDER BY кількість DESC LIMIT 8
            """
            rows = run_query_fn(engine, sql)
            if not rows.empty:
                details["причини_відмов"] = {
                    "title": "Причини відмов",
                    "columns": list(rows.columns),
                    "rows": clean_records(rows.values.tolist())
                }
        except Exception:
            pass

    return details
