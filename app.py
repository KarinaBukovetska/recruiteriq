import os
import io
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import openai
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory, send_file
from flask_cors import CORS

from services.llm import (generate_sql, analyze, get_kpi_data,
                           get_recruiter_comparison, get_recruiter_names,
                           get_group_comparison, load_schema_from_db, get_details)
from services.query import run_query, run_query_chat, df_to_response, safe, clean_records
from services.pdf_report import generate_pdf_report
from services import storage

load_dotenv()

engine = create_engine(os.getenv("DB_URL"))
load_schema_from_db(engine)
storage.init(engine)

LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://openrouter.ai/api/v1")
LLM_API_KEY  = os.getenv("LLM_API_KEY")
LLM_MODEL    = os.getenv("LLM_MODEL", "google/gemini-2.0-flash-exp:free")

client = openai.OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=BASE_DIR, static_url_path="")
CORS(app)

conversations: dict[str, list] = {}
ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "10"))


@app.route("/")
def index():
    return send_from_directory(BASE_DIR, "dashboard.html")


@app.route("/api/kpi", methods=["GET"])
def kpi():
    date_from = request.args.get("date_from")
    date_to   = request.args.get("date_to")
    try:
        data = get_kpi_data(engine, run_query, date_from, date_to)
        alerts = []
        for r in (data.get("top_recruiters") or []):
            rate  = r.get("win_rate") or 0
            total = r.get("total") or 0
            if total >= 5 and rate < ALERT_THRESHOLD:
                alerts.append({
                    "type":      "warning",
                    "recruiter": r["name"],
                    "message":   f"{r['name']}: win rate {rate}% — нижче порогу {ALERT_THRESHOLD}%",
                    "win_rate":  rate,
                    "total":     total,
                })
        data["alerts"] = alerts
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ask", methods=["POST"])
def ask():
    data       = request.json or {}
    question   = data.get("question", "").strip()
    session_id = data.get("session_id", "default")
    if not question:
        return jsonify({"error": "No question"}), 400

    history = conversations.get(session_id, [])

    if not history:
        try:
            saved_msgs = storage.get_session_messages(session_id)
            for msg in saved_msgs:
                if msg["role"] == "user":
                    last_user = msg["content"]
                elif msg["role"] == "assistant" and "last_user" in dir():
                    history.append({
                        "question": last_user,
                        "sql": msg.get("sql", ""),
                        "rows": len(msg.get("rows", []))
                    })
            history = history[-20:]
            if history:
                conversations[session_id] = history
        except Exception:
            pass
    try:
        sql, df, error = generate_sql(client, LLM_MODEL, engine, question, history, run_query_chat)
    except Exception as e:
        import traceback
        print(f"[CRITICAL] generate_sql crashed: {e}")
        traceback.print_exc()
        return jsonify({"error": f"Критична помилка: {str(e)}"}), 500

    if error:
        return jsonify({"error": error, "sql": sql}), 500

    analysis = analyze(client, LLM_MODEL, question, df, history)
    history.append({"question": question, "sql": sql, "rows": len(df)})
    conversations[session_id] = history[-20:]

    try:
        details = get_details(engine, run_query, df)
    except Exception:
        details = {}

    storage.save_session_message(session_id, "user", question)
    cols = list(df.columns)
    rows = [[safe(v) for v in row] for row in df.values.tolist()]
    storage.save_session_message(session_id, "assistant", analysis,
                                  sql=sql, columns=cols, rows=rows[:50])

    resp = df_to_response(df)
    resp["sql"]      = sql
    resp["analysis"] = analysis
    resp["details"]  = details
    return jsonify(resp)


@app.route("/api/compare", methods=["POST"])
def compare():
    data       = request.json or {}
    recruiters = data.get("recruiters", [])
    if not recruiters:
        rec1 = data.get("rec1", "").strip()
        rec2 = data.get("rec2", "").strip()
        if rec1 and rec2:
            recruiters = [rec1, rec2]
    recruiters = [r.strip() for r in recruiters if r.strip()]
    date_from  = data.get("date_from")
    date_to    = data.get("date_to")
    if len(recruiters) < 2:
        return jsonify({"error": "Потрібно мінімум 2 рекрутери"}), 400
    if len(set(recruiters)) != len(recruiters):
        return jsonify({"error": "Оберіть різних рекрутерів"}), 400
    try:
        result = get_recruiter_comparison(engine, run_query, recruiters, date_from, date_to)

        stats = result.get("stats") or []
        if isinstance(stats, dict):
            stats = []
        reasons = result.get("reasons") or []
        if isinstance(reasons, dict):
            reasons = []

        top_reasons = {}
        for r in reasons:
            if not isinstance(r, dict):
                continue
            name = r.get("recruiter")
            if name not in top_reasons:
                top_reasons[name] = []
            if len(top_reasons[name]) < 3:
                reason_str = str(r.get('reason','')).replace('_',' ')
                top_reasons[name].append(f"{reason_str}({r.get('cnt',0)})")

        lines = []
        for s in stats:
            if not isinstance(s, dict):
                continue
            name = s.get('recruiter', '—')
            lines.append(
                f"{name}: {s.get('total',0)} подач, {s.get('won',0)} успішних, "
                f"конверсія {s.get('win_rate',0)}%, сер. час {s.get('avg_days_to_won','—')} днів. "
                f"Топ причини: {', '.join(top_reasons.get(name, ['—']))}"
            )

        prompt = (
            f"Ти Senior Recruitment Analyst. Порівняй рекрутерів, пиши українською, "
            f"конкретно і коротко. Максимум 6 речень. "
            f"НЕ 'вінрейт' — а 'конверсія'. НЕ 'WON/LOST' — а 'успішних/відмов'.\n\n"
            + "\n".join(lines) +
            "\n\nХто найефективніший і чому? Що потрібно покращити кожному?"
        )
        try:
            resp_ai = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=4096,
            )
            result["ai_analysis"] = resp_ai.choices[0].message.content.strip()
        except Exception:
            result["ai_analysis"] = ""

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/compare/groups", methods=["POST"])
def compare_groups():
    data      = request.json or {}
    groups    = data.get("groups", [])
    date_from = data.get("date_from")
    date_to   = data.get("date_to")

    if len(groups) < 2:
        return jsonify({"error": "Потрібно мінімум 2 групи"}), 400
    for g in groups:
        if not g.get("recruiters"):
            return jsonify({"error": f"Група '{g.get('name','')}' порожня"}), 400

    try:
        result = get_group_comparison(engine, run_query, groups, date_from, date_to)
        gs_list = result.get("group_stats", [])

        lines = []
        for gs in gs_list:
            lines.append(
                f"{gs.get('name')} ({', '.join(gs.get('recruiters',[]))}): "
                f"{gs.get('total',0)} подач, {gs.get('won',0)} успішних, "
                f"конверсія {gs.get('win_rate',0)}%"
            )

        prompt = (
            f"Ти Senior Recruitment Analyst. Порівняй групи рекрутерів, пиши українською, "
            f"конкретно і коротко. Максимум 6 речень. НЕ 'вінрейт' — а 'конверсія'.\n\n"
            + "\n".join(lines) +
            "\n\nЯка група найефективніша і чому? Що рекомендуєш?"
        )
        try:
            resp_ai = client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3, max_tokens=4096,
            )
            result["ai_analysis"] = resp_ai.choices[0].message.content.strip()
        except Exception:
            result["ai_analysis"] = ""

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route("/api/recruiters", methods=["GET"])
def recruiters():
    try:
        return jsonify({"names": get_recruiter_names(engine, run_query)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/report/pdf", methods=["POST"])
def report_pdf():
    data         = request.json or {}
    period_label = data.get("period_label", "Всі дані")
    date_from    = data.get("date_from")
    date_to      = data.get("date_to")
    try:
        kpi_data  = get_kpi_data(engine, run_query, date_from, date_to)
        pdf_bytes = generate_pdf_report(kpi_data, period_label,
                                         ai_client=client, ai_model=LLM_MODEL)
        filename  = f"recruiteriq_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
        return send_file(io.BytesIO(pdf_bytes), mimetype="application/pdf",
                         as_attachment=True, download_name=filename)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/export/csv", methods=["POST"])
def export_csv():
    data    = request.json or {}
    columns = data.get("columns", [])
    rows    = data.get("rows", [])
    if not columns or not rows:
        return jsonify({"error": "No data"}), 400
    df = pd.DataFrame(rows, columns=columns)
    buf = io.StringIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    buf.seek(0)
    return send_file(io.BytesIO(buf.getvalue().encode("utf-8-sig")),
                     mimetype="text/csv", as_attachment=True,
                     download_name="recruiteriq_export.csv")


@app.route("/api/export/excel", methods=["POST"])
def export_excel():
    data    = request.json or {}
    columns = data.get("columns", [])
    rows    = data.get("rows", [])
    if not columns or not rows:
        return jsonify({"error": "No data"}), 400
    df = pd.DataFrame(rows, columns=columns)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Дані")
        ws = writer.sheets["Дані"]
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 40)
    buf.seek(0)
    return send_file(buf,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True, download_name="recruiteriq_export.xlsx")



@app.route("/api/sessions", methods=["GET"])
def list_sessions():
    return jsonify({"sessions": storage.get_all_sessions()})


@app.route("/api/sessions/<session_id>", methods=["GET"])
def get_session(session_id):
    return jsonify({"messages": storage.get_session_messages(session_id)})


@app.route("/api/sessions/<session_id>", methods=["DELETE"])
def delete_session(session_id):
    conversations.pop(session_id, None)
    return jsonify({"ok": storage.delete_session(session_id)})


@app.route("/api/history/clear", methods=["POST"])
def clear_history():
    session_id = (request.json or {}).get("session_id", "default")
    conversations.pop(session_id, None)
    return jsonify({"ok": True})


if __name__ == "__main__":
    print("-> http://127.0.0.1:5000")
    app.run(debug=True, port=5000)