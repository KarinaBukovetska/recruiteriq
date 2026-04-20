import io
import os
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

def _register_font():
    candidates = [
        ("C:/Windows/Fonts/arial.ttf",   "C:/Windows/Fonts/arialbd.ttf"),
        ("C:/Windows/Fonts/calibri.ttf", "C:/Windows/Fonts/calibrib.ttf"),
        ("C:/Windows/Fonts/verdana.ttf", "C:/Windows/Fonts/verdanab.ttf"),
        ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
         "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
        ("/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
         "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"),
    ]
    for reg, bold in candidates:
        if os.path.exists(reg):
            try:
                pdfmetrics.registerFont(TTFont("F",  reg))
                pdfmetrics.registerFont(TTFont("FB", bold if os.path.exists(bold) else reg))
                return "F", "FB"
            except Exception:
                continue
    return "Helvetica", "Helvetica-Bold"

FN, FB = _register_font()

C_BG     = colors.HexColor('#080c10')
C_SURF   = colors.HexColor('#0e1318')
C_SURF2  = colors.HexColor('#141b22')
C_BORDER = colors.HexColor('#1e2832')
C_TEXT   = colors.HexColor('#d4dde8')
C_MUTED  = colors.HexColor('#4a5968')
C_MUTED2 = colors.HexColor('#6b7d8f')
C_ACCENT = colors.HexColor('#00d4ff')
C_GREEN  = colors.HexColor('#00e5a0')
C_RED    = colors.HexColor('#ff4757')
C_YELLOW = colors.HexColor('#ffc107')
C_PURPLE = colors.HexColor('#a78bfa')
C_WHITE  = colors.white

PAGE_W, PAGE_H = A4
MARGIN = 18 * mm
CONTENT_W = PAGE_W - 2 * MARGIN

def _ai_comment(client, model, prompt: str) -> str:
    if client is None:
        return ""
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=300,
        )
        return resp.choices[0].message.content.strip()
    except Exception:
        return ""


def get_ai_comments(client, model, kpi_data: dict) -> dict:
    conv  = (kpi_data.get("overall_conversion") or [{}])[0]
    total = conv.get("total", 0)
    won   = conv.get("won", 0)
    pct   = conv.get("pct", 0)
    avg_days = (kpi_data.get("avg_time_to_won") or [{}])[0].get("days", "—")
    recruiters = kpi_data.get("top_recruiters") or []
    funnel     = kpi_data.get("funnel") or []
    reasons    = kpi_data.get("top_loss_reasons") or []

    rec_summary = "\n".join(
        f"{r['name']}: {r['total']} подач, {r['won']} WON, {r.get('win_rate',0)}%"
        for r in recruiters[:8]
    )

    funnel_order = ['NEWLY_ATTACHED_TO_REQUEST','CV_SENT_TO_THE_CLIENT',
                    'FIRST_INTERVIEW','INTERVIEW_WITH_CLIENT',
                    'SECOND_INTERVIEW','FINAL_INTERVIEW','WON','LOST']
    funnel_map = {r['status']: r['cnt'] for r in funnel}
    funnel_summary = " → ".join(
        f"{s.replace('_',' ')}({funnel_map[s]})"
        for s in funnel_order if s in funnel_map
    )

    reasons_summary = ", ".join(
        f"{r['reason'].replace('_',' ')}({r['cnt']})"
        for r in reasons[:6]
    )

    base = (
        "Ти Senior Recruitment Analyst. Пиши українською, коротко і конкретно, "
        "як досвідчений колега на мітингу. Без зайвих слів, тільки суть і цифри. "
        "Максимум 2-3 речення.\n\n"
    )

    comments = {}

    comments["kpi"] = _ai_comment(client, model,
        base +
        f"Загальні KPI рекрутингу:\n"
        f"- Всього подач: {total}, WON: {won}, конверсія: {pct}%\n"
        f"- Середній час до WON: {avg_days} днів\n"
        f"Що це означає для команди? Це добре чи погано?"
    )

    comments["recruiters"] = _ai_comment(client, model,
        base +
        f"Ефективність рекрутерів:\n{rec_summary}\n\n"
        f"Хто лідер, хто відстає і що з цим робити?"
    )

    comments["funnel"] = _ai_comment(client, model,
        base +
        f"Етапи підбору кандидатів:\n{funnel_summary}\n\n"
        f"На якому етапі найбільше втрат і що це означає?"
    )

    comments["reasons"] = _ai_comment(client, model,
        base +
        f"Причини відмов:\n{reasons_summary}\n\n"
        f"Що є головною проблемою і як її вирішити?"
    )

    comments["conclusion"] = _ai_comment(client, model,
        f"Ти Senior Recruitment Analyst. Пиши українською.\n"
        f"Дані рекрутингової команди:\n"
        f"- Конверсія: {pct}% ({won} WON з {total} подач)\n"
        f"- Середній час до WON: {avg_days} днів\n"
        f"- Топ рекрутери: {rec_summary[:200]}\n"
        f"- Головні причини відмов: {reasons_summary[:200]}\n\n"
        f"Напиши загальний висновок: що найважливіше зараз, які 2-3 конкретні дії "
        f"допоможуть покращити результати. Максимум 5 речень."
    )

    return comments

def S(name, **kw):
    defaults = dict(fontName=FN, fontSize=9, textColor=C_TEXT, leading=14)
    defaults.update(kw)
    return ParagraphStyle(name, **defaults)

def header_table_style():
    return TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), C_SURF),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
        ('LEFTPADDING',   (0,0), (-1,-1), 16),
        ('RIGHTPADDING',  (0,0), (-1,-1), 16),
        ('TOPPADDING',    (0,0), (-1,-1), 14),
        ('BOTTOMPADDING', (0,0), (-1,-1), 14),
        ('LINEBELOW',     (0,0), (-1,-1), 2, C_ACCENT),
    ])


def data_table_style(rows_count, accent_col=None):
    style = [
        ('BACKGROUND',    (0,0), (-1,0),  C_SURF2),
        ('BACKGROUND',    (0,1), (-1,-1), C_SURF),
        ('ROWBACKGROUNDS',(0,1), (-1,-1), [C_SURF, colors.HexColor('#12191f')]),
        ('TEXTCOLOR',     (0,0), (-1,0),  C_MUTED2),
        ('FONTNAME',      (0,0), (-1,0),  FB),
        ('FONTSIZE',      (0,0), (-1,0),  8),
        ('TEXTCOLOR',     (0,1), (-1,-1), C_TEXT),
        ('FONTNAME',      (0,1), (-1,-1), FN),
        ('FONTSIZE',      (0,1), (-1,-1), 9),
        ('GRID',          (0,0), (-1,-1), 0.4, C_BORDER),
        ('LINEBELOW',     (0,0), (-1,0),  1,   C_ACCENT),
        ('LEFTPADDING',   (0,0), (-1,-1), 10),
        ('RIGHTPADDING',  (0,0), (-1,-1), 10),
        ('TOPPADDING',    (0,0), (-1,-1), 7),
        ('BOTTOMPADDING', (0,0), (-1,-1), 7),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
    ]
    return TableStyle(style)


def comment_box(text: str) -> Table:
    if not text:
        return Spacer(1, 1)
    label = Paragraph("AI Аналіз", S("lbl", fontName=FB, fontSize=8,
                                      textColor=C_ACCENT, leading=10))
    body  = Paragraph(text, S("cb", fontSize=9, textColor=C_TEXT,
                               leading=15, leftIndent=0))
    inner = Table([[label], [body]], colWidths=[CONTENT_W - 32])
    inner.setStyle(TableStyle([
        ('TOPPADDING',    (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('LEFTPADDING',   (0,0), (-1,-1), 0),
        ('RIGHTPADDING',  (0,0), (-1,-1), 0),
    ]))
    outer = Table([[inner]], colWidths=[CONTENT_W])
    outer.setStyle(TableStyle([
        ('BACKGROUND',   (0,0), (-1,-1), colors.HexColor('#0a1520')),
        ('LINEABOVE',    (0,0), (-1,0),  2, C_ACCENT),
        ('LINEBEFORE',   (0,0), (0,-1),  2, C_ACCENT),
        ('BOX',          (0,0), (-1,-1), 0.4, C_BORDER),
        ('LEFTPADDING',  (0,0), (-1,-1), 14),
        ('RIGHTPADDING', (0,0), (-1,-1), 14),
        ('TOPPADDING',   (0,0), (-1,-1), 10),
        ('BOTTOMPADDING',(0,0), (-1,-1), 10),
    ]))
    return outer


def section_title(text: str) -> Paragraph:
    return Paragraph(text.upper(), S("sec",
        fontName=FB, fontSize=10, textColor=C_ACCENT,
        leading=14, spaceBefore=8, spaceAfter=6,
    ))


def on_page(canvas, doc):
    canvas.saveState()
    canvas.setFillColor(C_BG)
    canvas.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)
    canvas.setStrokeColor(C_BORDER)
    canvas.setLineWidth(0.5)
    canvas.line(MARGIN, 13*mm, PAGE_W - MARGIN, 13*mm)
    canvas.setFont(FN, 7)
    canvas.setFillColor(C_MUTED)
    canvas.drawString(MARGIN, 9*mm, "RecruiterIQ Analytics Platform")
    canvas.drawRightString(PAGE_W - MARGIN, 9*mm, f"Сторінка {doc.page}")
    canvas.restoreState()


def generate_pdf_report(kpi_data: dict, period_label: str = "Всі дані",
                         ai_client=None, ai_model: str = "") -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=MARGIN, bottomMargin=22*mm,
        title="RecruiterIQ — Аналітичний звіт",
    )
    story = []

    comments = get_ai_comments(ai_client, ai_model, kpi_data) if ai_client else {}

    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    header = Table([[
        Paragraph(f'Recruit<font color="#00d4ff">IQ</font>',
                  S("logo", fontName=FB, fontSize=20, textColor=C_WHITE, leading=24)),
        Paragraph(
            f'Аналітичний звіт<br/>'
            f'<font color="#4a5968" size="9">{period_label} · {now}</font>',
            S("hr", fontSize=13, textColor=C_TEXT, leading=18, alignment=TA_RIGHT)
        )
    ]], colWidths=[CONTENT_W * 0.45, CONTENT_W * 0.55])
    header.setStyle(header_table_style())
    story.append(header)
    story.append(Spacer(1, 6*mm))

    conv     = (kpi_data.get("overall_conversion") or [{}])[0]
    t_cand   = (kpi_data.get("total_candidates")   or [{}])[0].get("n", "—")
    t_req    = (kpi_data.get("total_requests")     or [{}])[0].get("n", "—")
    a_req    = (kpi_data.get("active_requests")    or [{}])[0].get("n", "—")
    w_req    = (kpi_data.get("won_requests")       or [{}])[0].get("n", "—")
    c_pct    = conv.get("pct", "—")
    c_total  = conv.get("total", "—")
    avg_days = (kpi_data.get("avg_time_to_won")    or [{}])[0].get("days", "—")

    def kpi_cell(value, label, sub, accent):
        v = Paragraph(str(value), S("kv", fontName=FB, fontSize=22,
                                     textColor=accent, leading=26, alignment=TA_CENTER))
        l = Paragraph(label.upper(), S("kl", fontSize=8, textColor=C_MUTED2,
                                        leading=10, alignment=TA_CENTER))
        s = Paragraph(sub, S("ks", fontSize=8, textColor=C_MUTED,
                               leading=10, alignment=TA_CENTER))
        inner = Table([[v],[l],[s]], colWidths=[None])
        inner.setStyle(TableStyle([
            ('ALIGN',         (0,0),(-1,-1),'CENTER'),
            ('TOPPADDING',    (0,0),(-1,-1), 3),
            ('BOTTOMPADDING', (0,0),(-1,-1), 3),
        ]))
        outer = Table([[inner]], colWidths=[None])
        outer.setStyle(TableStyle([
            ('BACKGROUND',    (0,0),(-1,-1), C_SURF),
            ('LINEABOVE',     (0,0),(-1,0),  3, accent),
            ('BOX',           (0,0),(-1,-1), 0.4, C_BORDER),
            ('TOPPADDING',    (0,0),(-1,-1), 12),
            ('BOTTOMPADDING', (0,0),(-1,-1), 12),
            ('LEFTPADDING',   (0,0),(-1,-1), 8),
            ('RIGHTPADDING',  (0,0),(-1,-1), 8),
        ]))
        return outer

    kpi_row = Table([[
        kpi_cell(t_cand,   "Кандидатів",  "у пулі",          C_ACCENT),
        kpi_cell(t_req,    "Вакансій",    f"{a_req} активних", C_GREEN),
        kpi_cell(w_req,    "WON",         "закрито",          C_PURPLE),
        kpi_cell(f"{c_pct}%", "Конверсія", f"{c_total} подач", C_YELLOW),
        kpi_cell(avg_days, "Днів до WON", "середнє",          C_RED),
    ]], colWidths=[CONTENT_W/5]*5)
    kpi_row.setStyle(TableStyle([
        ('LEFTPADDING',  (0,0),(-1,-1), 3),
        ('RIGHTPADDING', (0,0),(-1,-1), 3),
        ('VALIGN',       (0,0),(-1,-1), 'TOP'),
    ]))
    story.append(section_title("Ключові показники"))
    story.append(kpi_row)
    story.append(Spacer(1, 4*mm))

    if comments.get("kpi"):
        story.append(comment_box(comments["kpi"]))
    story.append(Spacer(1, 6*mm))
    recruiters = kpi_data.get("top_recruiters") or []
    if recruiters:
        story.append(section_title("Рекрутери — ефективність"))
        rows = [["Рекрутер", "Подачі", "WON", "LOST", "Win Rate"]]
        for r in recruiters:
            rate = r.get("win_rate", 0) or 0
            rows.append([
                str(r.get("name", "—")),
                str(r.get("total", 0)),
                str(r.get("won", 0)),
                str(r.get("lost", 0)),
                f"{rate}%",
            ])

        t = Table(rows, colWidths=[
            CONTENT_W*0.40, CONTENT_W*0.15,
            CONTENT_W*0.15, CONTENT_W*0.15, CONTENT_W*0.15
        ])
        ts = data_table_style(len(rows))
        for i, r in enumerate(recruiters, start=1):
            rate = r.get("win_rate", 0) or 0
            c = C_GREEN if rate >= 20 else (C_YELLOW if rate >= 10 else C_RED)
            ts.add('TEXTCOLOR', (4, i), (4, i), c)
            ts.add('FONTNAME',  (4, i), (4, i), FB)
        t.setStyle(ts)
        story.append(t)
        story.append(Spacer(1, 4*mm))

        if comments.get("recruiters"):
            story.append(comment_box(comments["recruiters"]))
        story.append(Spacer(1, 6*mm))

    funnel = kpi_data.get("funnel") or []
    if funnel:
        story.append(section_title("Етапи підбору"))
        order = ['NEWLY_ATTACHED_TO_REQUEST','CV_SENT_TO_THE_CLIENT',
                 'FIRST_INTERVIEW','INTERVIEW_WITH_CLIENT',
                 'SECOND_INTERVIEW','FINAL_INTERVIEW','WON','LOST']
        labels = {
            'NEWLY_ATTACHED_TO_REQUEST': 'Приєднано до вакансії',
            'CV_SENT_TO_THE_CLIENT':     'CV відправлено клієнту',
            'FIRST_INTERVIEW':           'Перше інтерв\'ю',
            'INTERVIEW_WITH_CLIENT':     'Інтерв\'ю з клієнтом',
            'SECOND_INTERVIEW':          'Друге інтерв\'ю',
            'FINAL_INTERVIEW':           'Фінальне інтерв\'ю',
            'WON':                       'Успішно (WON)',
            'LOST':                      'Відмова (LOST)',
        }
        funnel_map = {r['status']: r['cnt'] for r in funnel}
        total_f = funnel_map.get('NEWLY_ATTACHED_TO_REQUEST', 1) or 1

        rows = [["Етап", "Кількість", "% від початку"]]
        for s in order:
            if s not in funnel_map:
                continue
            cnt = funnel_map[s]
            pct_f = round(cnt / total_f * 100, 1)
            rows.append([labels.get(s, s), str(cnt), f"{pct_f}%"])

        t = Table(rows, colWidths=[CONTENT_W*0.55, CONTENT_W*0.22, CONTENT_W*0.23])
        ts = data_table_style(len(rows))
        for i, s in enumerate([s for s in order if s in funnel_map], start=1):
            if s == 'WON':
                ts.add('TEXTCOLOR', (0,i), (-1,i), C_GREEN)
                ts.add('FONTNAME',  (0,i), (-1,i), FB)
            elif s == 'LOST':
                ts.add('TEXTCOLOR', (0,i), (-1,i), C_RED)
                ts.add('FONTNAME',  (0,i), (-1,i), FB)
        t.setStyle(ts)
        story.append(t)
        story.append(Spacer(1, 4*mm))

        if comments.get("funnel"):
            story.append(comment_box(comments["funnel"]))
        story.append(Spacer(1, 6*mm))

    reasons = kpi_data.get("top_loss_reasons") or []
    if reasons:
        story.append(section_title("Причини відмов"))
        reason_labels = {
            'NO_ANSWER':                          'Немає відповіді',
            'INSUFFICIENT_SKILLS':                'Недостатньо скілів',
            'CANDIDATE_WAS_NOT_CONSIDERED':        'Кандидат не розглянутий',
            'CLIENT_CLOSED_DEAL':                 'Клієнт закрив угоду',
            'NO_RELEVANT_EXPERIENCE':             'Немає релевантного досвіду',
            'RATE_TOO_HIGH':                      'Завищений рейт',
            'CANDIDATE_REFUSED_FROM_REQUEST':     'Кандидат відмовився',
            'TOO_LATE':                           'Запізно',
            'BAD_ENGLISH':                        'Погана англійська',
            'CLIENT_CLOSED_POSITION':             'Клієнт закрив позицію',
            'CLIENT_CLOSED_POSITION_ON_THEIR_OWN':'Клієнт закрив самостійно',
            'LOCATION_DID_NOT_MATCH':             'Локація не підходить',
            'BAD_SOFT_SKILLS':                    'Погані soft skills',
            'LOW_MATCH_WITH_REQUEST_STACK':       'Не відповідає стеку',
            'CANDIDATE_IGNORED_US':               'Кандидат ігнорує',
            'NO_FEEDBACK_FROM_CLIENT':            'Немає відповіді від клієнта',
            'TOO_EXPENSIVE':                      'Занадто дорого',
        }
        total_r = sum(r['cnt'] for r in reasons) or 1

        rows = [["Причина відмови", "Кількість", "% від відмов"]]
        for r in reasons:
            label = reason_labels.get(r['reason'], r['reason'].replace('_', ' '))
            pct_r = round(r['cnt'] / total_r * 100, 1)
            rows.append([label, str(r['cnt']), f"{pct_r}%"])

        t = Table(rows, colWidths=[CONTENT_W*0.58, CONTENT_W*0.20, CONTENT_W*0.22])
        t.setStyle(data_table_style(len(rows)))
        story.append(t)
        story.append(Spacer(1, 4*mm))

        if comments.get("reasons"):
            story.append(comment_box(comments["reasons"]))
        story.append(Spacer(1, 6*mm))

    if comments.get("conclusion"):
        story.append(section_title("Загальний висновок"))
        conclusion = Table([[
            Paragraph(comments["conclusion"],
                      S("conc", fontSize=10, textColor=C_TEXT, leading=17))
        ]], colWidths=[CONTENT_W])
        conclusion.setStyle(TableStyle([
            ('BACKGROUND',    (0,0),(-1,-1), C_SURF),
            ('BOX',           (0,0),(-1,-1), 0.4, C_BORDER),
            ('LINEABOVE',     (0,0),(-1,0),  3, C_GREEN),
            ('LEFTPADDING',   (0,0),(-1,-1), 16),
            ('RIGHTPADDING',  (0,0),(-1,-1), 16),
            ('TOPPADDING',    (0,0),(-1,-1), 14),
            ('BOTTOMPADDING', (0,0),(-1,-1), 14),
        ]))
        story.append(conclusion)

    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
    buf.seek(0)
    return buf.read()