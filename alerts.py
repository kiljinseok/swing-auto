import os, io, re, json, time, requests, pandas as pd
from typing import Dict
from datetime import datetime, timedelta, timezone

# === KST 시간대 (✅ 기록 파일명에 한국시간 사용)
KST = timezone(timedelta(hours=9))

# === Secrets ===
REST  = os.environ["KAKAO_REST_KEY"]
RT    = os.environ["KAKAO_REFRESH_TOKEN"]
CSV   = os.environ["SHEET_CSV_URL"]

# ---------- Kakao ----------
def refresh_access_token() -> str:
    r = requests.post(
        "https://kauth.kakao.com/oauth/token",
        data={"grant_type": "refresh_token", "client_id": REST, "refresh_token": RT},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def send_to_me(access_token: str, text: str):
    url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
    }
    tpl = {
        "object_type": "text",
        "text": text,
        "link": {"web_url": "https://chat.openai.com", "mobile_web_url": "https://chat.openai.com"},
        "button_title": "열기",
    }
    data = {"template_object": json.dumps(tpl, ensure_ascii=False)}
    r = requests.post(url, headers=headers, data=data, timeout=15)
    r.raise_for_status()

# ---------- Utils ----------
def _clean(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).strip()

def _score_to_num(score) -> float:
    if not isinstance(score, str): return 0.0
    stars = score.count("★") + 0.5*score.count("☆")
    return float(stars)

# ---------- Market-cap rank from Naver ----------
def fetch_kospi_ranks(limit_pages: int = 10) -> pd.DataFrame:
    rows = []
    base = "https://finance.naver.com/sise/sise_market_sum.naver?sosok=0&page={page}"
    for p in range(1, limit_pages + 1):
        url = base.format(page=p)
        resp = requests.get(url, timeout=15)
        resp.encoding = "euc-kr"
        html = resp.text
        for m in re.finditer(r'/item/main\.naver\?code=(\d{6})".*?>([^<]+)</a>', html, flags=re.S):
            code = m.group(1)
            name = m.group(2).strip()
            rows.append({"code": code, "name": name})
        time.sleep(0.2)
    if not rows:
        return pd.DataFrame(columns=["rank", "code", "name"])

    df = pd.DataFrame(rows).drop_duplicates(subset=["code"]).reset_index(drop=True)
    df.insert(0, "rank", df.index + 1)
    df = df[df["rank"] <= 200].copy()
    df["name_key"] = df["name"].map(_clean)
    return df[["rank", "code", "name", "name_key"]]

# ---------- CSV candidates ----------
def read_candidates_csv(csv_url: str) -> pd.DataFrame:
    resp = requests.get(csv_url, timeout=15)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    raw = resp.text
    try:
        df = pd.read_csv(io.StringIO(raw), encoding="utf-8-sig")
    except Exception:
        df = pd.read_csv(io.StringIO(raw), encoding="cp949")

    for col in ["name", "price", "stop", "target", "score", "reason"]:
        if col not in df.columns:
            df[col] = None

    df["name_key"] = df["name"].astype(str).map(_clean)
    df["score_num"] = df["score"].map(_score_to_num)
    return df

def filter_top3_kospi200(cands: pd.DataFrame, kospi200: pd.DataFrame) -> pd.DataFrame:
    merged = cands.merge(kospi200[["rank", "name_key"]], on="name_key", how="inner")
    merged = merged.sort_values(by=["score_num", "rank"], ascending=[False, True])
    return merged.head(3).copy()

# ---------- Message ----------
def format_message(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "오늘은 추천 종목이 없습니다."
    lines = ["[스윙매매 프로젝트] KOSPI 시총 200위 이내 매수 후보 (최대 3종목)"]
    for i, r in enumerate(df.to_dict("records"), 1):
        name   = r.get("name", "")
        price  = r.get("price", "")
        stop   = r.get("stop", "")
        target = r.get("target", "")
        score  = r.get("score", "")
        reason = r.get("reason", "")
        rank   = r.get("rank", "")
        try:   price  = f"{int(round(float(price))):,}원"
        except: pass
        try:   stop   = f"{int(round(float(stop))):,}"
        except: pass
        try:   target = f"{int(round(float(target))):,}"
        except: pass
        tag = f"(시총순위 #{rank})" if rank else ""
        lines.append(f"{i}) {name} {price} | 손절:{stop} | 목표:{target} | 강도:{score} {tag} | {reason}")
    lines.append("※ 종가는 장마감가로 확정, 슬리피지 가능. 손절/목표는 마감 후 재확인 권장.")
    return "\n".join(lines)

# ---------- History (✅ 추가) ----------
def save_history(df: pd.DataFrame):
    """
    추천 종목이 있을 때만 history/YYYY-MM-DD.csv 로 저장 (KST 날짜 기준)
    """
    if df is None or df.empty:
        return
    # 기록 폴더 생성
    os.makedirs("history", exist_ok=True)
    d = datetime.now(KST).strftime("%Y-%m-%d")
    path = os.path.join("history", f"{d}.csv")
    cols = ["name","price","stop","target","score","reason","rank"]
    # 누적 기록이 필요하면 mode="a", header=not os.path.exists(path) 로 변경 가능
    df[cols].to_csv(path, index=False, encoding="utf-8-sig")

# ---------- Main ----------
def main():
    at = refresh_access_token()
    try:
        cands = read_candidates_csv(CSV)
    except Exception:
        send_to_me(at, "오늘은 추천 종목이 없습니다. (CSV 접근 실패)")
        return

    try:
        ranks = fetch_kospi_ranks(limit_pages=10)  # ≈ Top200
    except Exception:
        send_to_me(at, "오늘은 추천 종목이 없습니다. (시총 순위 조회 실패)")
        return

    top3 = filter_top3_kospi200(cands, ranks)

    # ✅ 추천 내역 저장
    save_history(top3)

    # 카톡 발송
    msg = format_message(top3 if top3 is not None else pd.DataFrame())
    send_to_me(at, msg)

if __name__ == "__main__":
    main()
# alerts.py 안에서 추천 결과 문자열이 msg 라고 가정
from datetime import datetime
import os
os.makedirs("history", exist_ok=True)
with open(f"history/{datetime.now().strftime('%Y-%m-%d')}_picks.txt", "w", encoding="utf-8") as f:
    f.write(msg)

