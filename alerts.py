import os, io, re, json, time, requests, pandas as pd
from typing import Dict, Optional

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
    # '★★★★★', '★★★★☆' → 별 개수로 점수화 (없으면 0)
    if not isinstance(score, str): return 0.0
    stars = score.count("★") + 0.5*score.count("☆")  # ☆ 반점 처리
    return float(stars)

# ---------- Market-cap rank from Naver ----------
def fetch_kospi_ranks(limit_pages: int = 10) -> pd.DataFrame:
    """
    네이버금융 KOSPI 시가총액 페이지에서 종목명/코드/시총순위를 수집
    - page=1..limit_pages (일반적으로 1~10이면 200위까지 커버)
    """
    rows = []
    base = "https://finance.naver.com/sise/sise_market_sum.naver?sosok=0&page={page}"
    # 중요: EUC-KR
    for p in range(1, limit_pages + 1):
        url = base.format(page=p)
        resp = requests.get(url, timeout=15)
        resp.encoding = "euc-kr"
        html = resp.text
        # 아주 간단한 파서: 표 행에서 종목코드/이름 추출
        # 코드 a href="/item/main.naver?code=005930"
        for m in re.finditer(r'/item/main\.naver\?code=(\d{6})".*?>([^<]+)</a>', html, flags=re.S):
            code = m.group(1)
            name = m.group(2).strip()
            rows.append({"code": code, "name": name})
        time.sleep(0.2)  # 예의상 살짝 대기
    if not rows:
        return pd.DataFrame(columns=["rank", "code", "name"])

    # 각 페이지가 대략 시총 순으로 나열 → 등장 순서로 순위 부여 (중복 제거)
    df = pd.DataFrame(rows).drop_duplicates(subset=["code"]).reset_index(drop=True)
    df.insert(0, "rank", df.index + 1)  # 1부터
    # 상위 200만 남김
    df = df[df["rank"] <= 200].copy()
    # 정규화 컬럼(공백 제거)
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
    # 필수 컬럼 보정
    for col in ["name", "price", "stop", "target", "score", "reason"]:
        if col not in df.columns:
            df[col] = None
    # 정규화 키
    df["name_key"] = df["name"].astype(str).map(_clean)
    # 점수 수치화(정렬용)
    df["score_num"] = df["score"].map(_score_to_num)
    return df

def filter_top3_kospi200(cands: pd.DataFrame, kospi200: pd.DataFrame) -> pd.DataFrame:
    # 이름 기준 매칭 (필요하면 나중에 ticker 컬럼도 지원)
    merged = cands.merge(kospi200[["rank", "name_key"]], on="name_key", how="inner")
    # 우선 순위: score_num 내림차순 → rank 오름차순
    merged = merged.sort_values(by=["score_num", "rank"], ascending=[False, True])
    # 최대 3개
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

# ---------- Main ----------
def main():
    at = refresh_access_token()
    try:
        cands = read_candidates_csv(CSV)
    except Exception:
        send_to_me(at, "오늘은 추천 종목이 없습니다. (CSV 접근 실패)")
        return

    try:
        ranks = fetch_kospi_ranks(limit_pages=10)  # 1~10페이지 ≈ Top200
    except Exception:
        # 순위 페이지 이슈 시, 안전하게 '추천 없음'
        send_to_me(at, "오늘은 추천 종목이 없습니다. (시총 순위 조회 실패)")
        return

    top3 = filter_top3_kospi200(cands, ranks)
    msg = format_message(top3 if top3 is not None else pd.DataFrame())
    send_to_me(at, msg)

if __name__ == "__main__":
    main()

