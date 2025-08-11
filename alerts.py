import os
import io
import json
import requests
import pandas as pd
from datetime import datetime

# ── Secrets (GitHub Actions에서 env로 주입)
REST = os.environ["KAKAO_REST_KEY"]
RT   = os.environ["KAKAO_REFRESH_TOKEN"]
CSV  = os.environ["SHEET_CSV_URL"]

# ── Kakao OAuth: refresh_token -> access_token
def refresh_access_token():
    r = requests.post(
        "https://kauth.kakao.com/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": REST,
            "refresh_token": RT
        },
        timeout=20
    )
    r.raise_for_status()
    return r.json()["access_token"]

# ── 카카오 나에게 보내기 (UTF-8 명시)
def send_to_me(access_token: str, text: str):
    url = "https://kapi.kakao.com/v2/api/talk/memo/default/send"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8"
    }
    template_object = {
        "object_type": "text",
        "text": text,
        "link": {
            "web_url": "https://chat.openai.com",
            "mobile_web_url": "https://chat.openai.com"
        },
        "button_title": "열기"
    }
    data = {"template_object": json.dumps(template_object, ensure_ascii=False)}
    r = requests.post(url, headers=headers, data=data, timeout=20)
    r.raise_for_status()

# ── 메시지 생성 (최대 3종목, KOSPI 시총 200 이내 필터)
def format_message(df: pd.DataFrame) -> str:
    lines = ["[스윙매매 프로젝트] KOSPI 시총 200위 이내 매수 후보 (최대 3종목)"]

    # 시총순위 컬럼이 있으면 200위 이내만 필터
    for cand in ("mcap_rank", "시총순위", "시가총액순위"):
        if cand in df.columns:
            df = df[pd.to_numeric(df[cand], errors="coerce") <= 200]
            break

    # 점수(강도) 높은 순으로 정렬
    for sc in ("score", "강도", "점수"):
        if sc in df.columns:
            df = df.sort_values(by=sc, ascending=False)
            break

    # 최대 3종목만 선택
    for i, r in enumerate(df.head(3).to_dict("records"), 1):
        name   = r.get("name") or r.get("종목명") or ""
        price  = r.get("price") or r.get("현재가") or ""
        stop   = r.get("stop") or r.get("손절") or ""
        target = r.get("target") or r.get("목표") or ""
        score  = r.get("score") or r.get("강도") or ""
        reason = r.get("reason") or r.get("사유") or ""

        def fmt_int(x, suffix=""):
            try:
                return f"{int(round(float(str(x).replace(',', '').strip()))):,}{suffix}"
            except Exception:
                return str(x)

        price  = fmt_int(price, "원") if price != "" else ""
        stop   = fmt_int(stop)       if stop  != "" else ""
        target = fmt_int(target)     if target!= "" else ""

        lines.append(
            f"{i}) {name} {price} | 손절:{stop} | 목표:{target} | 강도:{score} | {reason}"
        )

    lines.append("※ 종가는 장마감가로 확정, 슬리피지 가능. 손절/목표는 마감 후 재확인 권장.")
    return "\n".join(lines)

# ── 추천 결과를 저장
def save_history(text: str):
    os.makedirs("history", exist_ok=True)
    path = f"history/{datetime.now().strftime('%Y-%m-%d')}_picks.txt"
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    print(f"[history] wrote {path}")

def main():
    # 1) 액세스 토큰 발급
    at = refresh_access_token()

    # 2) CSV 가져오기 (UTF-8 + BOM, 필요시 EUC-KR 폴백)
    r = requests.get(CSV, timeout=30)
    r.raise_for_status()
    csv_text = r.content.decode("utf-8-sig", errors="replace")
    if csv_text.count("�") > 5:  # 손상문자 많으면 EUC-KR 재시도
        try:
            csv_text = r.content.decode("euc-kr")
        except Exception:
            pass

    df = pd.read_csv(io.StringIO(csv_text))

    # 3) 메시지 생성 + 저장 + 전송
    msg = format_message(df)
    save_history(msg)
    send_to_me(at, msg)
    print("Done.")

if __name__ == "__main__":
    main()

