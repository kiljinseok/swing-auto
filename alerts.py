import os, io, json, requests, pandas as pd

REST  = os.environ["KAKAO_REST_KEY"]
RT    = os.environ["KAKAO_REFRESH_TOKEN"]
CSV   = os.environ["SHEET_CSV_URL"]

def refresh_access_token():
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
        # ✅ UTF-8 명시 (중요)
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
    }
    tpl = {
        "object_type": "text",
        "text": text,
        "link": {
            "web_url": "https://chat.openai.com",
            "mobile_web_url": "https://chat.openai.com",
        },
        "button_title": "열기",
    }
    # ✅ ensure_ascii=False (중요)
    data = {"template_object": json.dumps(tpl, ensure_ascii=False)}

    r = requests.post(url, headers=headers, data=data, timeout=10)
    r.raise_for_status()

def format_message(df: pd.DataFrame) -> str:
    lines = ["[스윙매매 프로젝트] KOSPI Top100 매수 후보 (15:20 스캔)"]
    for i, r in enumerate(df.head(5).to_dict("records"), 1):
        name   = r.get("name", "")
        price  = r.get("price", "")
        stop   = r.get("stop", "")
        target = r.get("target", "")
        score  = r.get("score", "")
        reason = r.get("reason", "")
        try:   price  = f"{int(round(float(price))):,}원"
        except: pass
        try:   stop   = f"{int(round(float(stop))):,}"
        except: pass
        try:   target = f"{int(round(float(target))):,}"
        except: pass
        lines.append(f"{i}) {name} {price} | 손절:{stop} | 목표:{target} | 강도:{score} | {reason}")
    lines.append("※ 종가는 장마감가로 확정, 슬리피지 가능. 손절/목표는 마감 후 재확인 권장.")
    return "\n".join(lines)

def main():
    at = refresh_access_token()

    # ✅ CSV 인코딩 보정
    resp = requests.get(CSV, timeout=10)
    # 우선 utf-8로 시도, 필요하면 아래 라인 주석 해제해서 cp949로 테스트
    resp.encoding = "utf-8"  # or "utf-8-sig"
    csv_text = resp.text

    # BOM 가능성 대비 utf-8-sig 먼저 시도
    try:
        df = pd.read_csv(io.StringIO(csv_text), encoding="utf-8-sig")
    except Exception:
        # 드물게 cp949인 경우가 있어 예외 시 백업 전략
        df = pd.read_csv(io.StringIO(csv_text), encoding="cp949")

    msg = format_message(df)
    send_to_me(at, msg)

if __name__ == "__main__":
    main()

