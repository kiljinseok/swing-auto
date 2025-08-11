import os, io, json, requests, pandas as pd
from datetime import datetime

REST = os.environ["KAKAO_REST_KEY"]
RT   = os.environ["KAKAO_REFRESH_TOKEN"]
CSV  = os.environ["SHEET_CSV_URL"]

def refresh_access_token():
    r = requests.post(
        "https://kauth.kakao.com/oauth/token",
        data={"grant_type":"refresh_token","client_id":REST,"refresh_token":RT},
        timeout=20
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
    r = requests.post(url, headers=headers, data=data, timeout=20)
    r.raise_for_status()

def format_message(df: pd.DataFrame) -> str:
    lines = ["[스윙매매 프로젝트] KOSPI Top100 매수 후보 (15:20 스캔)"]
    for i, r in enumerate(df.head(3).to_dict("records"), 1):  # 최대 3종목
        name   = r.get("name",""); price  = r.get("price","")
        stop   = r.get("stop",""); target = r.get("target","")
        score  = r.get("score",""); reason = r.get("reason","")
        try:    price  = f"{int(round(float(price))):,}원"
        except: pass
        try:    stop   = f"{int(round(float(stop))):,}"
        except: pass
        try:    target = f"{int(round(float(target))):,}"
        except: pass
        lines.append(f"{i}) {name} {price} | 손절:{stop} | 목표:{target} | 강도:{score} | {reason}")
    lines.append("※ 종가는 장마감가로 확정, 슬리피지 가능. 손절/목표는 마감 후 재확인 권장.")
    return "\n".join(lines)

def save_history(text: str):
    os.makedirs("history", exist_ok=True)
    path = f"history/{datetime.now().strftime('%Y-%m-%d')}_picks.txt"
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"[history] wrote {path}")

def main():
    # 예외가 나면 스택트레이스를 출력하고 exit(1) 하도록 구성
    try:
        at = refresh_access_token()
        csv_text = requests.get(CSV, timeout=30).text
        df = pd.read_csv(io.StringIO(csv_text))
        msg = format_message(df)
        save_history(msg)
        send_to_me(at, msg)
        print("Done.")
    except Exception as e:
        import traceback, sys
        traceback.print_exc()
        print(f"ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

