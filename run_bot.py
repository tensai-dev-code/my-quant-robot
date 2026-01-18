import os, sys, json
import FinanceDataReader as fdr
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

# 환경 변수에서 설정 로드
SERVICE_ACCOUNT_INFO = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT'))
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

def get_worksheet(sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_INFO, scope)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SPREADSHEET_ID)
    return doc.worksheet(sheet_name)

def update_all_stocks():
    print("Step 1: 전체 종목 수집 중...")
    df = fdr.StockListing('KRX') # 상장 종목 전체
    ws = get_worksheet("전체종목")
    ws.clear()
    ws.update([df.columns.values.tolist()] + df.values.tolist())
    print("성공: '전체종목' 시트 업데이트 완료.")

def update_quant_target():
    print("Step 2: 퀀트 대상 추출 중...")
    # '전체종목' 시트 데이터 기반 필터링
    all_ws = get_worksheet("전체종목")
    df = pd.DataFrame(all_ws.get_all_records())
    
    # 퀀트 필터 예시: 시가총액 상위 200개 & 거래소(Market)가 KOSPI인 종목
    # (본인의 퀀트 로직으로 아래 조건을 수정하세요)
    quant_df = df[df['Market'] == 'KOSPI'].nlargest(200, 'MarCap')
    
    target_ws = get_worksheet("퀀트대상")
    target_ws.clear()
    target_ws.update([quant_df.columns.values.tolist()] + quant_df.values.tolist())
    print(f"성공: {len(quant_df)}개 종목 '퀀트대상' 시트 업데이트 완료.")

if __name__ == "__main__":
    job_type = sys.argv[1] if len(sys.argv) > 1 else 'all_stocks'
    
    if job_type == 'all_stocks':
        update_all_stocks()
    elif job_type == 'quant_target':
        update_quant_target()
