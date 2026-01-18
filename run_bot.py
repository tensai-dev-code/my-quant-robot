import os, sys, json
import FinanceDataReader as fdr
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

# 1. 환경 변수 로드
raw_json = os.environ.get('GCP_SERVICE_ACCOUNT') 
if not raw_json:
    raw_json = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')

# [추가] SPREADSHEET_ID를 환경 변수에서 가져오는 코드
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

if not raw_json:
    print("에러: 시크릿 데이터를 찾을 수 없습니다.")
    sys.exit(1)
if not SPREADSHEET_ID:
    print("에러: SPREADSHEET_ID가 설정되지 않았습니다.")
    sys.exit(1)

try:
    SERVICE_ACCOUNT_INFO = json.loads(raw_json)
except json.JSONDecodeError as e:
    print(f"에러: JSON 형식이 올바르지 않습니다.")
    sys.exit(1)

# 2. 구글 시트 연결 함수
def get_worksheet(sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_INFO, scope)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SPREADSHEET_ID)
    return doc.worksheet(sheet_name)

# 3. 전체 종목 업데이트 함수
def update_all_stocks():
    print("Step 1: 전체 종목 수집 중...")
    df = fdr.StockListing('KRX') 
    ws = get_worksheet("전체종목")
    ws.clear()
    # NaN(결측치) 값을 빈 문자열로 변환해야 구글 시트 업데이트 시 에러가 안 납니다.
    df = df.fillna('')
    ws.update([df.columns.values.tolist()] + df.values.tolist())
    print("성공: '전체종목' 시트 업데이트 완료.")

# 4. 퀀트 대상 추출 함수
def update_quant_target():
    print("Step 2: 퀀트 대상 추출 시작...")
    all_ws = get_worksheet("전체종목")
    data = all_ws.get_all_records()
    if not data:
        print("에러: '전체종목' 시트에 데이터가 없습니다. 먼저 전체 수집을 실행하세요.")
        return
        
    df = pd.DataFrame(data)
    
    # 숫자형 변환
    numeric_cols = ['Close', 'ChagesRatio', 'Volume', 'Amount', 'Marcap', 'Stocks']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 필터링 로직
    upper_marcap = df['Marcap'].quantile(0.8)
    quant_df = df[
        (df['Marcap'] >= upper_marcap) &
        (df['Volume'] > 50000) &
        (df['Market'].isin(['KOSPI', 'KOSDAQ'])) &
        (~df['Name'].str.contains('스팩|제[0-9]+호|우$|우[A-C]$'))
    ].copy()

    quant_df = quant_df.sort_values(by='Amount', ascending=False).head(100)
    
    target_ws = get_worksheet("퀀트대상")
    target_ws.clear()
    
    if not quant_df.empty:
        # 데이터프레임 내 NaN 처리 후 전송
        quant_df = quant_df.fillna('')
        target_ws.update([quant_df.columns.values.tolist()] + quant_df.values.tolist())
        print(f"성공: {len(quant_df)}개 종목 저장 완료.")
    else:
        print("조건에 맞는 종목이 없습니다.")

# 5. 메인 실행부
if __name__ == "__main__":
    job_type = sys.argv[1] if len(sys.argv) > 1 else 'all_stocks'
    
    if job_type == 'all_stocks':
        update_all_stocks()
    elif job_type == 'quant_target':
        update_quant_target()
