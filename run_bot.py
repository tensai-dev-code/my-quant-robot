import os, sys, json
import FinanceDataReader as fdr
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

# 환경 변수 가져오기
raw_json = os.environ.get('GCP_SERVICE_ACCOUNT')

# 디버깅을 위한 체크
if not raw_json:
    print("에러: GCP_SERVICE_ACCOUNT Secrets가 설정되지 않았습니다.")
    sys.exit(1)

try:
    SERVICE_ACCOUNT_INFO = json.loads(raw_json)
except json.JSONDecodeError as e:
    print(f"에러: JSON 형식이 올바르지 않습니다. 첫 글자 확인: {raw_json[:1]}")
    print(f"상세 에러: {e}")
    sys.exit(1)

SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

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
    print("Step 2: 퀀트 대상 추출 시작...")
    # '전체종목' 시트 데이터 불러오기
    all_ws = get_worksheet("전체종목")
    df = pd.DataFrame(all_ws.get_all_records())
    
    # 데이터 전처리: 숫자형이어야 할 컬럼들을 강제로 변환 (쉼표 등 제거)
    numeric_cols = ['Close', 'ChagesRatio', 'Volume', 'Amount', 'Marcap', 'Stocks']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # --- [퀀트 필터링 조건 설정] ---
    
    # 1. 시가총액 하위 20% 및 초소형주 제외 (변동성 위험 방지)
    #    통상적으로 시총 상위 20%~50% 내에서 고르는 것이 안정적입니다.
    upper_marcap = df['Marcap'].quantile(0.8) # 상위 20% 기준선
    
    # 2. 필터링 조건 결합
    quant_df = df[
        (df['Marcap'] >= upper_marcap) &       # 시가총액 상위 20% 이내 (대형/중형주)
        (df['Volume'] > 50000) &               # 일 거래량 5만 주 이상 (환금성 확보)
        (df['Market'].isin(['KOSPI', 'KOSDAQ'])) & # 코넥스 제외
        (~df['Name'].str.contains('스팩|제[0-9]+호|우$|우[A-C]$')) # 스팩주, 우선주 제외
    ].copy()

    # 3. 랭킹 부여 (예: 거래대금 상위 순으로 정렬)
    #    단순 필터링 후 거래대금(Amount)이 높은 순서로 정렬하여 시장 주도주 포착
    quant_df = quant_df.sort_values(by='Amount', ascending=False).head(100)
    
    # ------------------------------

    target_ws = get_worksheet("퀀트대상")
    target_ws.clear()
    
    if not quant_df.empty:
        target_ws.update([quant_df.columns.values.tolist()] + quant_df.values.tolist())
        print(f"성공: 필터링된 {len(quant_df)}개 종목을 '퀀트대상' 시트에 저장했습니다.")
    else:
        print("조건에 맞는 종목이 없습니다.")
