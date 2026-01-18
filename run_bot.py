import os, sys, json
import FinanceDataReader as fdr
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

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

def update_yearly_data():
    print("Step 3: 퀀트 대상 종목의 1년치 주가 수집 시작...")
    
    # 1. '퀀트대상' 시트에서 종목 코드 가져오기
    quant_ws = get_worksheet("퀀트대상")
    quant_data = quant_ws.get_all_records()
    
    if not quant_data:
        print("에러: '퀀트대상' 데이터가 없습니다.")
        return

    df_quant = pd.DataFrame(quant_data)
    # 컬럼 이름이 'Code'인 것을 확인하세요 (FinanceDataReader 기본값)
    target_codes = df_quant['Code'].astype(str).tolist()

    # 2. 날짜 설정 (오늘 기준 1년 전)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

    all_history = []
    
    print(f"{len(target_codes)}개 종목 수집 중...")
    for code in target_codes:
        try:
            # 6자리 코드가 되도록 zfill(6) 처리
            clean_code = code.zfill(6)
            df_hist = fdr.DataReader(clean_code, start_date, end_date)
            
            if not df_hist.empty:
                df_hist = df_hist.reset_index() # 날짜(Date)를 컬럼으로 변경
                df_hist['Code'] = clean_code
                # 데이터 양이 너무 많을 수 있으므로 필요한 컬럼만 선택 (옵션)
                # df_hist = df_hist[['Date', 'Code', 'Open', 'High', 'Low', 'Close', 'Volume']]
                all_history.append(df_hist)
        except Exception as e:
            print(f"종목코드 {code} 수집 중 오류: {e}")

    # 3. 모든 데이터를 하나로 합치기
    if all_history:
        final_df = pd.concat(all_history, ignore_index=True)
        final_df['Date'] = final_df['Date'].dt.strftime('%Y-%m-%d') # 날짜 형식 변환
        final_df = final_df.fillna('')

        # 4. '수집대상' 시트에 저장
        # 데이터가 수만 줄이 될 수 있으므로 시트 용량 주의
        target_ws = get_worksheet("수집대상")
        target_ws.clear()
        
        # 구글 시트 업데이트 제한을 피하기 위해 리스트로 변환하여 전송
        data_to_send = [final_df.columns.values.tolist()] + final_df.values.tolist()
        target_ws.update(data_to_send)
        print(f"성공: 총 {len(final_df)}행의 주가 데이터를 '수집대상' 시트에 저장했습니다.")
    else:
        print("수집된 데이터가 없습니다.")

# 메인 실행부에 추가
if __name__ == "__main__":
    job_type = sys.argv[1] if len(sys.argv) > 1 else 'all_stocks'
    
    if job_type == 'all_stocks':
        update_all_stocks()
    elif job_type == 'quant_target':
        update_quant_target()
    elif job_type == 'yearly_data': # 추가
        update_yearly_data()
