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
    print("Step 2: 퀀트 대상 추출 시작 (250개)...")
    all_ws = get_worksheet("전체종목")
    df = pd.DataFrame(all_ws.get_all_records())
    
    # 숫자형 변환
    numeric_cols = ['Close', 'ChagesRatio', 'Volume', 'Amount', 'Marcap', 'Stocks']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    upper_marcap = df['Marcap'].quantile(0.8)
    quant_df = df[
        (df['Marcap'] >= upper_marcap) &
        (df['Volume'] > 50000) &
        (df['Market'].isin(['KOSPI', 'KOSDAQ'])) &
        (~df['Name'].str.contains('스팩|제[0-9]+호|우$|우[A-C]$'))
    ].copy()

    # 추출 개수를 250개로 하향 조정 (상위 250개)
    quant_df = quant_df.sort_values(by='Amount', ascending=False).head(250)
    
    target_ws = get_worksheet("퀀트대상")
    target_ws.clear()
    
    if not quant_df.empty:
        quant_df = quant_df.fillna('')
        target_ws.update([quant_df.columns.values.tolist()] + quant_df.values.tolist())
        print(f"성공: {len(quant_df)}개 종목 저장 완료.")

def update_yearly_data():
    print("Step 3: 퀀트 대상 종목의 1년치 주가 수집 시작...")
    
    quant_ws = get_worksheet("퀀트대상")
    quant_data = quant_ws.get_all_records()
    
    if not quant_data:
        print("에러: '퀀트대상' 데이터가 없습니다.")
        return

    df_quant = pd.DataFrame(quant_data)
    # 퀀트대상 시트에서 코드와 이름만 따로 딕셔너리로 보관 (나중에 매칭용)
    code_to_name = dict(zip(df_quant['Code'].astype(str).str.zfill(6), df_quant['Name']))
    target_codes = list(code_to_name.keys())

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

    all_history = []
    
    print(f"{len(target_codes)}개 종목 상세 데이터 수집 중...")
    for code in target_codes:
        try:
            df_hist = fdr.DataReader(code, start_date, end_date)
            
            if not df_hist.empty:
                df_hist = df_hist.reset_index() # Date를 컬럼으로
                df_hist['Code'] = code          # 종목코드 추가
                df_hist['Name'] = code_to_name[code] # 종목명 추가
                
                # 컬럼 순서 보기 좋게 정리 (날짜, 코드, 이름, 종가...)
                cols = ['Date', 'Code', 'Name', 'Open', 'High', 'Low', 'Close', 'Volume', 'Chg']
                # 실제 데이터에 있는 컬럼만 필터링 (DataReader 버전에 따라 다를 수 있음)
                existing_cols = [c for c in cols if c in df_hist.columns]
                df_hist = df_hist[existing_cols]
                
                all_history.append(df_hist)
        except Exception as e:
            print(f"종목 {code} 오류: {e}")

    if all_history:
        final_df = pd.concat(all_history, ignore_index=True)
        final_df['Date'] = final_df['Date'].dt.strftime('%Y-%m-%d')
        final_df = final_df.fillna('')

        target_ws = get_worksheet("수집대상")
        target_ws.clear()
        
        # 데이터가 클 경우를 대비해 5000줄씩 끊어서 업데이트 (안정성)
        data_to_send = [final_df.columns.values.tolist()] + final_df.values.tolist()
        
        # 구글 시트 API의 한 번에 보낼 수 있는 용량 제한이 있으므로 안전하게 업데이트
        try:
            target_ws.update(data_to_send)
            print(f"성공: {len(final_df)}행 저장 완료.")
        except Exception as e:
            print(f"시트 업데이트 중 오류 (데이터가 너무 클 수 있음): {e}")

# 메인 실행부에 추가
if __name__ == "__main__":
    job_type = sys.argv[1] if len(sys.argv) > 1 else 'all_stocks'
    
    if job_type == 'all_stocks':
        update_all_stocks()
    elif job_type == 'quant_target':
        update_quant_target()
    elif job_type == 'yearly_data': # 추가
        update_yearly_data()
