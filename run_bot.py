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

def update_yearly_data(is_append=False):
    """
    is_append=True 이면 기존 데이터를 지우지 않고 아래에 추가합니다.
    """
    print("Step 3: 주가 데이터 수집 중...")
    quant_ws = get_worksheet("퀀트대상")
    df_quant = pd.DataFrame(quant_ws.get_all_records())
    
    code_to_name = dict(zip(df_quant['Code'].astype(str).str.zfill(6), df_quant['Name']))
    target_codes = list(code_to_name.keys())

    # 매일 업데이트용이면 오늘 데이터만, 수동이면 1년치
    days = 1 if is_append else 365
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    all_history = []
    for code in target_codes:
        try:
            df_hist = fdr.DataReader(code, start_date, end_date).reset_index()
            if not df_hist.empty:
                df_hist['Code'] = code
                df_hist['Name'] = code_to_name[code]
                all_history.append(df_hist)
        except: continue

    if all_history:
        final_df = pd.concat(all_history, ignore_index=True)
        final_df['Date'] = final_df['Date'].dt.strftime('%Y-%m-%d')
        final_df = final_df.fillna('')
        
        target_ws = get_worksheet("수집대상")
        
        # [핵심] 누적 업데이트 로직
        new_data = final_df.values.tolist()
        if not is_append:
            target_ws.clear()
            target_ws.update([final_df.columns.values.tolist()] + new_data)
        else:
            # 기존 데이터 아래에 추가
            target_ws.append_rows(new_data)
        print(f"성공: '수집대상' {len(new_data)}건 업데이트 완료.")

def daily_recommend():
    """
    수집대상 데이터를 분석하여 5개 종목을 추천 시트에 누적 기록
    """
    print("Step 4: 오늘의 추천 종목 선정 중...")
    history_ws = get_worksheet("수집대상")
    df = pd.DataFrame(history_ws.get_all_records())
    
    # 가장 최근 날짜 데이터만 추출
    latest_date = df['Date'].max()
    today_df = df[df['Date'] == latest_date].copy()
    
    # 추천 로직 예시: 거래대금(Volume * Close)이 가장 높은 상위 5개
    # (원하는 퀀트 전략에 따라 정렬 기준을 변경하세요)
    today_df['TradeAmount'] = pd.to_numeric(today_df['Close']) * pd.to_numeric(today_df['Volume'])
    recommend_5 = today_df.sort_values(by='TradeAmount', ascending=False).head(5)
    
    # 추천 일자 추가
    recommend_5['Recommend_Date'] = datetime.now().strftime('%Y-%m-%d')
    
    result_ws = get_worksheet("종목추천")
    
    # 처음인 경우 헤더 추가, 아니면 데이터만 누적
    if not result_ws.get_all_values():
        result_ws.append_row(recommend_5.columns.tolist())
    
    result_ws.append_rows(recommend_5.values.tolist())
    print(f"성공: {latest_date} 기준 추천 종목 5개 저장 완료.")

if __name__ == "__main__":
    job = sys.argv[1] if len(sys.argv) > 1 else 'daily_full_process'
    
    if job == 'daily_full_process':
        # 매일 돌아가는 자동화 사이클
        update_all_stocks()     # 1. 전체 리스트 갱신
        update_quant_target()   # 2. 퀀트 250개 추출
        update_yearly_data(is_append=True) # 3. 오늘치 데이터만 시트에 누적
        daily_recommend()       # 4. 5개 추천해서 누적
    elif job == 'all_stocks': update_all_stocks()
    elif job == 'quant_target': update_quant_target()
    elif job == 'yearly_data': update_yearly_data(is_append=False) # 1년치 전체 새로고침
    elif job == 'daily_recommend': daily_recommend()
