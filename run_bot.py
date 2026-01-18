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
    target_ws = get_worksheet("수집대상")
    
    # 1. 누적 모드일 때, 마지막 수집 날짜 확인
    if is_append:
        existing_data = target_ws.get_all_records()
        if existing_data:
            last_date = max([row['Date'] for row in existing_data])
            today_str = datetime.now().strftime('%Y-%m-%d')
            
            if last_date >= today_str:
                print(f"이미 {last_date}까지의 데이터가 시트에 존재합니다. 수집을 건너뜁니다.")
                return # 함수 종료

    # 2. 수집 대상 종목 가져오기
    quant_ws = get_worksheet("퀀트대상")
    df_quant = pd.DataFrame(quant_ws.get_all_records())
    code_to_name = dict(zip(df_quant['Code'].astype(str).str.zfill(6), df_quant['Name']))
    target_codes = list(code_to_name.keys())

    # 3. 날짜 범위 설정
    days = 2 if is_append else 365 # 누적일 땐 안전하게 최근 2일치 조회
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
        # 중복 제거 (데이터 프레임 내에서 한 번 더 체크)
        final_df = final_df.drop_duplicates(['Date', 'Code'])
        
        new_data = final_df.values.tolist()
        if not is_append:
            target_ws.clear()
            target_ws.update([final_df.columns.values.tolist()] + new_data)
        else:
            target_ws.append_rows(new_data)


def daily_recommend():
    """
    수집대상 데이터를 분석하여 52주 최저가에 근접한 종목 5개를 추천 시트에 누적 기록
    """
    print("Step 4: 52주 최저가 근접 종목 추천 시작...")
    history_ws = get_worksheet("수집대상")
    df = pd.DataFrame(history_ws.get_all_records())
    
    if df.empty:
        print("에러: 수집대상 시트에 데이터가 없습니다.")
        return

    # 숫자형 변환
    df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
    df['Low'] = pd.to_numeric(df['Low'], errors='coerce')

    # 1. 종목별 52주 최저가 계산
    min_prices = df.groupby('Code')['Low'].min().reset_index()
    min_prices.columns = ['Code', 'Min_52Week']

    # 2. 가장 최근 날짜의 종가 가져오기
    latest_date = df['Date'].max()
    today_df = df[df['Date'] == latest_date].copy()
    
    # 3. 데이터 병합 (현재가와 최저가 비교)
    merged = pd.merge(today_df, min_prices, on='Code')
    
    # 4. 최저가 대비 근접도 계산 (현재가 / 52주최저가)
    # 1에 가까울수록 최저가에 붙어 있는 상태
    merged['Recovery_Rate'] = merged['Close'] / merged['Min_52Week']
    
    # 5. 근접도 순으로 정렬하여 상위 5개 추출 (반등 예상 종목)
    recommend_5 = merged.sort_values(by='Recovery_Rate', ascending=True).head(5)

    # 6. 추천 일자 및 제목 컬럼 정리
    recommend_5['Recommend_Date'] = datetime.now().strftime('%Y-%m-%d')
    
    # 저장할 컬럼 순서 정의 (제목/헤더 포함)
    final_cols = ['Recommend_Date', 'Code', 'Name', 'Close', 'Min_52Week', 'Recovery_Rate', 'Volume']
    recommend_final = recommend_5[final_cols].copy()
    
    # 컬럼명 한글로 변경 (시트 가독성용)
    recommend_final.columns = ['추천일자', '종목코드', '종목명', '현재가', '52주최저가', '최저가대비비율', '거래량']

    result_ws = get_worksheet("종목추천")
    
    # 7. 누적 저장 로직 (기존 데이터가 없으면 헤더부터, 있으면 데이터만 추가)
    existing_values = result_ws.get_all_values()
    
    if not existing_values:
        # 시트가 비어있으면 헤더(제목컬럼) 포함하여 업데이트
        result_ws.append_row(recommend_final.columns.tolist())
    
    # 데이터 누적 추가 (append_rows)
    result_ws.append_rows(recommend_final.values.tolist())
    print(f"성공: {latest_date} 기준 최저가 근접 종목 5개 누적 저장 완료.")


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
