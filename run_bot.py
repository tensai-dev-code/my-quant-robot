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

SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

if not raw_json or not SPREADSHEET_ID:
    print("에러: 환경 변수(Secrets) 설정 확인이 필요합니다.")
    sys.exit(1)

try:
    SERVICE_ACCOUNT_INFO = json.loads(raw_json)
except json.JSONDecodeError:
    print("에러: JSON 형식이 올바르지 않습니다.")
    sys.exit(1)

# 2. 구글 시트 연결 함수
def get_worksheet(sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_INFO, scope)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SPREADSHEET_ID)
    return doc.worksheet(sheet_name)

# 3. 전체 종목 업데이트
def update_all_stocks():
    print("Step 1: 전체 종목 수집 중...")
    df = fdr.StockListing('KRX') 
    ws = get_worksheet("전체종목")
    ws.clear()
    df = df.fillna('')
    ws.update([df.columns.values.tolist()] + df.values.tolist())
    print("성공: '전체종목' 시트 업데이트 완료.")

# 4. 퀀트 대상 추출 (250개)
def update_quant_target():
    print("Step 2: 퀀트 대상 추출 시작 (250개)...")
    all_ws = get_worksheet("전체종목")
    df = pd.DataFrame(all_ws.get_all_records())
    
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

    quant_df = quant_df.sort_values(by='Amount', ascending=False).head(250)
    
    target_ws = get_worksheet("퀀트대상")
    target_ws.clear()
    if not quant_df.empty:
        quant_df = quant_df.fillna('')
        target_ws.update([quant_df.columns.values.tolist()] + quant_df.values.tolist())
        print(f"성공: {len(quant_df)}개 종목 저장 완료.")

# 5. 주가 데이터 누적 수집
def update_yearly_data(is_append=False):
    target_ws = get_worksheet("수집대상")
    
    if is_append:
        existing_data = target_ws.get_all_records()
        if existing_data:
            last_date = max([str(row['Date']) for row in existing_data])
            today_str = datetime.now().strftime('%Y-%m-%d')
            if last_date >= today_str:
                print(f"이미 {last_date}까지 데이터가 존재합니다. 건너뜁니다.")
                return

    quant_ws = get_worksheet("퀀트대상")
    df_quant = pd.DataFrame(quant_ws.get_all_records())
    code_to_name = dict(zip(df_quant['Code'].astype(str).str.zfill(6), df_quant['Name']))
    target_codes = list(code_to_name.keys())

    days = 3 if is_append else 365 # 주말 대비 3일치 수집
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
        if 'Date' in final_df.columns:
            final_df['Date'] = final_df['Date'].dt.strftime('%Y-%m-%d')
        final_df = final_df.fillna('')

        new_data = final_df.values.tolist()
        if not is_append:
            target_ws.clear()
            target_ws.update([final_df.columns.values.tolist()] + new_data)
        else:
            target_ws.append_rows(new_data)
        print(f"성공: '수집대상' {len(new_data)}건 업데이트 완료.")

# 6. 신뢰도 높은 스코어링 기반 추천 로직
def calculate_stock_score(row):
    try:
        cur_price = float(row.get('Close', 0))
        high_52 = float(row.get('High', 0))
        low_52 = float(row.get('Low', 0))
        marcap = float(row.get('Marcap', 0))
        volume = float(row.get('Volume', 0))
        
        if marcap <= 0 or cur_price <= 0 or high_52 <= low_52: return 0
        
        total_score = 0
        
        # 시총 가점 (8)
        marcap_b = marcap / 100000000
        if marcap_b >= 10000: m_score = 8
        elif marcap_b >= 1000: m_score = 8 * (min(marcap_b, 5000) - 1000) / 4000
        elif marcap_b >= 100: m_score = 2 * (marcap_b - 100) / 900
        else: m_score = 0
        total_score += m_score

        # 52주 Position 및 반등 기대 (45)
        position = min(max((cur_price - low_52) / (high_52 - low_52), 0), 1)
        if 0.15 <= position <= 0.30: rebound = 40 + 5 * (0.30 - position) / 0.15
        elif position < 0.15: rebound = 35 - 10 * (0.15 - position)
        elif position <= 0.50: rebound = 28 + 5 * (0.50 - position) / 0.20
        else: rebound = 18 - 20 * (position - 0.50)
        total_score += max(min(rebound, 45), 0)

        # 변동성 패널티
        volatility = (high_52 - low_52) / low_52
        if volatility > 2.0: total_score -= 15
        elif volatility > 1.2: total_score -= 8
        elif volatility < 0.8: total_score += 6

        # 유동성 (10)
        total_score += min(volume / 500000 * 10, 10)
        if volume < 200000: total_score -= 10
        
        return min(max(total_score, 0), 100)
    except: return 0

def daily_recommend():
    print("Step 4: 스코어링 기반 종목 추천 시작...")
    history_ws = get_worksheet("수집대상")
    df = pd.DataFrame(history_ws.get_all_records())
    if df.empty: return

    # 종목별 52주 고가/저가/현재 데이터 집계
    agg_df = df.groupby('Code').agg({
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Marcap': 'last',
        'Volume': 'last',
        'Name': 'last',
        'Date': 'last'
    }).reset_index()

    agg_df['Total_Score'] = agg_df.apply(calculate_stock_score, axis=1)
    recommend_5 = agg_df.sort_values(by='Total_Score', ascending=False).head(5)
    
    # 결과 정리
    recommend_5['Recommend_Date'] = datetime.now().strftime('%Y-%m-%d')
    output_df = recommend_5[['Recommend_Date', 'Code', 'Name', 'Total_Score', 'Close', 'High', 'Low', 'Volume']].copy()
    output_df.columns = ['추천일자', '종목코드', '종목명', '종합점수', '현재가', '52주고가', '52주저가', '거래량']

    result_ws = get_worksheet("종목추천")
    if not result_ws.get_all_values():
        result_ws.append_row(output_df.columns.tolist())
    
    result_ws.append_rows(output_df.values.tolist())
    print(f"성공: {datetime.now().strftime('%Y-%m-%d')} 추천 완료.")

# 7. 메인 실행부
if __name__ == "__main__":
    job = sys.argv[1] if len(sys.argv) > 1 else 'daily_full_process'
    if job == 'daily_full_process':
        update_all_stocks()
        update_quant_target()
        update_yearly_data(is_append=True)
        daily_recommend()
    elif job == 'all_stocks': update_all_stocks()
    elif job == 'quant_target': update_quant_target()
    elif job == 'yearly_data': update_yearly_data(is_append=False)
    elif job == 'daily_recommend': daily_recommend()
