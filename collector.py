import FinanceDataReader as fdr
import pandas as pd
import gspread
import json
import os
from google.oauth2.service_account import Credentials
from datetime import datetime

def main():
    # 1. 인증 및 시트 연결
    service_account_info = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])
    spreadsheet_id = os.environ['SPREADSHEET_ID']
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    client = gspread.authorize(creds)
    doc = client.open_by_key(spreadsheet_id)

    # 각 시트 가져오기
    ws_all = doc.worksheet("전체종목")
    ws_target = doc.worksheet("퀀트대상")
    ws_info = doc.worksheet("수집정보")
    ws_recommend = doc.worksheet("종목추천")

    # --- [STEP 1] 전체종목 수집 (KRX 전체) ---
    print("STEP 1: 전체 종목 리스트 수집 중...")
    df_all = fdr.StockListing('KRX') # 한국거래소 전체 종목 (KOSPI, KOSDAQ, KONEX)
    
    # 불필요한 우선주나 관리종목을 1차로 걸러내고 싶다면 여기서 처리
    # 예: df_all = df_all[df_all['Market'].isin(['KOSPI', 'KOSDAQ'])]
    
    update_sheet(ws_all, df_all)
    print(f"전체종목 업데이트 완료: {len(df_all)} 건")

    # --- [STEP 2] 퀀트대상 필터링 ---
    # 예: 전체 종목 중 시가총액 상위 200개만 대상으로 선정
    print("STEP 2: 퀀트 유니버스(대상) 추출 중...")
    df_target = df_all.sort_values(by='MarCap', ascending=False).head(200) 
    update_sheet(ws_target, df_target)

    # --- [STEP 3] 수집정보 (퀀트대상의 가격/지표 수집) ---
    # 이 부분은 종목이 많으면 시간이 걸리므로 퀀트대상 종목에 대해서만 실행
    print("STEP 3: 대상 종목 상세 정보 수집 중...")
    target_codes = df_target['Code'].tolist()
    
    # (여기에 실제 수집 로직 추가 - 현재는 구조만 유지)
    # ...

def update_sheet(worksheet, df):
    """시트를 완전히 비우고 Pandas DataFrame 데이터로 채움"""
    # NaN(결측치) 처리 (구글 시트 전송 오류 방지)
    df = df.fillna('')
    worksheet.clear()
    # 컬럼 헤더와 데이터를 합쳐서 리스트로 변환 후 업데이트
    data = [df.columns.values.tolist()] + df.values.tolist()
    worksheet.update(data)

if __name__ == "__main__":
    main()
