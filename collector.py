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

    # 시트 인스턴스 생성 (시트 이름이 정확해야 합니다)
    ws_all = doc.worksheet("전체종목")
    ws_target = doc.worksheet("퀀트대상")
    ws_info = doc.worksheet("수집정보")
    ws_recommend = doc.worksheet("종목추천")

    # --- [STEP 1] 전체종목 수집 (1회성 혹은 필요시 실행) ---
    # 매일 실행할 필요는 없으므로 조건문을 걸거나 수동 실행 권장
    # df_all = fdr.StockListing('KRX')
    # update_sheet(ws_all, df_all)

    # --- [STEP 2] 퀀트대상 (전체종목에서 필터링) ---
    # 예: 전체종목 시트에서 데이터를 읽어와 특정 조건(시총 등)으로 필터링
    df_all_raw = pd.DataFrame(ws_all.get_all_records())
    # 예시 필터: 상장된 종목 중 일부만 선택 (로직에 맞게 수정)
    df_target = df_all_raw.head(50) # 예시로 상위 50개
    update_sheet(ws_target, df_target)

    # --- [STEP 3] 수집정보 (퀀트대상의 상세 지표 수집) ---
    # 퀀트대상 종목코드를 돌면서 일별 시세나 재무정보 수집
    target_codes = df_target['Code'].tolist()
    collected_data = []
    
    for code in target_codes:
        # 실제로는 여기서 종목별 상세 지표(PER, PBR 등)를 가져오는 로직 필요
        # 현재는 예시로 오늘 종가만 가져옴
        df_price = fdr.DataReader(code, datetime.now().strftime('%Y-%m-%d'))
        if not df_price.empty:
            row = {'Date': datetime.now().strftime('%Y-%m-%d'), 'Code': code, 'Close': df_price['Close'].iloc[-1]}
            collected_data.append(row)
    
    df_info = pd.DataFrame(collected_data)
    update_sheet(ws_info, df_info) # 매일 갱신 혹은 누적

    # --- [STEP 4] 종목추천 (수집정보 기준 필터링 및 기록) ---
    # 예: 종가가 특정 기준 이상인 것 추천
    df_recommend = df_info[df_info['Close'] > 10000] 
    
    # 추천 종목은 매일 기록을 쌓아야 하므로 append_rows 사용
    if not df_recommend.empty:
        ws_recommend.append_rows(df_recommend.values.tolist())

    print(f"{datetime.now()} - 퀀트 파이프라인 업데이트 완료!")

def update_sheet(worksheet, df):
    """시트의 기존 내용을 지우고 새로운 데이터로 업데이트하는 함수"""
    worksheet.clear()
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())

if __name__ == "__main__":
    main()
