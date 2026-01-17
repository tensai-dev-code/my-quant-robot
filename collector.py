import FinanceDataReader as fdr
import pandas as pd
import gspread
import json
import os
from google.oauth2.service_account import Credentials

def main():
    # 1. GitHub Secrets에서 인증 정보 가져오기
    service_account_info = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])
    spreadsheet_id = os.environ['SPREADSHEET_ID']

    # 2. 구글 시트 인증
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    client = gspread.authorize(creds)
    
    # 3. 시트 열기
    doc = client.open_by_key(spreadsheet_id)
    sheet = doc.get_worksheet(0) # 첫 번째 탭 선택

    # 4. 데이터 수집 (기존 코드)
    df = fdr.StockListing('KRX').head(20)
    
    # 5. 구글 시트에 업로드 (기존 내용 지우고 새로 쓰기)
    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    print("구글 시트 업데이트 완료!")

if __name__ == "__main__":
    main()
