import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime

def collect_data():
    # 1. 오늘 날짜 확인
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 2. KRX 종목 리스트 수집
    df_krx = fdr.StockListing('KRX')
    
    # 3. 간단한 필터링 예시 (예: 시가총액 상위 10개)
    # 실제 퀀트 전략 로직을 여기에 넣으시면 됩니다.
    top10 = df_krx.nlargest(10, 'MarCap')
    
    # 4. 결과 저장
    top10.to_csv(f"result_{today}.csv", index=False)
    print(f"{today} 데이터 수집 완료!")

if __name__ == "__main__":
    collect_data()
