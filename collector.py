import FinanceDataReader as fdr
import pandas as pd
import os

# 데이터 수집 함수
def main():
    print("데이터 수집을 시작합니다...")
    
    # 한국 거래소 종목 리스트 가져오기
    df = fdr.StockListing('KRX')
    
    # 예시: 상위 20개 종목만 추출 (테스트용)
    result = df.head(20)
    
    # 결과 저장 (csv 파일)
    result.to_csv("daily_stock_report.csv", index=False, encoding='utf-8-sig')
    print("파일 저장 완료: daily_stock_report.csv")

if __name__ == "__main__":
    main()
