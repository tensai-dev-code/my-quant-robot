import os
import sys
import requests
import json
from datetime import datetime, timedelta

# 1. 환경 변수 설정
NAVER_CLIENT_ID = os.environ.get('NAVER_CLIENT_ID')
NAVER_CLIENT_SECRET = os.environ.get('NAVER_CLIENT_SECRET')
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
RECIPIENT_EMAIL = "leedch@gabiacns.com"

KEYWORDS = ['지마켓', '11번가', '아임웹', '쿠팡', '카페24', '고도몰', '메이크샵', '네이버', '카카오', '메타', '인스타그램', '구글', '유튜브', '롯데온']

def get_naver_news():
    all_articles = []
    one_day_ago = datetime.now() - timedelta(days=1)
    
    headers = {
        'X-Naver-Client-Id': NAVER_CLIENT_ID,
        'X-Naver-Client-Secret': NAVER_CLIENT_SECRET
    }

    for keyword in KEYWORDS:
        url = f"https://openapi.naver.com/v1/search/news.json?query={keyword}&display=10&sort=date"
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                items = response.json().get('items', [])
                for item in items:
                    # 네이버 날짜 형식 변환: "Mon, 06 Jan 2025 14:30:00 +0900"
                    pub_date = datetime.strptime(item['pub_date'], '%a, %d %b %Y %H:%M:%S +0900')
                    
                    if pub_date >= one_day_ago:
                        title = item['title'].replace('<b>', '').replace('</b>', '').replace('&quot;', '"')
                        all_articles.append({
                            'title': title,
                            'link': item['link'],
                            'pubDate': pub_date.strftime('%Y-%m-%d %H:%M'),
                            'keyword': keyword
                        })
        except Exception as e:
            print(f"{keyword} 검색 중 오류: {e}")
            
    return all_articles

def call_gemini_ai(articles):
    if not articles:
        return None
    
    article_text = ""
    for a in articles:
        article_text += f"- 제목: {a['title']}\n  링크: {a['link']}\n  날짜: {a['pubDate']}\n  키워드: {a['keyword']}\n\n"

    prompt = f"""
    당신은 이커머스 전문 뉴스 큐레이터입니다. 다음 뉴스 목록을 분석하여 그룹화된 리포트를 작성하세요.
    [작업 지침]
    1. 이커머스 관련 뉴스만 선별(광고 플랫폼 포함), 증시/종목 뉴스는 제외.
    2. 중복 뉴스 그룹화 및 핵심 내용 요약.
    3. HTML 형식(중첩 리스트 <ul><li>)으로 작성. 링크는 <a> 태그 사용.
    4. 인사말 없이 <ul>로 시작해서 </ul>로 끝나는 본문만 응답하세요.

    [뉴스 목록]
    {article_text}
    """

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    
    try:
        res = requests.post(url, json=payload)
        result = res.json()
        content = result['candidates'][0]['content']['parts'][0]['text']
        return content.replace('```html', '').replace('```', '').strip()
    except Exception as e:
        print(f"Gemini API 호출 오류: {e}")
        return None

if __name__ == "__main__":
    articles = get_naver_news()
    if not articles:
        print("24시간 이내 뉴스가 없습니다.")
        sys.exit(0)
    
    summary = call_gemini_ai(articles)
    if summary:
        with open("news_body.html", "w", encoding="utf-8") as f:
            f.write(summary)
        print("뉴스 요약 완료.")
