import os
import sys
import requests
import json
from datetime import datetime, timedelta

# 1. 환경 변수 설정 및 체크
NAVER_CLIENT_ID = os.environ.get('NAVER_CLIENT_ID')
NAVER_CLIENT_SECRET = os.environ.get('NAVER_CLIENT_SECRET')
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')

# 값이 없으면 프로그램 종료 (로그에 범인 표시)
if not all([NAVER_CLIENT_ID, NAVER_CLIENT_SECRET, GEMINI_API_KEY]):
    print(f"에러: 필수 환경변수가 누락되었습니다.")
    print(f"NAVER_ID: {'OK' if NAVER_CLIENT_ID else 'MISSING'}")
    print(f"GEMINI_KEY: {'OK' if GEMINI_API_KEY else 'MISSING'}")
    sys.exit(1) # 에러를 내며 종료시켜서 파일 미생성 문제를 방지

KEYWORDS = ['지마켓', '11번가', '아임웹', '쿠팡', '카페24', '고도몰', '메이크샵', '네이버', '카카오', '메타', '인스타그램', '구글', '유튜브', '롯데온']

def get_naver_news():
    all_articles = []
    # 현재 시간으로부터 정확히 24시간 전 설정
    one_day_ago = datetime.now() - timedelta(days=1)
    
    headers = {
        'X-Naver-Client-Id': NAVER_CLIENT_ID,
        'X-Naver-Client-Secret': NAVER_CLIENT_SECRET
    }

    # 1. 중복 제거를 위한 set 준비 (URL 기준)
    seen_links = set()

    for keyword in KEYWORDS:
        # 검색 품질을 높이기 위해 키워드에 "이커머스" 등을 조합하면 더 좋습니다.
        url = f"https://openapi.naver.com/v1/search/news.json?query={keyword}&display=20&sort=date"
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                items = response.json().get('items', [])
                for item in items:
                    pub_date = datetime.strptime(item['pubDate'], '%a, %d %b %Y %H:%M:%S +0900')
                    
                    # 2. 24시간 이내 뉴스인지 확인
                    if pub_date >= one_day_ago:
                        link = item['link']
                        # 3. 여러 키워드에서 중복으로 수집된 기사 제외
                        if link not in seen_links:
                            seen_links.add(link)
                            title = item['title'].replace('<b>', '').replace('</b>', '').replace('&quot;', '"').replace('&amp;', '&')
                            all_articles.append({
                                'title': title,
                                'link': link,
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

    # call_gemini_ai 함수 내의 prompt 부분 수정
    # 1. 포함할 뉴스: 신규 서비스 런칭, 판매자 지원 정책, 이커머스 실적 분석, 물류/배송 혁신, 플랫폼 UI/UX 변경, 광고 솔루션 업데이트.
    prompt = f"""
    당신은 이커머스 전문 뉴스 분석가입니다. 아래 뉴스 목록에서 '이커머스(전자상거래) 비즈니스'와 직접적인 관련이 있는 뉴스만 엄선하여 리포트를 작성하세요.

    [필터링 기준 - 반드시 준수]
    1. 제외할 뉴스 (매우 중요): 
       - 단순 주식 시황, 증권사 리포트, 주가 등락 관련 뉴스.
       - 사회 일반(사건/사고), 정치, 연예 뉴스.
       - 기업의 단순 사회공헌이나 인사 이동.
       - '카카오' 키워드에서 단순 메신저 기능 관련 뉴스.

    [작성 형식]
    - 이커머스 이슈별로 그룹화하여 <ul><li> 구조로 작성.
    - 제목에 링크를 걸고 날짜를 포함: <li><a href="링크">기사제목</a> - <span style="color: #666;">(발행시간)</span></li>
    - 인사말 없이 <ul>로 시작해서 </ul>로 끝나는 HTML 본문만 응답하세요.

    [뉴스 목록]
    {article_text}
    """

    model_name = "gemini-2.5-flash-lite" 
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
    
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    
    try:
        res = requests.post(url, json=payload)
        result = res.json()
        
        if 'error' in result:
            print(f"Gemini API 에러 발생: {result['error']['message']}")
            return None
            
        if 'candidates' not in result or not result['candidates']:
            print(f"Gemini API 응답 구조 이상: {result}")
            return None

        content = result['candidates'][0]['content']['parts'][0]['text']
        # 마크다운 태그 제거
        clean_content = content.replace('```html', '').replace('```', '').strip()
        return clean_content
    except Exception as e:
        print(f"Gemini API 호출 중 예외 발생: {e}")
        return None
            
        content = result['candidates'][0]['content']['parts'][0]['text']
        
        # 중요: 마크다운 코드 블록 제거 (GAS에서 하신 것과 동일하게)
        clean_content = content.replace('```html', '').replace('```', '').strip()
        return clean_content
    except Exception as e:
        print(f"Gemini API 호출 중 예외 발생: {e}")
        return None

# news_summarizer.py 수정 부분

if __name__ == "__main__":
    articles = get_naver_news()
    
    # 1. 뉴스 데이터가 없을 경우 처리
    if not articles:
        msg = "<h3>알림</h3><p>최근 24시간 이내에 수집된 이커머스 뉴스가 없습니다.</p>"
        with open("news_body.html", "w", encoding="utf-8") as f:
            f.write(msg)
        print("뉴스 없음 - 안내 메시지 생성 완료.")
    else:
        # 2. 뉴스 데이터가 있을 경우 요약 진행
        summary = call_gemini_ai(articles)
        
        if summary:
            with open("news_body.html", "w", encoding="utf-8") as f:
                f.write(summary)
            print("뉴스 요약 파일 생성 완료.")
        else:
            # Gemini 호출 실패 시 방어 코드
            with open("news_body.html", "w", encoding="utf-8") as f:
                f.write("<h3>오류</h3><p>뉴스 요약 중 AI 서버 오류가 발생했습니다.</p>")
            print("AI 요약 실패 - 에러 메시지 생성 완료.")
