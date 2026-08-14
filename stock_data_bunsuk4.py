# ==============================================================================
# 파일명: 260814_1810_stock_data_bunsuk5.py
# 코딩 목적: 
#   1. GitHub Releases(ver6.8)에서 DB 및 CSV 파일 자동 다운로드 및 상시 동기화
#   2. stock_data.db(min1_data, daily_data)와 stock_name.csv 종목명 정밀 매핑
#   3. 원본 다크 테마 정밀 분석 대시보드(종목명, 코드, 종류 태그, 기간, 건수, 뱃지) 완벽 복원
#
# [흐름도 (Flowchart)]
# 1. 서버 시작 (Lifespan Startup 이벤트)
#    └─ stock_data.db 및 stock_name.csv 파일 부재 시 GitHub Releases에서 1회 자동 다운로드
# 2. CSV 파일 로드 및 코드-종목명 딕셔너리 빌드
# 3. SQLite DB 테이블 구조 분석 (PRAGMA table_info)
#    ├─ min1_data / daily_data 테이블 내 고유 종목코드(DISTINCT code) 추출
#    ├─ 종목코드별 기간(MIN/MAX 일자), 총 레코드 건수(COUNT), 테이블 필드 수 산출
#    └─ 테이블명에 따라 종류 태그(분봉: 골드, 일봉: 그린) 분류
# 4. 분석 결과 통합 및 다크 테마 UI 렌더링
# ==============================================================================

import os
import urllib.request
import sqlite3
import pandas as pd
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

# [상수 및 설정 정의]
INFO = {
    "title": "아란 주식 데이터 & CSV 종목명 연동 정밀 분석 대시보드",
    "purpose": "클라우드 기반 주가 DB(분봉/일봉) 정밀 집계 및 다크 테마 웹 UI 서빙",
    "version": "v5.0 (260814_1810)"
}

DB_PATH = "stock_data.db"
CSV_PATH = "stock_name.csv"

# GitHub Releases(ver6.8) 직링크
DB_DOWNLOAD_URL = "https://github.com/aran2023/stock_web/releases/download/ver6.8/stock_data.db"
CSV_DOWNLOAD_URL = "https://github.com/aran2023/stock_web/releases/download/ver6.8/stock_name.csv"

def download_if_not_exists(url: str, target_path: str):
    """파일이 로컬에 없을 경우에만 원격 직링크에서 자동 다운로드"""
    if not os.path.exists(target_path):
        print(f"[*] {target_path} 파일 다운로드 시작: {url}")
        try:
            urllib.request.urlretrieve(url, target_path)
            file_size = os.path.getsize(target_path)
            print(f"[+] {target_path} 다운로드 완료! (용량: {file_size:,} Bytes)")
        except Exception as e:
            print(f"[-] {target_path} 다운로드 오류: {e}")
    else:
        print(f"[*] {target_path} 파일이 이미 존재합니다.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 서버 기동 시 필요한 데이터 파일 자동 다운로드 및 검증
    download_if_not_exists(DB_DOWNLOAD_URL, DB_PATH)
    download_if_not_exists(CSV_DOWNLOAD_URL, CSV_PATH)
    yield

app = FastAPI(lifespan=lifespan)

# ------------------------------------------------------------------------------
# 데이터 파싱 및 정밀 분석 엔진
# ------------------------------------------------------------------------------
def get_analysis_data():
    """DB와 CSV를 분석하여 UI에 필요한 정밀 통계 데이터를 추출"""
    data = {
        "db_path": os.path.abspath(DB_PATH) if os.path.exists(DB_PATH) else "파일 없음",
        "db_size_mb": 0.0,
        "db_size_bytes": 0,
        "csv_mapped_count": 0,
        "extracted_codes": [],
        "rows": [],
        "total_codes_count": 0,
        "total_records_count": 0
    }

    # 1. DB 파일 용량 계산
    if os.path.exists(DB_PATH):
        b_size = os.path.getsize(DB_PATH)
        data["db_size_bytes"] = b_size
        data["db_size_mb"] = round(b_size / (1024 * 1024), 2)

    # 2. CSV 종목명 매핑 딕셔너리 생성
    name_map = {}
    if os.path.exists(CSV_PATH):
        try:
            df_name = pd.read_csv(CSV_PATH, dtype=str)
            # 컬럼명이 무엇이든 첫 번째 컬럼을 코드, 두 번째 컬럼을 종목명으로 안전 처리
            if df_name.shape[1] >= 2:
                c_col = df_name.columns[0]
                n_col = df_name.columns[1]
                for _, r in df_name.iterrows():
                    code_val = str(r[c_col]).strip().zfill(6)
                    name_map[code_val] = str(r[n_col]).strip()
            data["csv_mapped_count"] = len(name_map)
        except Exception as e:
            print(f"[-] CSV 파싱 오류: {e}")

    # 3. SQLite DB 테이블 스캔 및 종목별 집계
    if os.path.exists(DB_PATH):
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            # 모든 테이블 목록 조회
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall() if not row[0].startswith("sqlite_")]
            
            all_codes_set = set()
            total_records = 0

            for tbl in tables:
                # 테이블 컬럼 정보 조회
                cursor.execute(f"PRAGMA table_info({tbl});")
                cols_info = cursor.fetchall()
                col_names = [c[1].lower() for c in cols_info]
                field_count = len(cols_info)

                # 종류(분봉/일봉) 판정
                if "min" in tbl.lower():
                    data_type = "분봉"
                elif "daily" in tbl.lower() or "day" in tbl.lower():
                    data_type = "일봉"
                else:
                    data_type = "기타"

                # date/time 컬럼명 탐색
                date_col = next((c for c in col_names if c in ["date", "datetime", "일자", "시간", "체결시간", "dt"]), None)
                has_code = "code" in col_names or "종목코드" in col_names
                code_col = "code" if "code" in col_names else ("종목코드" if "종목코드" in col_names else None)

                if has_code and code_col:
                    # 종목코드별 집계
                    cursor.execute(f"SELECT DISTINCT {code_col} FROM {tbl};")
                    codes_in_tbl = [str(r[0]).strip().zfill(6) for r in cursor.fetchall() if r[0] is not None]

                    for c in codes_in_tbl:
                        all_codes_set.add(c)
                        # 건수 및 기간 조회
                        if date_col:
                            query = f"SELECT COUNT(*), MIN({date_col}), MAX({date_col}) FROM {tbl} WHERE {code_col} = ?"
                            cursor.execute(query, (c,))
                            cnt, min_d, max_d = cursor.fetchone()
                        else:
                            cursor.execute(f"SELECT COUNT(*) FROM {tbl} WHERE {code_col} = ?", (c,))
                            cnt = cursor.fetchone()[0]
                            min_d, max_d = "-", "-"

                        cnt = cnt or 0
                        total_records += cnt
                        
                        # 기간 포맷팅 (YYYY-MM-DD 또는 원본)
                        period_str = f"{min_d} ~ {max_d}" if min_d and max_d else "-"

                        # 종목명 찾기 (없으면 코드명 유지)
                        stock_name = name_map.get(c, f"종목_{c}")

                        data["rows"].append({
                            "stock_name": stock_name,
                            "table_name": tbl,
                            "code": c,
                            "type": data_type,
                            "period": period_str,
                            "count": cnt,
                            "field_count": field_count
                        })
                else:
                    # code 컬럼이 없는 단일 테이블 형태인 경우
                    cursor.execute(f"SELECT COUNT(*) FROM {tbl};")
                    cnt = cursor.fetchone()[0] or 0
                    total_records += cnt
                    data["rows"].append({
                        "stock_name": tbl,
                        "table_name": tbl,
                        "code": "-",
                        "type": data_type,
                        "period": "-",
                        "count": cnt,
                        "field_count": field_count
                    })

            data["extracted_codes"] = sorted(list(all_codes_set))
            data["total_codes_count"] = len(all_codes_set)
            data["total_records_count"] = total_records
            conn.close()
        except Exception as e:
            print(f"[-] DB 분석 오류: {e}")

    return data

# ------------------------------------------------------------------------------
# 웹 대시보드 엔드포인트
# ------------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    d = get_analysis_data()
    
    # 뱃지 HTML 생성
    badges_html = "".join([f'<span class="badge">{c}</span>' for c in d["extracted_codes"]])
    if not badges_html:
        badges_html = '<span style="color:#8892b0; font-size:14px;">추출된 종목 코드가 없습니다.</span>'

    # 테이블 행 HTML 생성
    table_rows_html = ""
    for r in d["rows"]:
        type_class = "type-min" if r["type"] == "분봉" else "type-day"
        table_rows_html += f"""
        <tr>
            <td>
                <div class="stock-title">{r['stock_name']}</div>
                <div class="table-sub">({r['table_name']})</div>
            </td>
            <td class="code-text">{r['code']}</td>
            <td><span class="{type_class}">{r['type']}</span></td>
            <td class="period-text">{r['period']}</td>
            <td class="count-text">{r['count']:,}건</td>
            <td class="field-text">{r['field_count']}개 필드</td>
        </tr>
        """

    html_content = f"""
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>주가 DB & CSV 종목명 연동 정밀 분석</title>
        <style>
            * {{ box-sizing: border-box; margin: 0; padding: 0; }}
            body {{
                background-color: #0b0f19;
                color: #e2e8f0;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
                padding: 30px 20px;
                display: flex;
                justify-content: center;
            }}
            .container {{
                width: 100%;
                max-width: 1100px;
                background-color: #111827;
                border: 1px solid #1f293d;
                border-radius: 12px;
                padding: 28px;
                box-shadow: 0 10px 25px rgba(0, 0, 0, 0.5);
            }}
            .header-title {{
                display: flex;
                align-items: center;
                gap: 10px;
                font-size: 20px;
                font-weight: 700;
                color: #60a5fa;
                margin-bottom: 24px;
            }}
            .info-card {{
                background-color: #162032;
                border: 1px solid #223049;
                border-radius: 8px;
                padding: 16px 20px;
                font-size: 14px;
                line-height: 1.8;
                margin-bottom: 20px;
            }}
            .info-row {{
                display: flex;
                justify-content: space-between;
                align-items: center;
            }}
            .info-label {{ color: #94a3b8; }}
            .info-val {{ color: #f1f5f9; font-weight: 600; }}
            .highlight-green {{ color: #34d399; font-weight: 700; }}
            .highlight-yellow {{ color: #fbbf24; font-weight: 700; }}
            
            .badge-section {{
                background-color: #162032;
                border: 1px solid #223049;
                border-radius: 8px;
                padding: 14px 20px;
                margin-bottom: 24px;
            }}
            .badge-title {{
                color: #f87171;
                font-size: 14px;
                font-weight: 700;
                margin-bottom: 10px;
                display: flex;
                align-items: center;
                gap: 6px;
            }}
            .badge-group {{
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
            }}
            .badge {{
                background-color: #1e293b;
                border: 1px solid #334155;
                color: #38bdf8;
                padding: 4px 14px;
                border-radius: 6px;
                font-weight: 700;
                font-size: 14px;
                letter-spacing: 0.5px;
            }}
            
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 10px;
            }}
            th {{
                background-color: #172236;
                color: #94a3b8;
                font-size: 13px;
                font-weight: 600;
                text-align: center;
                padding: 14px 10px;
                border-top: 1px solid #223049;
                border-bottom: 1px solid #223049;
            }}
            td {{
                padding: 16px 10px;
                text-align: center;
                font-size: 14px;
                border-bottom: 1px solid #1a2436;
            }}
            tr:hover {{
                background-color: #141d2e;
            }}
            .stock-title {{
                color: #f8fafc;
                font-weight: 700;
                font-size: 15px;
            }}
            .table-sub {{
                color: #64748b;
                font-size: 12px;
                margin-top: 2px;
            }}
            .code-text {{
                color: #38bdf8;
                font-weight: 700;
                font-size: 14px;
            }}
            .type-min {{
                color: #fbbf24;
                font-weight: 700;
            }}
            .type-day {{
                color: #34d399;
                font-weight: 700;
            }}
            .period-text {{
                color: #cbd5e1;
                font-size: 13px;
            }}
            .count-text {{
                color: #f8fafc;
                font-weight: 700;
            }}
            .field-text {{
                color: #94a3b8;
                font-size: 13px;
            }}
            
            .footer-summary {{
                text-align: right;
                margin-top: 20px;
                font-size: 14px;
                color: #94a3b8;
            }}
            .footer-summary strong {{
                color: #38bdf8;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header-title">
                <span>🔍</span>
                <span>주가 DB & CSV 종목명 연동 정밀 분석</span>
            </div>

            <div class="info-card">
                <div class="info-row">
                    <span class="info-label">📁 DB 파일 경로:</span>
                    <span class="info-val">{d['db_path']}</span>
                </div>
                <div class="info-row">
                    <span class="info-label">📦 DB 파일 용량:</span>
                    <span class="highlight-yellow">{d['db_size_mb']} MB ({d['db_size_bytes']:,} Bytes)</span>
                </div>
                <div class="info-row">
                    <span class="info-label">📑 CSV 매핑된 종목 수:</span>
                    <span class="highlight-green">{d['csv_mapped_count']:,}개 종목 매핑됨 (stock_name.csv)</span>
                </div>
            </div>

            <div class="badge-section">
                <div class="badge-title">📌 DB 내 추출된 종목 코드(CODE) 목록:</div>
                <div class="badge-group">
                    {badges_html}
                </div>
            </div>

            <table>
                <thead>
                    <tr>
                        <th style="width: 22%;">종목명 (테이블명)</th>
                        <th style="width: 12%;">코드</th>
                        <th style="width: 10%;">종류</th>
                        <th style="width: 28%;">기간</th>
                        <th style="width: 14%;">갯수 (행수)</th>
                        <th style="width: 14%;">필드수</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows_html}
                </tbody>
            </table>

            <div class="footer-summary">
                총 <strong>{d['total_codes_count']}개 CODE</strong> 파싱 완료 (누적 레코드: <strong>{d['total_records_count']:,}건</strong>)
            </div>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

# ------------------------------------------------------------------------------
# 실행 엔트리포인트 (uvicorn 구동)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("260814_1810_stock_data_bunsuk5:app", host="0.0.0.0", port=port)
