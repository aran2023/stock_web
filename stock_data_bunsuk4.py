# ==============================================================================
# 파일명: 260814_1750_stock_data_bunsuk3.py
# 코딩 목적: Render 서버 기동 시 GitHub Releases(ver6.8)에서 주가 DB 및 종목명 CSV 자동 다운로드 연동
# 
# [흐름도 (Flowchart)]
# 1. 서버 시작 (Lifespan Startup 이벤트)
# 2. 로컬 디스크 파일 검사:
#    - stock_data.db, stock_name.csv 존재 여부 확인
#    - 파일 부재 시: GitHub Releases 직링크를 통해 자동 1회 다운로드 수행
#    - 파일 존재 시: 다운로드 생략하여 빠른 시작(0초) 보장
# 3. 예외 및 안전 처리: 다운로드 실패 시 에러 로깅 후 서버 중단 방지
# 4. FastAPI 웹 서비스 정상 가동 및 SQLite DB 기반 초고속 데이터 서빙
# ==============================================================================

import os
import urllib.request
import sqlite3
import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse
from contextlib import asynccontextmanager

# [상수 및 설정]
DB_PATH = "stock_data.db"
CSV_PATH = "stock_name.csv"

# GitHub Releases(ver6.8) 직링크 주소
DB_DOWNLOAD_URL = "https://github.com/aran2023/stock_web/releases/download/ver6.8/stock_data.db"
CSV_DOWNLOAD_URL = "https://github.com/aran2023/stock_web/releases/download/ver6.8/stock_name.csv"

def download_if_not_exists(url: str, target_path: str):
    """파일이 로컬에 없을 경우에만 원격 링크에서 다운로드"""
    if not os.path.exists(target_path):
        print(f"[다운로드 시작] {target_path} 파일을 가져옵니다: {url}")
        try:
            urllib.request.urlretrieve(url, target_path)
            print(f"[다운로드 완료] {target_path} 준비 완료! (크기: {os.path.getsize(target_path)} bytes)")
        except Exception as e:
            print(f"[다운로드 실패] {target_path} 다운로드 중 오류 발생: {e}")
    else:
        print(f"[기존 파일 확인] {target_path} 파일이 이미 존재합니다. (다운로드 생략)")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 서버 기동 시 파일 자동 확인 및 다운로드
    download_if_not_exists(DB_DOWNLOAD_URL, DB_PATH)
    download_if_not_exists(CSV_DOWNLOAD_URL, CSV_PATH)
    yield
    # 서버 종료 시 정리 로직이 필요할 경우 여기에 작성

app = FastAPI(lifespan=lifespan)

# ------------------------------------------------------------------------------
# 데이터베이스 헬퍼 함수
# ------------------------------------------------------------------------------
def get_db_connection():
    if not os.path.exists(DB_PATH):
        return None
    return sqlite3.connect(DB_PATH)

def load_stock_names():
    if os.path.exists(CSV_PATH):
        try:
            return pd.read_csv(CSV_PATH, dtype=str)
        except Exception:
            return pd.DataFrame(columns=["code", "name"])
    return pd.DataFrame(columns=["code", "name"])

# ------------------------------------------------------------------------------
# 웹 엔드포인트 라우트
# ------------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def main_page():
    db_exists = os.path.exists(DB_PATH)
    csv_exists = os.path.exists(CSV_PATH)
    
    status_db = "🟢 정상 준비됨" if db_exists else "🔴 파일 없음"
    status_csv = "🟢 정상 준비됨" if csv_exists else "🔴 파일 없음"
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>아란 주식 데이터 대시보드</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; padding: 20px; background-color: #f4f6f9; }}
            .card {{ background: white; border-radius: 12px; padding: 24px; max-width: 800px; margin: 0 auto; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
            h1 {{ color: #1a202c; font-size: 24px; margin-bottom: 20px; }}
            .status-box {{ background: #edf2f7; border-radius: 8px; padding: 15px; margin-bottom: 20px; }}
            .status-item {{ margin: 8px 0; font-size: 15px; }}
            .btn {{ background-color: #3182ce; color: white; border: none; padding: 10px 18px; border-radius: 6px; cursor: pointer; font-size: 15px; font-weight: bold; }}
            .btn:hover {{ background-color: #2b6cb0; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th, td {{ border: 1px solid #e2e8f0; padding: 10px; text-align: left; font-size: 14px; }}
            th {{ background-color: #f7fafc; }}
        </style>
    </head>
    <body>
        <div class="card">
            <h1>📊 아란 주식 데이터 대시보드 (자동 연동형)</h1>
            <div class="status-box">
                <div class="status-item">📦 <strong>DB 상태 (stock_data.db):</strong> {status_db}</div>
                <div class="status-item">📋 <strong>종목 목록 (stock_name.csv):</strong> {status_csv}</div>
                <div class="status-item">🌐 <strong>데이터 소스:</strong> GitHub Releases ver6.8 자동 연동</div>
            </div>
            
            <h3>🔍 DB 보유 테이블 목록 조회</h3>
            <button class="btn" onclick="fetchTables()">테이블 목록 불러오기</button>
            <div id="table-result" style="margin-top: 15px;"></div>
        </div>

        <script>
            async function fetchTables() {{
                const resDiv = document.getElementById('table-result');
                resDiv.innerHTML = '불러오는 중...';
                try {{
                    const response = await fetch('/api/tables');
                    const data = await response.json();
                    if (data.tables && data.tables.length > 0) {{
                        let html = '<table><tr><th>번호</th><th>테이블명 (종목/분봉)</th></tr>';
                        data.tables.forEach((tbl, idx) => {{
                            html += `<tr><td>${{idx + 1}}</td><td><strong>${{tbl}}</strong></td></tr>`;
                        }});
                        html += '</table>';
                        resDiv.innerHTML = html;
                    }} else {{
                        resDiv.innerHTML = '<p style="color:red;">테이블이 없거나 DB를 불러올 수 없습니다.</p>';
                    }}
                }} catch (e) {{
                    resDiv.innerHTML = '<p style="color:red;">조회 실패: ' + e + '</p>';
                }}
            }}
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/api/tables")
async def get_tables():
    conn = get_db_connection()
    if conn is None:
        return JSONResponse(status_code=500, content={"error": "DB 파일이 로드되지 않았습니다."})
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return {"tables": tables}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
