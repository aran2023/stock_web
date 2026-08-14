# ================================================================================
# [파일명]: 260813_2340_main_launcher.py
# [작성일시]: 2026년 08월 13일 23:40
# [코딩 목적]: 
#   1. D:\data\Project\aran_trading2\data\stock_name.csv 파일 로드 (code, 종목명 매핑)
#   2. D:\data\Project\aran_trading2\data\stock_data.db 내 daily_data, min1_data 전수 파싱
#   3. DB의 code와 CSV의 종목명을 정밀 매핑하여 [종목명 | 코드 | 종류 | 기간 | 갯수 | 필드수] 완벽 표출
#   4. 단일 실행을 통한 백엔드 가동 및 브라우저 자동 오픈
#
# [프로그램 흐름도 (Flowchart)]:
#   [스크립트 실행] ➔ [info 상수 콘솔 출력 (void_setup)] ➔ [FastAPI/서버 초기화]
#          │
#          ▼
#   [stock_name.csv 로드 ➔ Dict 맵 생성] ➔ [stock_data.db 연결 및 메타 검증]
#          │
#          ▼
#   [daily_data / min1_data 순회 ➔ DISTINCT code 추출 ➔ CSV 맵에서 진짜 종목명 바인딩]
#          │
#          ▼
#   [로컬 웹 서버 가동 (http://127.0.0.1:8000)]
#          ├─ 메인 주소 접속 (/) ➔ HTML 화면 렌더링 (CSV 매핑 표 출력)
#          └─ API 호출 (/api/stock-info) ➔ 정밀 분석 결과 JSON 전달
# ================================================================================

import os
import sqlite3
import csv
import datetime
import threading
import webbrowser
import time
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
import uvicorn

# ================================================================================
# [코딩 11계명] 필수 info 상수 정의 및 void setup 콘솔 출력
# ================================================================================
info = {
    "title": "주가 DB 및 CSV 종목명 매핑 정밀 런처 (ver5.0)",
    "purpose": "stock_name.csv 연동을 통한 정확한 종목명 결합, stock_data.db 전수 파싱 및 웹 표 출력",
    "pinNumber": "HTTP Port 8000 (Local Integrated Web/API)"
}

def void_setup():
    print("================================================================================")
    print("[프로그램 정보 (void setup)]")
    print(f"제목: {info['title']}")
    print(f"목적: {info['purpose']}")
    print(f"핀번호: {info['pinNumber']}")
    print("================================================================================")

# 스크립트 가동 시 void_setup 필수 실행
void_setup()

app = FastAPI(title=info["title"])

# 대상 파일 경로 정의
DB_PATH = r"D:\data\Project\aran_trading2\data\stock_data.db"
CSV_PATH = r"D:\data\Project\aran_trading2\data\stock_name.csv"


def load_stock_names():
    """
    stock_name.csv 파일을 읽어 {code: 종목명} 딕셔너리를 반환하는 함수
    """
    stock_map = {}
    if not os.path.exists(CSV_PATH):
        return stock_map

    try:
        # utf-8, cp949(euc-kr) 인코딩 예외 처리
        encodings = ['utf-8-sig', 'cp949', 'euc-kr', 'utf-8']
        for enc in encodings:
            try:
                with open(CSV_PATH, mode='r', encoding=enc) as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if not row or len(row) < 2:
                            continue
                        code_val = str(row[0]).strip().zfill(6)  # 6자리 코드 맞춤 (예: 005930)
                        name_val = str(row[1]).strip()
                        
                        # 헤더 행 제외 (code, 종목명 등)
                        if code_val.lower() in ['code', '종목코드', 'symbol']:
                            continue
                        
                        stock_map[code_val] = name_val
                break  # 성공 시 루프 탈출
            except UnicodeDecodeError:
                continue
    except Exception as e:
        print(f"⚠️ CSV 로드 중 경고: {str(e)}")

    return stock_map


def analyze_stock_db_with_csv():
    """
    stock_name.csv와 stock_data.db를 연동하여 정밀 분석 데이터를 반환하는 함수
    """
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"지정된 DB 파일 경로를 찾을 수 없습니다: {DB_PATH}")

    # 1. CSV 종목명 맵 로드
    stock_name_map = load_stock_names()

    # 2. DB 물리 정보 파싱
    file_size_bytes = os.path.getsize(DB_PATH)
    file_size_mb = f"{file_size_bytes / (1024 * 1024):.2f} MB ({file_size_bytes:,} Bytes)"
    file_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(DB_PATH)).strftime('%Y-%m-%d %H:%M:%S')

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        if not tables:
            raise ValueError("DB 파일 내에 유효한 데이터 테이블이 존재하지 않습니다.")

        all_code_list = []
        parsed_items = []
        grand_total_rows = 0

        for tbl_item in tables:
            table_name = tbl_item[0]
            if table_name.startswith("sqlite_"):
                continue

            # 필드 정보 파싱
            cursor.execute(f'PRAGMA table_info("{table_name}");')
            columns_info = cursor.fetchall()
            field_count = len(columns_info)
            col_names = [col[1] for col in columns_info]

            date_col = next((c for c in col_names if any(k in c.lower() for k in ['date', 'time', 'datetime', '일자', 'candle'])), None)
            code_col = next((c for c in col_names if any(k in c.lower() for k in ['code', 'stock_code', 'item_code', '종목코드'])), None)

            if code_col:
                cursor.execute(f'SELECT DISTINCT "{code_col}" FROM "{table_name}" WHERE "{code_col}" IS NOT NULL AND "{code_col}" != "";')
                codes_in_table = [str(r[0]).strip().zfill(6) for r in cursor.fetchall()]

                for c_code in codes_in_table:
                    if c_code not in all_code_list:
                        all_code_list.append(c_code)

                    # CSV 맵에서 정확한 종목명 바인딩
                    c_name = stock_name_map.get(c_code, f"미등록종목({c_code})")

                    # 해당 CODE의 레코드 수
                    cursor.execute(f'SELECT COUNT(*) FROM "{table_name}" WHERE "{code_col}" = ? OR "{code_col}" = ?;', (c_code, c_code.lstrip('0')))
                    row_cnt = cursor.fetchone()[0]
                    grand_total_rows += row_cnt

                    # 날짜 기간 및 일봉/분봉 구분
                    period_str = "-"
                    data_type = "일봉"

                    if date_col:
                        cursor.execute(f'SELECT MIN("{date_col}"), MAX("{date_col}") FROM "{table_name}" WHERE ("{code_col}" = ? OR "{code_col}" = ?) AND "{date_col}" IS NOT NULL;', (c_code, c_code.lstrip('0')))
                        min_max = cursor.fetchone()
                        if min_max and min_max[0] and min_max[1]:
                            d_first = str(min_max[0]).strip()
                            d_last = str(min_max[1]).strip()
                            
                            p_first = d_first.split()[0] if " " in d_first else d_first
                            p_last = d_last.split()[0] if " " in d_last else d_last
                            period_str = f"{p_first} ~ {p_last}"

                            # 시:분:초 포함 여부로 일봉/분봉 판별
                            if ":" in d_first or len(d_first) > 10 or "min" in table_name.lower():
                                data_type = "분봉"
                            else:
                                data_type = "일봉"

                    parsed_items.append({
                        "stock_name": c_name,
                        "stock_code": c_code,
                        "data_type": data_type,
                        "period": period_str,
                        "total_count": f"{row_cnt:,}건",
                        "field_count": f"{field_count}개 필드",
                        "table_name": table_name
                    })

        return {
            "file_info": {
                "path": DB_PATH,
                "size": file_size_mb,
                "mtime": file_mtime,
                "csv_loaded_count": len(stock_name_map)
            },
            "all_codes": all_code_list,
            "code_count": len(all_code_list),
            "parsed_items": parsed_items,
            "grand_total_count": f"{grand_total_rows:,}건",
            "table_count": len(tables)
        }

    except Exception as e:
        raise RuntimeError(f"DB/CSV 정밀 연동 파싱 중 오류: {str(e)}")
    finally:
        conn.close()


@app.get("/api/stock-info")
def get_stock_info_api():
    """웹 화면에서 호출하는 데이터 API 엔드포인트"""
    try:
        result = analyze_stock_db_with_csv()
        return {
            "success": True,
            "data": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/", response_class=HTMLResponse)
def read_root_web():
    """
    브라우저 접속 시 렌더링되는 내장 HTML 화면
    """
    html_content = """
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>주가 DB & CSV 종목명 매핑 정밀 분석</title>
        <style>
            * { box-sizing: border-box; margin: 0; padding: 0; }
            body { background-color: #121721; color: #ffffff; font-family: 'Segoe UI', sans-serif; display: flex; flex-direction: column; justify-content: center; align-items: center; min-height: 100vh; padding: 20px; }
            .container { background: #1e2430; border: 1px solid #2a3243; border-radius: 12px; padding: 25px; width: 100%; max-width: 950px; box-shadow: 0 8px 20px rgba(0, 0, 0, 0.4); }
            .title { font-size: 18px; font-weight: bold; color: #4e80ee; margin-bottom: 6px; text-align: center; }
            
            .file-meta-box { background: #181d27; border: 1px solid #2e374a; border-radius: 8px; padding: 12px 15px; margin-bottom: 15px; font-size: 12px; color: #a0aec0; }
            .meta-item { display: flex; justify-content: space-between; margin-bottom: 4px; }
            .meta-item:last-child { margin-bottom: 0; }
            .meta-val { color: #e2e8f0; font-weight: bold; }

            .code-list-box { background: #232d3f; border: 1px solid #3b4861; border-radius: 8px; padding: 12px 15px; margin-bottom: 20px; }
            .code-list-title { font-size: 12px; font-weight: bold; color: #ecc94b; margin-bottom: 6px; }
            .code-tags { display: flex; flex-wrap: wrap; gap: 6px; }
            .code-tag { background: #2d3748; color: #63b3ed; border: 1px solid #4a5568; padding: 3px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }

            table { width: 100%; border-collapse: collapse; margin-top: 5px; background: #181d27; border-radius: 8px; overflow: hidden; border: 1px solid #2e374a; }
            th, td { padding: 12px 12px; text-align: center; font-size: 13px; }
            th { background: #222938; color: #8a96a8; font-weight: bold; border-bottom: 1px solid #2e374a; }
            td { color: #ffffff; border-bottom: 1px solid #1e2430; font-weight: bold; }
            tr:hover td { background-color: #242c3d; }
            .type-day { color: #48bb78; font-weight: bold; }   /* 일봉 녹색 */
            .type-min { color: #ecc94b; font-weight: bold; }   /* 분봉 노란색 */
            .stock-title { color: #4e80ee; font-weight: bold; }
            .tbl-name-sub { display: block; font-size: 10px; color: #718096; font-weight: normal; margin-top: 2px; }

            .summary-bar { margin-top: 15px; font-size: 13px; color: #a0aec0; text-align: right; padding-right: 5px; }
            .summary-highlight { color: #4e80ee; font-weight: bold; }
            .error-box { margin-top: 15px; font-size: 12px; padding: 10px; border-radius: 6px; text-align: center; background: #742a2a; color: #feb2b2; display: none; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="title">🔍 주가 DB & CSV 종목명 연동 정밀 분석</div>
            
            <div class="file-meta-box">
                <div class="meta-item"><span>📁 DB 파일 경로:</span> <span id="meta-path" class="meta-val">-</span></div>
                <div class="meta-item"><span>⚖️ DB 파일 용량:</span> <span id="meta-size" class="meta-val" style="color: #ecc94b;">-</span></div>
                <div class="meta-item"><span>📄 CSV 매핑된 종목 수:</span> <span id="meta-csv-cnt" class="meta-val" style="color: #48bb78;">-</span></div>
            </div>

            <div class="code-list-box">
                <div class="code-list-title">📌 DB 내 추출된 종목 코드(CODE) 목록:</div>
                <div id="code-tags" class="code-tags">
                    <span style="font-size: 11px; color: #a0aec0;">CODE 목록을 읽어오는 중입니다...</span>
                </div>
            </div>

            <table>
                <thead>
                    <tr>
                        <th>종목명 (테이블명)</th>
                        <th>코드</th>
                        <th>종류</th>
                        <th>기간</th>
                        <th>갯수 (행수)</th>
                        <th>필드수</th>
                    </tr>
                </thead>
                <tbody id="table-body">
                    <tr>
                        <td colspan="6" style="color: #718096;">데이터를 정밀 파싱 중입니다...</td>
                    </tr>
                </tbody>
            </table>

            <div id="summary-bar" class="summary-bar"></div>
            <div id="error-box" class="error-box"></div>
        </div>

        <script>
            window.onload = function() {
                fetchStockInfo();
            };

            function fetchStockInfo() {
                const tbody = document.getElementById('table-body');
                const summaryBar = document.getElementById('summary-bar');
                const codeTagsDiv = document.getElementById('code-tags');
                const errorBox = document.getElementById('error-box');

                fetch('/api/stock-info')
                    .then(response => {
                        if (!response.ok) throw new Error("서버 응답 오류");
                        return response.json();
                    })
                    .then(result => {
                        if (result.success && result.data) {
                            const d = result.data;
                            
                            document.getElementById('meta-path').innerText = d.file_info.path;
                            document.getElementById('meta-size').innerText = d.file_info.size;
                            document.getElementById('meta-csv-cnt').innerText = `${d.file_info.csv_loaded_count}개 종목 매핑됨 (stock_name.csv)`;

                            codeTagsDiv.innerHTML = '';
                            if (d.all_codes && d.all_codes.length > 0) {
                                d.all_codes.forEach(code => {
                                    codeTagsDiv.innerHTML += `<span class="code-tag">${code}</span>`;
                                });
                            }

                            const parsedItems = d.parsed_items;
                            tbody.innerHTML = '';

                            if (parsedItems.length === 0) {
                                tbody.innerHTML = '<tr><td colspan="6">조회된 데이터가 없습니다.</td></tr>';
                                return;
                            }

                            parsedItems.forEach(item => {
                                const typeClass = item.data_type === '일봉' ? 'type-day' : 'type-min';
                                const row = `
                                    <tr>
                                        <td>
                                            <span class="stock-title">${item.stock_name}</span>
                                            <span class="tbl-name-sub">(${item.table_name})</span>
                                        </td>
                                        <td><span style="color:#ecc94b;">${item.stock_code}</span></td>
                                        <td class="${typeClass}">${item.data_type}</td>
                                        <td style="color:#63b3ed;">${item.period}</td>
                                        <td>${item.total_count}</td>
                                        <td style="color:#a0aec0; font-size:12px;">${item.field_count}</td>
                                    </tr>
                                `;
                                tbody.innerHTML += row;
                            });

                            summaryBar.innerHTML = `총 <span class="summary-highlight">${d.code_count}개 CODE</span> 파싱 완료 (누적 레코드: <span class="summary-highlight">${d.grand_total_count}</span>)`;
                        } else {
                            throw new Error("데이터 파싱 실패");
                        }
                    })
                    .catch(err => {
                        errorBox.style.display = 'block';
                        errorBox.innerHTML = `⚠️ DB 분석 데이터를 가져올 수 없습니다: ${err.message}`;
                    });
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


def open_browser():
    """서버 가동 직후 브라우저 자동 호출 함수"""
    time.sleep(0.8)
    webbrowser.open("http://127.0.0.1:8000")


if __name__ == "__main__":
    # 백그라운드 스레드로 브라우저 자동 오픈 예약
    threading.Thread(target=open_browser, daemon=True).start()

    # 파이썬 통합 서버 실행 (Port: 8000)
    uvicorn.run(app, host="127.0.0.1", port=8000)