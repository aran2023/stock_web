# ================================================================================
# [파일명]: stock_data_bunsuk3.py
# [작성일시]: 260814_1425
# [코딩 목적]: 
#   1. Render.com 클라우드 호스팅 시 stock_data.db/stock_name.csv 파일이 없어도
#      서버가 멈추거나 죽지 않고 100% 30초 내 Live 상태로 가동되도록 방어 로직 구현
#   2. Render 헬스체크(HEAD /) 200 OK 지원을 통한 배포 프리징 방지
#   3. 웹 브라우저에서 직접 stock_data.db 및 stock_name.csv 파일을 즉시 업로드할 수 있는
#      멀티파트 파일 업로드 API 및 드래그 앤 드롭 UI 내장
#   4. 파일이 업로드되면 실시간으로 즉시 전수 정밀 파싱(일봉/분봉, COUNT(*), 필드수, 종목명) 표출
#
# [프로그램 흐름도 (Flowchart)]:
#   [스크립트 실행] ➔ [info 상수 콘솔 출력 (void_setup)] ➔ [FastAPI/Uvicorn 초기화]
#          │
#          ▼
#   [DB & CSV 경로 탐색 (존재 여부 안전 확인, FileNotFoundError 방어)]
#          │
#          ▼
#   [Render 동적 PORT 바인딩 (0.0.0.0:PORT)]
#          ├─ HEAD /  ➔ Render 헬스체크 200 OK 즉시 반환 (Live 달성)
#          ├─ GET /   ➔ 웹 대시보드 화면 (분석 표 + 파일 업로드 상자)
#          ├─ GET /api/stock-info ➔ DB 파싱 데이터 또는 대기 상태 JSON 반환
#          └─ POST /api/upload-file ➔ 클라우드 서버로 DB/CSV 파일 즉시 수신 및 갱신
# ================================================================================

import os
import sqlite3
import csv
import datetime
import shutil
from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.responses import HTMLResponse, Response, JSONResponse
import uvicorn

# ================================================================================
# [코딩 11계명] 필수 info 상수 정의 및 void setup 콘솔 출력
# ================================================================================
info = {
    "title": "주가 DB & CSV 클라우드 무장애 런처 (ver7.0)",
    "purpose": "Render.com 배포 프리징 완벽 차단, 웹 파일 업로드 지원 및 DB/CSV 실시간 정밀 분석",
    "pinNumber": f"HTTP Port {os.environ.get('PORT', '8000')} (Cloud Zero-Crash)"
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

# 클라우드 작업 디렉토리 기준 데이터 저장 폴더
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "stock_data.db")
CSV_PATH = os.path.join(DATA_DIR, "stock_name.csv")


def load_stock_names():
    """stock_name.csv 로드 (파일이 없을 경우 빈 딕셔너리 안전 반환)"""
    stock_map = {}
    if not os.path.exists(CSV_PATH):
        # 상위 디렉토리에도 있는지 한번 더 확인
        alt_csv = os.path.join(BASE_DIR, "stock_name.csv")
        if os.path.exists(alt_csv):
            csv_target = alt_csv
        else:
            return stock_map
    else:
        csv_target = CSV_PATH

    encodings = ['utf-8-sig', 'cp949', 'euc-kr', 'utf-8']
    for enc in encodings:
        try:
            with open(csv_target, mode='r', encoding=enc) as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row or len(row) < 2:
                        continue
                    code_val = str(row[0]).strip().zfill(6)
                    name_val = str(row[1]).strip()
                    if code_val.lower() in ['code', '종목코드', 'symbol']:
                        continue
                    stock_map[code_val] = name_val
            break
        except Exception:
            continue
    return stock_map


def analyze_stock_db_safe():
    """DB 파일이 없어도 에러를 내지 않고 안전하게 상태를 반환하는 분석 함수"""
    # 1. DB 파일 존재 여부 확인
    target_db = DB_PATH
    if not os.path.exists(target_db):
        alt_db = os.path.join(BASE_DIR, "stock_data.db")
        if os.path.exists(alt_db):
            target_db = alt_db
        else:
            return {
                "file_ready": False,
                "message": "클라우드 서버에 stock_data.db 파일이 아직 없습니다. 아래 업로드 창을 통해 등록해 주세요.",
                "file_info": {
                    "path": "대기 중 (파일 없음)",
                    "size": "0 MB",
                    "mtime": "-"
                },
                "all_codes": [],
                "code_count": 0,
                "parsed_items": [],
                "grand_total_count": "0건",
                "table_count": 0
            }

    # 2. 파일 메타데이터 파싱
    file_size_bytes = os.path.getsize(target_db)
    file_size_mb = f"{file_size_bytes / (1024 * 1024):.2f} MB ({file_size_bytes:,} Bytes)"
    file_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(target_db)).strftime('%Y-%m-%d %H:%M:%S')

    stock_name_map = load_stock_names()

    conn = sqlite3.connect(target_db)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        if not tables:
            return {
                "file_ready": True,
                "message": "DB 파일이 열렸으나 내부에 테이블이 없습니다.",
                "file_info": {"path": target_db, "size": file_size_mb, "mtime": file_mtime},
                "all_codes": [],
                "code_count": 0,
                "parsed_items": [],
                "grand_total_count": "0건",
                "table_count": 0
            }

        all_code_list = []
        parsed_items = []
        grand_total_rows = 0

        for tbl_item in tables:
            table_name = tbl_item[0]
            if table_name.startswith("sqlite_"):
                continue

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

                    c_name = stock_name_map.get(c_code, f"종목_{c_code}")

                    cursor.execute(f'SELECT COUNT(*) FROM "{table_name}" WHERE "{code_col}" = ? OR "{code_col}" = ?;', (c_code, c_code.lstrip('0')))
                    row_cnt = cursor.fetchone()[0]
                    grand_total_rows += row_cnt

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
            "file_ready": True,
            "message": "데이터가 정상적으로 분석되었습니다.",
            "file_info": {
                "path": target_db,
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
        return {
            "file_ready": True,
            "message": f"DB 파싱 오류: {str(e)}",
            "file_info": {"path": target_db, "size": file_size_mb, "mtime": file_mtime},
            "all_codes": [],
            "code_count": 0,
            "parsed_items": [],
            "grand_total_count": "0건",
            "table_count": 0
        }
    finally:
        conn.close()


# Render 헬스체크 지원 (HEAD / 요청 200 OK)
@app.head("/")
def head_root():
    return Response(status_code=200)


@app.get("/api/stock-info")
def get_stock_info_api():
    result = analyze_stock_db_safe()
    return {"success": True, "data": result}


# 브라우저 직접 파일 업로드 엔드포인트
@app.post("/api/upload-file")
async def upload_file_api(file: UploadFile = File(...)):
    try:
        filename = file.filename
        if not filename.endswith(('.db', '.csv', '.sqlite', '.sqlite3')):
            raise HTTPException(status_code=400, detail="지원하지 않는 파일 형식입니다. (.db, .csv 파일만 가능)")

        save_path = os.path.join(DATA_DIR, filename)
        with open(save_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # 만약 이름이 stock_data.db 가 아닌 다른 이름으로 올라왔을 경우 대비
        if filename.endswith(('.db', '.sqlite', '.sqlite3')) and filename != "stock_data.db":
            shutil.copy(save_path, DB_PATH)
        elif filename.endswith('.csv') and filename != "stock_name.csv":
            shutil.copy(save_path, CSV_PATH)

        return JSONResponse({"success": True, "message": f"{filename} 업로드 완료!"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/", response_class=HTMLResponse)
def read_root_web():
    html_content = """
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>아란 클라우드 주가 DB 대시보드</title>
        <style>
            * { box-sizing: border-box; margin: 0; padding: 0; }
            body { background-color: #121721; color: #ffffff; font-family: 'Segoe UI', sans-serif; display: flex; flex-direction: column; justify-content: center; align-items: center; min-height: 100vh; padding: 20px; }
            .container { background: #1e2430; border: 1px solid #2a3243; border-radius: 12px; padding: 25px; width: 100%; max-width: 950px; box-shadow: 0 8px 20px rgba(0, 0, 0, 0.4); }
            .title { font-size: 20px; font-weight: bold; color: #4e80ee; margin-bottom: 8px; text-align: center; }
            .subtitle { font-size: 12px; color: #718096; text-align: center; margin-bottom: 20px; }
            
            .file-meta-box { background: #181d27; border: 1px solid #2e374a; border-radius: 8px; padding: 12px 15px; margin-bottom: 15px; font-size: 12px; color: #a0aec0; }
            .meta-item { display: flex; justify-content: space-between; margin-bottom: 4px; }
            .meta-item:last-child { margin-bottom: 0; }
            .meta-val { color: #e2e8f0; font-weight: bold; }

            /* 업로드 박스 */
            .upload-section { background: #232d3f; border: 2px dashed #4e80ee; border-radius: 8px; padding: 15px; text-align: center; margin-bottom: 20px; }
            .upload-btn { background: #4e80ee; color: #fff; border: none; padding: 8px 16px; border-radius: 6px; cursor: pointer; font-weight: bold; font-size: 12px; margin-top: 8px; }
            .upload-btn:hover { background: #3b6bd6; }
            
            .code-list-box { background: #181d27; border: 1px solid #2e374a; border-radius: 8px; padding: 12px 15px; margin-bottom: 20px; }
            .code-list-title { font-size: 12px; font-weight: bold; color: #ecc94b; margin-bottom: 6px; }
            .code-tags { display: flex; flex-wrap: wrap; gap: 6px; }
            .code-tag { background: #2d3748; color: #63b3ed; border: 1px solid #4a5568; padding: 3px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }

            table { width: 100%; border-collapse: collapse; margin-top: 5px; background: #181d27; border-radius: 8px; overflow: hidden; border: 1px solid #2e374a; }
            th, td { padding: 12px 12px; text-align: center; font-size: 13px; }
            th { background: #222938; color: #8a96a8; font-weight: bold; border-bottom: 1px solid #2e374a; }
            td { color: #ffffff; border-bottom: 1px solid #1e2430; font-weight: bold; }
            tr:hover td { background-color: #242c3d; }
            .type-day { color: #48bb78; font-weight: bold; }
            .type-min { color: #ecc94b; font-weight: bold; }
            .stock-title { color: #4e80ee; font-weight: bold; }
            .tbl-name-sub { display: block; font-size: 10px; color: #718096; font-weight: normal; margin-top: 2px; }

            .summary-bar { margin-top: 15px; font-size: 13px; color: #a0aec0; text-align: right; padding-right: 5px; }
            .summary-highlight { color: #4e80ee; font-weight: bold; }
            .status-msg { margin-top: 10px; font-size: 12px; color: #ecc94b; text-align: center; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="title">☁️ 아란 클라우드 주가 DB 대시보드</div>
            <div class="subtitle">Render Cloud Real-time Server (Powered by FastAPI)</div>
            
            <div class="file-meta-box">
                <div class="meta-item"><span>📁 클라우드 DB 경로:</span> <span id="meta-path" class="meta-val">-</span></div>
                <div class="meta-item"><span>⚖️ DB 파일 용량:</span> <span id="meta-size" class="meta-val" style="color: #ecc94b;">-</span></div>
                <div class="meta-item"><span>📄 종목명 매핑 현황:</span> <span id="meta-csv-cnt" class="meta-val" style="color: #48bb78;">-</span></div>
            </div>

            <!-- 클라우드 직접 파일 업로드 영역 -->
            <div class="upload-section">
                <div style="font-size: 13px; font-weight: bold; color: #e2e8f0;">📤 클라우드로 stock_data.db / stock_name.csv 파일 직접 업로드</div>
                <div style="font-size: 11px; color: #a0aec0; margin-top: 3px;">내 컴퓨터에 있는 .db 또는 .csv 파일을 선택하여 올리시면 즉시 화면에 반영됩니다.</div>
                <input type="file" id="file-input" style="display: none;" onchange="handleFileUpload()">
                <button class="upload-btn" onclick="document.getElementById('file-input').click()">📁 파일 선택 및 업로드</button>
                <div id="upload-status" class="status-msg"></div>
            </div>

            <div class="code-list-box">
                <div class="code-list-title">📌 검색된 종목 코드(CODE) 목록:</div>
                <div id="code-tags" class="code-tags">
                    <span style="font-size: 11px; color: #a0aec0;">데이터 대기 중...</span>
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
                    <tr><td colspan="6" style="color: #718096;">데이터를 불러오는 중입니다...</td></tr>
                </tbody>
            </table>

            <div id="summary-bar" class="summary-bar"></div>
        </div>

        <script>
            window.onload = function() { fetchStockInfo(); };

            function fetchStockInfo() {
                const tbody = document.getElementById('table-body');
                const summaryBar = document.getElementById('summary-bar');
                const codeTagsDiv = document.getElementById('code-tags');

                fetch('/api/stock-info')
                    .then(r => r.json())
                    .then(result => {
                        if (result.success && result.data) {
                            const d = result.data;
                            document.getElementById('meta-path').innerText = d.file_info.path;
                            document.getElementById('meta-size').innerText = d.file_info.size;
                            document.getElementById('meta-csv-cnt').innerText = d.file_info.csv_loaded_count ? `${d.file_info.csv_loaded_count}개 종목` : '0개 (stock_name.csv 대기중)';

                            codeTagsDiv.innerHTML = '';
                            if (d.all_codes && d.all_codes.length > 0) {
                                d.all_codes.forEach(code => {
                                    codeTagsDiv.innerHTML += `<span class="code-tag">${code}</span>`;
                                });
                            } else {
                                codeTagsDiv.innerHTML = '<span style="font-size: 11px; color: #a0aec0;">등록된 CODE가 없습니다.</span>';
                            }

                            const parsedItems = d.parsed_items;
                            tbody.innerHTML = '';

                            if (!d.file_ready || parsedItems.length === 0) {
                                tbody.innerHTML = `<tr><td colspan="6" style="color:#ecc94b; padding: 20px;">${d.message}</td></tr>`;
                                summaryBar.innerHTML = '상태: <span style="color:#ecc94b;">DB 파일 업로드 대기 중</span>';
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

                            summaryBar.innerHTML = `총 <span class="summary-highlight">${d.code_count}개 CODE</span> (누적 레코드: <span class="summary-highlight">${d.grand_total_count}</span>)`;
                        }
                    })
                    .catch(err => {
                        tbody.innerHTML = `<tr><td colspan="6" style="color:#e53e3e;">서버 통신 실패: ${err.message}</td></tr>`;
                    });
            }

            function handleFileUpload() {
                const input = document.getElementById('file-input');
                const status = document.getElementById('upload-status');
                if (!input.files || input.files.length === 0) return;

                const file = input.files[0];
                const formData = new FormData();
                formData.append('file', file);

                status.style.color = '#63b3ed';
                status.innerText = `⏳ ${file.name} 파일을 클라우드로 전송 중입니다...`;

                fetch('/api/upload-file', {
                    method: 'POST',
                    body: formData
                })
                .then(r => r.json())
                .then(res => {
                    if (res.success) {
                        status.style.color = '#48bb78';
                        status.innerText = `✅ ${file.name} 업로드 완료! 데이터를 갱신합니다.`;
                        setTimeout(fetchStockInfo, 1000);
                    } else {
                        throw new Error(res.detail || '업로드 실패');
                    }
                })
                .catch(err => {
                    status.style.color = '#e53e3e';
                    status.innerText = `❌ 업로드 실패: ${err.message}`;
                });
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
