# ================================================================================
# [파일명]: stock_data_bunsuk3.py
# [작성일시]: 260814_1445
# [코딩 목적]: 
#   1. 클라우드(Render) data 폴더 내 저장된 파일(stock_data.db, stock_name.csv 등)을
#      웹 화면에서 실시간 파일 탐색기 형태로 한눈에 확인(파일명, 용량, 저장시각)
#   2. 클라우드 파일 다운로드 및 삭제 관리 기능 제공
#   3. Render 헬스체크(HEAD / 200 OK) 유지 및 무장애 가동
#   4. DB 파싱(일봉/분봉, 종목명 매핑, COUNT, 필드수) 실시간 자동 연동
#
# [프로그램 흐름도 (Flowchart)]:
#   [서버 가동] ➔ [info 상수 콘솔 출력 (void_setup)] ➔ [FastAPI 초기화]
#          │
#          ▼
#   [Render 동적 PORT 바인딩 (0.0.0.0:PORT)]
#          │
#          ├─ HEAD /                    ➔ Render 헬스체크 200 OK
#          ├─ GET /                     ➔ 웹 대시보드 (파일 탐색기 + 주식 분석 표)
#          ├─ GET /api/file-list        ➔ 클라우드 저장 파일 목록 실시간 반환
#          ├─ POST /api/upload-file     ➔ DB/CSV 파일 업로드 및 자동 저장
#          ├─ GET /api/download-file/{name} ➔ 저장된 파일 내 PC로 다운로드
#          ├─ DELETE /api/delete-file/{name} ➔ 저장된 파일 삭제
#          └─ GET /api/stock-info       ➔ DB/CSV 통합 정밀 분석 결과 JSON
# ================================================================================

import os
import sqlite3
import csv
import datetime
import shutil
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, Response, JSONResponse, FileResponse
import uvicorn

# ================================================================================
# [코딩 11계명] 필수 info 상수 정의 및 void setup 콘솔 출력
# ================================================================================
info = {
    "title": "주가 DB & CSV 클라우드 파일 탐색/분석 통합 시스템 (ver8.0)",
    "purpose": "클라우드 저장 파일 실시간 시각화, 다운로드/삭제 관리 및 DB/CSV 무결점 정밀 분석",
    "pinNumber": f"HTTP Port {os.environ.get('PORT', '10000')} (Cloud Explorer Ready)"
}

def void_setup():
    print("================================================================================")
    print("[프로그램 정보 (void setup)]")
    print(f"제목: {info['title']}")
    print(f"목적: {info['purpose']}")
    print(f"핀번호: {info['pinNumber']}")
    print("================================================================================")

# 스크립트 실행 시 void_setup 필수 실행
void_setup()

app = FastAPI(title=info["title"])

# 클라우드 데이터 디렉토리 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "stock_data.db")
CSV_PATH = os.path.join(DATA_DIR, "stock_name.csv")


def get_stored_files_info():
    """data 폴더 내 모든 파일 목록 및 메타 정보 반환"""
    files_list = []
    if os.path.exists(DATA_DIR):
        for fname in os.listdir(DATA_DIR):
            fpath = os.path.join(DATA_DIR, fname)
            if os.path.isfile(fpath):
                size_bytes = os.path.getsize(fpath)
                if size_bytes >= 1024 * 1024:
                    size_str = f"{size_bytes / (1024 * 1024):.2f} MB"
                elif size_bytes >= 1024:
                    size_str = f"{size_bytes / 1024:.1f} KB"
                else:
                    size_str = f"{size_bytes} Bytes"
                
                mtime_str = datetime.datetime.fromtimestamp(os.path.getmtime(fpath)).strftime('%Y-%m-%d %H:%M:%S')
                files_list.append({
                    "name": fname,
                    "size": size_str,
                    "size_bytes": size_bytes,
                    "mtime": mtime_str,
                    "ext": os.path.splitext(fname)[1].lower()
                })
    return files_list


def load_stock_names():
    """stock_name.csv 로드 (인코딩 자동 판별)"""
    stock_map = {}
    target_csv = CSV_PATH if os.path.exists(CSV_PATH) else os.path.join(BASE_DIR, "stock_name.csv")
    if not os.path.exists(target_csv):
        return stock_map

    encodings = ['utf-8-sig', 'cp949', 'euc-kr', 'utf-8']
    for enc in encodings:
        try:
            with open(target_csv, mode='r', encoding=enc) as f:
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
    """DB 파일 안전 파싱 함수"""
    target_db = DB_PATH if os.path.exists(DB_PATH) else os.path.join(BASE_DIR, "stock_data.db")
    if not os.path.exists(target_db):
        return {
            "file_ready": False,
            "message": "클라우드 보관함에 stock_data.db 파일이 없습니다. 상단에서 업로드해 주세요.",
            "file_info": {"path": "미등록", "size": "0 MB", "mtime": "-", "csv_loaded_count": 0},
            "all_codes": [],
            "code_count": 0,
            "parsed_items": [],
            "grand_total_count": "0건",
            "table_count": 0
        }

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
                "message": "DB 파일이 비어있거나 테이블이 없습니다.",
                "file_info": {"path": target_db, "size": file_size_mb, "mtime": file_mtime, "csv_loaded_count": len(stock_name_map)},
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
                            data_type = "분봉" if (":" in d_first or len(d_first) > 10 or "min" in table_name.lower()) else "일봉"

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
            "message": "데이터가 정상적으로 파싱되었습니다.",
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
            "file_info": {"path": target_db, "size": file_size_mb, "mtime": file_mtime, "csv_loaded_count": 0},
            "all_codes": [],
            "code_count": 0,
            "parsed_items": [],
            "grand_total_count": "0건",
            "table_count": 0
        }
    finally:
        conn.close()


# Render 헬스체크 엔드포인트
@app.head("/")
def head_root():
    return Response(status_code=200)


@app.get("/api/file-list")
def api_get_files():
    """클라우드 저장 파일 목록 실시간 반환"""
    files = get_stored_files_info()
    return {"success": True, "files": files}


@app.get("/api/stock-info")
def api_get_stock_info():
    result = analyze_stock_db_safe()
    return {"success": True, "data": result}


@app.post("/api/upload-file")
async def api_upload_file(file: UploadFile = File(...)):
    """파일 업로드 처리"""
    try:
        filename = os.path.basename(file.filename)
        save_path = os.path.join(DATA_DIR, filename)
        with open(save_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # 주요 파일명 매핑 동기화
        if filename.endswith(('.db', '.sqlite', '.sqlite3')) and filename != "stock_data.db":
            shutil.copy(save_path, DB_PATH)
        elif filename.endswith('.csv') and filename != "stock_name.csv":
            shutil.copy(save_path, CSV_PATH)

        return JSONResponse({"success": True, "message": f"'{filename}' 업로드 완료"})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/download-file/{filename}")
def api_download_file(filename: str):
    """클라우드 파일 내 PC 다운로드"""
    safe_name = os.path.basename(filename)
    target_path = os.path.join(DATA_DIR, safe_name)
    if not os.path.exists(target_path):
        raise HTTPException(status_code=404, detail="해당 파일이 클라우드에 존재하지 않습니다.")
    return FileResponse(path=target_path, filename=safe_name, media_type='application/octet-stream')


@app.delete("/api/delete-file/{filename}")
def api_delete_file(filename: str):
    """클라우드 파일 삭제"""
    safe_name = os.path.basename(filename)
    target_path = os.path.join(DATA_DIR, safe_name)
    if os.path.exists(target_path):
        os.remove(target_path)
        return {"success": True, "message": f"'{safe_name}' 삭제 완료"}
    raise HTTPException(status_code=404, detail="삭제할 파일을 찾을 수 없습니다.")


@app.get("/", response_class=HTMLResponse)
def read_root_web():
    html_content = """
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>아란 클라우드 주가 DB & 파일 탐색기</title>
        <style>
            * { box-sizing: border-box; margin: 0; padding: 0; }
            body { background-color: #121721; color: #ffffff; font-family: 'Segoe UI', sans-serif; display: flex; flex-direction: column; align-items: center; min-height: 100vh; padding: 25px; }
            .container { background: #1e2430; border: 1px solid #2a3243; border-radius: 12px; padding: 25px; width: 100%; max-width: 980px; box-shadow: 0 8px 24px rgba(0,0,0,0.4); }
            .title { font-size: 20px; font-weight: bold; color: #4e80ee; text-align: center; margin-bottom: 4px; }
            .subtitle { font-size: 12px; color: #718096; text-align: center; margin-bottom: 20px; }
            
            /* 파일 탐색기 보관함 카드 */
            .explorer-card { background: #181d27; border: 1px solid #2e374a; border-radius: 8px; padding: 16px; margin-bottom: 20px; }
            .explorer-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; border-bottom: 1px solid #283142; padding-bottom: 8px; }
            .explorer-title { font-size: 13px; font-weight: bold; color: #ecc94b; }
            .file-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 10px; margin-bottom: 12px; }
            .file-chip { background: #232d3f; border: 1px solid #3b4861; border-radius: 6px; padding: 10px 12px; display: flex; justify-content: space-between; align-items: center; }
            .file-meta-name { font-size: 13px; font-weight: bold; color: #e2e8f0; word-break: break-all; }
            .file-meta-sub { font-size: 11px; color: #a0aec0; margin-top: 3px; }
            .file-actions { display: flex; gap: 6px; }
            .btn-action { background: #2d3748; border: 1px solid #4a5568; color: #cbd5e0; padding: 4px 8px; border-radius: 4px; font-size: 11px; cursor: pointer; text-decoration: none; }
            .btn-action:hover { background: #4a5568; color: #fff; }
            .btn-del { color: #feb2b2; }
            .btn-del:hover { background: #742a2a; color: #fff; }

            /* 업로드 구역 */
            .upload-bar { display: flex; justify-content: space-between; align-items: center; background: #232d3f; border: 1px dashed #4e80ee; border-radius: 6px; padding: 10px 14px; }
            .btn-upload { background: #4e80ee; color: #fff; border: none; padding: 6px 14px; border-radius: 4px; font-weight: bold; font-size: 12px; cursor: pointer; }
            .btn-upload:hover { background: #3b6bd6; }

            /* 종목 코드 리스트 */
            .code-list-box { background: #181d27; border: 1px solid #2e374a; border-radius: 8px; padding: 12px 15px; margin-bottom: 20px; }
            .code-list-title { font-size: 12px; font-weight: bold; color: #ecc94b; margin-bottom: 6px; }
            .code-tags { display: flex; flex-wrap: wrap; gap: 6px; }
            .code-tag { background: #2d3748; color: #63b3ed; border: 1px solid #4a5568; padding: 3px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }

            /* 데이터 표 */
            table { width: 100%; border-collapse: collapse; background: #181d27; border-radius: 8px; overflow: hidden; border: 1px solid #2e374a; }
            th, td { padding: 12px; text-align: center; font-size: 13px; }
            th { background: #222938; color: #8a96a8; font-weight: bold; border-bottom: 1px solid #2e374a; }
            td { color: #ffffff; border-bottom: 1px solid #1e2430; font-weight: bold; }
            tr:hover td { background-color: #242c3d; }
            .type-day { color: #48bb78; font-weight: bold; }
            .type-min { color: #ecc94b; font-weight: bold; }
            .stock-title { color: #4e80ee; font-weight: bold; }
            .tbl-name-sub { display: block; font-size: 10px; color: #718096; font-weight: normal; margin-top: 2px; }

            .summary-bar { margin-top: 15px; font-size: 13px; color: #a0aec0; text-align: right; }
            .summary-highlight { color: #4e80ee; font-weight: bold; }
            .status-text { font-size: 11px; color: #ecc94b; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="title">☁️ 아란 클라우드 주가 DB 대시보드</div>
            <div class="subtitle">Render Cloud Multi-View File Explorer (FastAPI Engine)</div>
            
            <!-- 📁 1. 클라우드 파일 보관함 탐색기 카드 -->
            <div class="explorer-card">
                <div class="explorer-header">
                    <span class="explorer-title">📁 클라우드 보관함 파일 현황</span>
                    <span id="file-count-text" class="status-text">로딩 중...</span>
                </div>
                <div id="file-grid" class="file-grid">
                    <div style="font-size: 12px; color: #718096;">보관된 파일 목록을 읽고 있습니다...</div>
                </div>
                
                <div class="upload-bar">
                    <span style="font-size: 12px; color: #cbd5e0;">내 컴퓨터의 파일(.db, .csv)을 클라우드로 추가/교체:</span>
                    <div>
                        <input type="file" id="file-input" style="display: none;" onchange="uploadSelectedFile()">
                        <button class="btn-upload" onclick="document.getElementById('file-input').click()">+ 파일 선택 및 업로드</button>
                    </div>
                </div>
                <div id="upload-status" style="margin-top: 8px; font-size: 11px; text-align: right;"></div>
            </div>

            <!-- 📌 2. CODE 태그 박스 -->
            <div class="code-list-box">
                <div class="code-list-title">📌 DB 내 식별된 종목 코드(CODE) 목록:</div>
                <div id="code-tags" class="code-tags">
                    <span style="font-size: 11px; color: #a0aec0;">데이터 분석 대기 중...</span>
                </div>
            </div>

            <!-- 📊 3. 전수 분석 데이터 테이블 -->
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
            window.onload = function() {
                refreshAll();
            };

            function refreshAll() {
                loadFileList();
                fetchStockInfo();
            }

            // 1. 파일 보관함 목록 로드
            function loadFileList() {
                const grid = document.getElementById('file-grid');
                const cntText = document.getElementById('file-count-text');

                fetch('/api/file-list')
                    .then(r => r.json())
                    .then(res => {
                        if (res.success) {
                            const files = res.files;
                            cntText.innerText = `총 ${files.length}개 파일 보관 중`;
                            if (files.length === 0) {
                                grid.innerHTML = '<div style="font-size: 12px; color: #ecc94b; grid-column: 1/-1; padding: 10px;">⚠️ 보관함이 비어 있습니다. 아래 버튼으로 파일을 올려주세요.</div>';
                                return;
                            }

                            grid.innerHTML = '';
                            files.forEach(f => {
                                const isDb = f.name.includes('.db') || f.name.includes('.sqlite');
                                const icon = isDb ? '🗄️' : '📄';
                                const item = `
                                    <div class="file-chip">
                                        <div>
                                            <div class="file-meta-name">${icon} ${f.name}</div>
                                            <div class="file-meta-sub">용량: <span style="color:#ecc94b;">${f.size}</span> | 저장: ${f.mtime}</div>
                                        </div>
                                        <div class="file-actions">
                                            <a href="/api/download-file/${encodeURIComponent(f.name)}" class="btn-action" title="다운로드">⬇️ 받기</a>
                                            <button onclick="deleteFile('${f.name}')" class="btn-action btn-del" title="삭제">🗑️</button>
                                        </div>
                                    </div>
                                `;
                                grid.innerHTML += item;
                            });
                        }
                    })
                    .catch(err => {
                        cntText.innerText = '파일 목록 조회 실패';
                    });
            }

            // 2. 주식 DB 정밀 분석 정보 로드
            function fetchStockInfo() {
                const tbody = document.getElementById('table-body');
                const summaryBar = document.getElementById('summary-bar');
                const codeTagsDiv = document.getElementById('code-tags');

                fetch('/api/stock-info')
                    .then(r => r.json())
                    .then(result => {
                        if (result.success && result.data) {
                            const d = result.data;
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
                                summaryBar.innerHTML = '상태: <span style="color:#ecc94b;">stock_data.db 파일 업로드 대기 중</span>';
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
                        tbody.innerHTML = `<tr><td colspan="6" style="color:#e53e3e;">서버 통신 오류: ${err.message}</td></tr>`;
                    });
            }

            // 3. 파일 업로드 실행
            function uploadSelectedFile() {
                const input = document.getElementById('file-input');
                const status = document.getElementById('upload-status');
                if (!input.files || input.files.length === 0) return;

                const file = input.files[0];
                const formData = new FormData();
                formData.append('file', file);

                status.style.color = '#63b3ed';
                status.innerText = `⏳ '${file.name}' 클라우드 전송 중...`;

                fetch('/api/upload-file', {
                    method: 'POST',
                    body: formData
                })
                .then(r => r.json())
                .then(res => {
                    if (res.success) {
                        status.style.color = '#48bb78';
                        status.innerText = `✅ '${file.name}' 업로드 성공!`;
                        input.value = '';
                        setTimeout(() => { status.innerText = ''; }, 3000);
                        refreshAll();
                    } else {
                        throw new Error(res.detail || '업로드 실패');
                    }
                })
                .catch(err => {
                    status.style.color = '#e53e3e';
                    status.innerText = `❌ 실패: ${err.message}`;
                });
            }

            // 4. 파일 삭제 실행
            function deleteFile(fname) {
                if (!confirm(`정말로 '${fname}' 파일을 클라우드에서 삭제하시겠습니까?`)) return;

                fetch(`/api/delete-file/${encodeURIComponent(fname)}`, { method: 'DELETE' })
                    .then(r => r.json())
                    .then(res => {
                        if (res.success) {
                            alert(res.message);
                            refreshAll();
                        } else {
                            alert('삭제 실패: ' + res.detail);
                        }
                    })
                    .catch(err => alert('삭제 요청 에러: ' + err.message));
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
