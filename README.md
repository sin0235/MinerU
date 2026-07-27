<div align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=6,12,20&height=180&section=header&text=PDF%20to%20Word%20with%20MinerU&fontSize=38&fontColor=fff&animation=twinkling&fontAlignY=35&desc=Document%20AI%20%E2%80%A2%20Layout%20Recovery%20%E2%80%A2%20DOCX%20Export&descAlignY=56&descSize=17" width="100%"/>
  <p>
    <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
    <img src="https://img.shields.io/badge/Flask-Web%20App-000000?style=for-the-badge&logo=flask&logoColor=white" alt="Flask"/>
    <img src="https://img.shields.io/badge/MinerU-Document%20AI-6A5ACD?style=for-the-badge" alt="MinerU"/>
  </p>
</div>

# PDF to Word Studio

Ứng dụng Flask chuyển PDF thành DOCX có thể chỉnh sửa bằng MinerU.

MinerU phân tích bố cục PDF. Dịch vụ này ưu tiên dữ liệu cấu trúc của MinerU để dựng lại tiêu đề, đoạn văn, danh sách, bảng, ảnh, biểu đồ, mã nguồn và công thức trong DOCX. Đây không phải bộ chuyển đổi giữ bố cục tuyệt đối; chất lượng đầu ra phụ thuộc PDF nguồn và kết quả phân tích của MinerU.

## Chức năng

- Tải PDF qua giao diện web, theo dõi tiến độ và log MinerU theo thời gian thực.
- Chọn backend, phương thức phân tích, ngôn ngữ OCR, phạm vi trang, nhận diện bảng và công thức.
- Ưu tiên `*_content_list_v2.json`, rồi `*_content_list.json`; chỉ fallback sang Markdown khi thiếu JSON phù hợp.
- Tạo DOCX với bảng HTML, ảnh, biểu đồ, LaTeX/MathML và định dạng đề thi trắc nghiệm.
- Preview DOCX/PDF, tải DOCX và ZIP artifact của từng job.
- Lớp LLM tùy chọn: chỉ kiểm tra (`review`) hoặc áp dụng các patch văn bản an toàn (`correct`).

## Luồng xử lý

```text
PDF upload
  └─ MinerU
      └─ structured JSON hoặc Markdown
          └─ normalized blocks
              └─ DOCX + artifact + preview/download
```

Mỗi job chạy trong `webapp/runtime/jobs/<job_id>/`. Thư mục này bị Git bỏ qua.

## Yêu cầu

- Python cho web app.
- Python 3.10–3.13 riêng cho MinerU.
- MinerU và model phù hợp backend đang dùng.

`requirements.txt` chỉ chứa dependency web/DOCX. MinerU được tách thành môi trường riêng để tránh ràng buộc Python và CUDA của MinerU ảnh hưởng web app.

## Chạy trên Linux/macOS

Tạo môi trường web app.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Tạo môi trường MinerU bằng Python 3.10–3.13. Ví dụ dùng Python 3.12.

```bash
python3.12 -m venv .venv-mineru
.venv-mineru/bin/python -m pip install --upgrade pip
.venv-mineru/bin/python -m pip install -U "mineru[all]" "mineru-vl-utils[transformers]"
```

Chạy web app.

```bash
export MINERU_PYTHON_EXE="$PWD/.venv-mineru/bin/python"
python -m webapp.app
```

Mở [http://127.0.0.1:8386](http://127.0.0.1:8386). Giao diện hiển thị trạng thái sẵn sàng của MinerU trước khi nhận job.

## Chạy trên Windows

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
.\scripts\setup_mineru_env.ps1
$env:MINERU_PYTHON_EXE = (Resolve-Path ".\.venv-mineru\Scripts\python.exe").Path
python -m webapp.app
```

`setup_mineru_env.ps1` cần Python 3.10–3.13; mặc định tìm Python 3.12 qua `py` launcher.

## Cấu hình MinerU

MinerU command được chọn theo thứ tự sau:

1. `MINERU_COMMAND`
2. `MINERU_PYTHON_EXE` — ứng dụng tìm CLI `mineru` trong cùng môi trường.
3. `mineru` có trong `PATH`.

Các biến thường dùng:

```bash
# App web
export PDF_WORD_WEBAPP_HOST="0.0.0.0"       # mặc định
export PDF_WORD_WEBAPP_PORT="8386"          # mặc định
export PDF_WORD_MAX_UPLOAD_MB="128"         # 1–2048
export PDF_WORD_BACKEND="auto"               # mặc định

# MinerU
export MINERU_PYTHON_EXE="$PWD/.venv-mineru/bin/python"
export MINERU_MODEL_SOURCE="huggingface"     # mặc định
export MINERU_VL_MODEL_NAME="opendatalab/MinerU2.5-Pro-2605-1.2B"
export MINERU_TIMEOUT_SECONDS="3600"         # 60–86400

# Dùng MinerU HTTP API thay cho engine cục bộ
export MINERU_API_URL="https://mineru.example/v1"
```

`PDF_WORD_BACKEND=auto` chọn `vlm-http-client` khi có `MINERU_API_URL`; nếu không, chọn `hybrid-auto-engine` khi CUDA sẵn sàng, hoặc `pipeline` khi không có CUDA. Với lỗi CUDA/vLLM của backend tự động, job sẽ thử lại bằng `pipeline`.

Các backend giao diện chấp nhận:

```text
auto
pipeline
hybrid-auto-engine
hybrid-engine
vlm-auto-engine
vlm-engine
hybrid-http-client
vlm-http-client
```

Biến bổ sung:

- `PDF_WORD_KEEP_ARTIFACTS`: mặc định `true`. Đặt `false` để lọc file output không thuộc nhóm artifact tải xuống.
- `PDF_WORD_WEBAPP_DEBUG`: mặc định `false`.
- `PDF_WORD_WEBAPP_RELOADER`: mặc định `false`.

## LLM review

LLM mặc định tắt. Khi bật, nội dung đã trích xuất được gửi tới provider đã chọn để tạo báo cáo hoặc patch văn bản. `correct` chỉ áp dụng patch qua kiểm tra an toàn của ứng dụng; không đảm bảo sửa đúng mọi lỗi ngữ nghĩa.

| Provider | API key | Base URL mặc định |
| --- | --- | --- |
| NVIDIA | `NVIDIA_API_KEY` | `https://integrate.api.nvidia.com/v1` |
| OpenRouter | `OPENROUTER_API_KEY` | `https://openrouter.ai/api/v1` |
| 9route | `ROUTER9_API_KEY` | `http://localhost:20128/v1` |

9route cũng nhận `ROUTE9_API_KEY`, `NINEROUTE_API_KEY` và `9ROUTE_API_KEY`. Base URL có thể ghi đè bằng `NVIDIA_BASE_URL`, `OPENROUTER_BASE_URL`, `ROUTER9_BASE_URL`, `ROUTE9_BASE_URL` hoặc `9ROUTE_BASE_URL`.

Trang **Settings** có thể lưu API key override, base URL và model trong `localStorage` của trình duyệt. Key này được gửi tới backend khi chạy LLM. Không dùng máy dùng chung, không chạy qua HTTP công cộng và không commit key vào repository.

## API nội bộ

| Method | Route | Mục đích |
| --- | --- | --- |
| `GET` | `/api/status` | Trạng thái MinerU và job hoàn tất gần đây |
| `POST` | `/api/convert` | Tạo job từ form `multipart/form-data` chứa trường `pdf` |
| `GET` | `/api/jobs/<job_id>` | Theo dõi tiến độ hoặc nhận kết quả job |
| `GET` | `/downloads/<job_id>/<filename>` | Tải artifact |
| `GET` | `/api/previews/<job_id>/<filename>` | Nhận preview HTML của DOCX |

Giao diện web dùng các route này. `POST /api/convert` trả `202` khi job đã vào hàng đợi.

## Kiểm tra

```bash
pytest
```

Test tập trung vào chuẩn hóa output MinerU, sinh DOCX, công thức, bảng, fallback backend, LLM provider và API Flask.

## Cấu trúc

```text
webapp/
  app.py                 Flask routes, API và preview
  pdf_service.py         MinerU, chuẩn hóa block, DOCX, LLM, job queue
  templates/             giao diện Convert và Settings
  static/                CSS, logo, voice preset
  runtime/               job và artifact phát sinh, không commit
scripts/
  setup_mineru_env.ps1   tạo môi trường MinerU trên Windows
tests/
  test_pdf_service.py    test dịch vụ chuyển đổi
requirements.txt         dependency web/DOCX
```

## Giới hạn

- PDF scan cần backend/model OCR phù hợp.
- Bảng, ảnh, công thức và bố cục phức tạp phụ thuộc chất lượng nhận diện của MinerU.
- Job chạy nền bằng một worker trong tiến trình web. Khởi động lại ứng dụng làm mất trạng thái job trong bộ nhớ; file đã tạo vẫn nằm trong `webapp/runtime/jobs/`.

---

<div align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=6,11,20&height=120&section=footer" width="100%"/>
  <em>Document AI for editable, reusable content.</em>
</div>
