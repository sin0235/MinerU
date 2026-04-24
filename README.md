# PDF to Word Studio

Flask webapp chuyen PDF thanh file Word editable bang MinerU. App giu output phu cua MinerU nhu Markdown, JSON va `layout.pdf` de doi chieu khi can debug.

## Chay webapp

```powershell
python -m pip install -r requirements.txt
python webapp\app.py
```

Mac dinh app chay tai `http://localhost:8386`.

## Google Colab

Notebook `colab_pdf_to_word_cloudflare.ipynb` dung de:

- clone/pull repo len Colab,
- cai `MinerU` + dependencies cho webapp,
- mo Cloudflare tunnel de truy cap public,
- va tuy chon sync `webapp/runtime/jobs` len Google Drive.

Nho sua `REPO_URL` trong notebook truoc khi chay.

## Cai MinerU trong Python 3.12 rieng

Moi truong Python hien tai cua may nay la `3.14`, khong phu hop cho MinerU tren Windows. Cai Python 3.12 truoc, sau do chay:

```powershell
.\scripts\setup_mineru_env.ps1
```

Sau khi script xong, dat bien moi truong cho webapp:

```powershell
$env:MINERU_PYTHON_EXE = "D:\Programs\MinerU\.venv-mineru\Scripts\python.exe"
$env:MINERU_MODEL_SOURCE = "huggingface"
$env:MINERU_VL_MODEL_NAME = "opendatalab/MinerU2.5-Pro-2604-1.2B"
python webapp\app.py
```

Co the thay `MINERU_PYTHON_EXE` bang `MINERU_COMMAND` neu muon tro thang den lenh `mineru`:

```powershell
$env:MINERU_COMMAND = "D:\Programs\MinerU\.venv-mineru\Scripts\mineru.exe"
```

## Cau hinh

- `MINERU_PYTHON_EXE`: Python trong env MinerU rieng.
- `MINERU_COMMAND`: lenh MinerU day du, uu tien cao hon `MINERU_PYTHON_EXE`.
- `MINERU_MODEL_SOURCE`: mac dinh `huggingface`.
- `MINERU_VL_MODEL_NAME`: mac dinh `opendatalab/MinerU2.5-Pro-2604-1.2B`.
- `PDF_WORD_BACKEND`: `auto`, `pipeline`, `hybrid-auto-engine`, `vlm-auto-engine`, `hybrid-http-client`, `vlm-http-client`.
- `MINERU_API_URL`: URL MinerU FastAPI khi dung API rieng.
- `PDF_WORD_MAX_UPLOAD_MB`: mac dinh `128`.
- `PDF_WORD_KEEP_ARTIFACTS`: mac dinh `true`.
- `MINERU_TIMEOUT_SECONDS`: mac dinh `3600`.

Trong che do `auto`, app uu tien `vlm-http-client` khi co `MINERU_API_URL`, dung `hybrid-auto-engine` khi env MinerU co CUDA, va fallback ve `pipeline` neu chi co CPU.
