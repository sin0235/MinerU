<div align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=6,12,20&height=180&section=header&text=PDF%20to%20Word%20with%20MinerU&fontSize=38&fontColor=fff&animation=twinkling&fontAlignY=35&desc=Document%20AI%20%E2%80%A2%20Layout%20Recovery%20%E2%80%A2%20DOCX%20Export&descAlignY=56&descSize=17" width="100%"/>
  <p>
    <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
    <img src="https://img.shields.io/badge/Flask-Web%20App-000000?style=for-the-badge&logo=flask&logoColor=white" alt="Flask"/>
    <img src="https://img.shields.io/badge/MinerU-Document%20AI-6A5ACD?style=for-the-badge" alt="MinerU"/>
  </p>
</div>

# PDF to Word with MinerU

A Flask application that converts complex PDF documents into editable Word files while preserving text, images, tables, and mathematical expressions where possible.

## How it works

1. Upload a PDF through the web interface.
2. MinerU extracts the document into structured Markdown and assets.
3. The conversion service rebuilds the content as a `.docx` file.
4. Download the generated Word document.

The service also handles HTML fragments, images, LaTeX, MathML, and Office Math conversion during document generation.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -U "mineru[core]"
python -m webapp.app
```

On Windows, `scripts/setup_mineru_env.ps1` prepares the MinerU environment.

## Test

```bash
pytest
```

## Structure

```text
webapp/app.py              Flask routes and application setup
webapp/pdf_service.py      MinerU execution and DOCX conversion
webapp/templates/          web interface
scripts/                   environment setup
tests/                     conversion-service checks
```

## Notes

- Conversion quality depends on the source PDF and MinerU output.
- Scanned documents may require OCR-capable MinerU models.
- Large documents require more processing time and memory.

---

<div align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=6,11,20&height=120&section=footer" width="100%"/>
  <em>Document AI for editable, reusable content.</em>
</div>
