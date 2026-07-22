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
