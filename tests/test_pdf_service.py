from __future__ import annotations

import io
import sys
import time
import zipfile
from pathlib import Path

from docx import Document

from webapp.app import app
from webapp.pdf_service import (
    Artifact,
    ConversionJobManager,
    ConversionResult,
    ConversionSubmission,
    ConversionOptions,
    NormalizedBlock,
    PDFConversionService,
    _mineru_cli_from_python,
    _split_math_segments,
)


def test_content_list_v2_generates_editable_docx(tmp_path: Path) -> None:
    service = PDFConversionService(tmp_path)
    data = [
        [
            {
                "type": "title",
                "content": {"title_content": [{"type": "text", "content": "1 Introduction"}], "level": 1},
            },
            {
                "type": "paragraph",
                "content": {"paragraph_content": [{"type": "text", "content": "A paragraph from MinerU."}]},
            },
            {
                "type": "table",
                "content": {
                    "table_caption": ["Table 1"],
                    "table_body": "<table><tr><th>A</th><th>B</th></tr><tr><td>1</td><td>2</td></tr></table>",
                },
            },
        ]
    ]

    blocks, page_count = service._normalize_content_list_v2(data)
    output_path = tmp_path / "out.docx"
    service._write_docx(blocks, output_path, base_dirs=[tmp_path])

    doc = Document(output_path)
    assert page_count == 1
    assert [block.kind for block in blocks] == ["title", "paragraph", "table"]
    assert "1 Introduction" in [paragraph.text for paragraph in doc.paragraphs]
    assert doc.tables[0].cell(1, 1).text == "2"


def test_legacy_content_list_normalizes_core_blocks(tmp_path: Path) -> None:
    service = PDFConversionService(tmp_path)
    data = [
        {"type": "text", "text": "Heading", "text_level": 2, "page_idx": 0},
        {"type": "list", "list_items": ["First", "Second"], "page_idx": 0},
        {"type": "equation", "text": "$$a=b$$", "page_idx": 1},
        {"type": "code", "code_body": "print('ok')", "page_idx": 1},
    ]

    blocks, page_count = service._normalize_content_list_legacy(data)

    assert page_count == 2
    assert blocks[0] == NormalizedBlock(kind="title", text="Heading", level=2, page_idx=0)
    assert blocks[1].kind == "list"
    assert blocks[1].items == ["First", "Second"]
    assert blocks[2].kind == "equation"
    assert blocks[3].kind == "code"


def test_markdown_fallback_keeps_headings_lists_tables_and_code(tmp_path: Path) -> None:
    service = PDFConversionService(tmp_path)
    blocks = service._normalize_markdown(
        """# Title

Intro text.

- One
- Two

| A | B |
| --- | --- |
| 1 | 2 |

```
print("ok")
```
"""
    )

    assert [block.kind for block in blocks] == ["title", "paragraph", "list", "table", "code"]
    assert blocks[2].items == ["One", "Two"]
    assert "<table>" in blocks[3].table_html


def test_markdown_latex_is_written_as_word_math(tmp_path: Path) -> None:
    service = PDFConversionService(tmp_path)
    blocks = service._normalize_markdown(
        """# Math

Inline \\(a+b\\) text.

\\[c=d\\]
"""
    )
    output_path = tmp_path / "math.docx"
    service._write_docx(blocks, output_path, base_dirs=[tmp_path])

    with zipfile.ZipFile(output_path) as docx:
        document_xml = docx.read("word/document.xml").decode("utf-8")

    assert "<m:oMath" in document_xml
    assert ">a<" in document_xml
    assert ">b<" in document_xml
    assert ">c<" in document_xml
    assert ">d<" in document_xml
    assert "\\(a+b\\)" not in document_xml
    assert "\\[c=d\\]" not in document_xml


def test_plain_dollar_text_is_not_treated_as_math() -> None:
    assert _split_math_segments("The price is $5 and $10.") == [(False, "The price is $5 and $10.", False)]
    assert _split_math_segments("Use $x^2 + y^2$ here.") == [
        (False, "Use ", False),
        (True, "x^2 + y^2", False),
        (False, " here.", False),
    ]


def test_run_mineru_passes_cli_options_and_config(monkeypatch, tmp_path: Path) -> None:
    service = PDFConversionService(tmp_path)
    captured = {}

    def fake_command() -> list[str]:
        return ["mineru"]

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs["env"]

        class Completed:
            returncode = 0
            stdout = ""
            stderr = ""

        return Completed()

    monkeypatch.setattr(service, "_mineru_command", fake_command)
    monkeypatch.setattr("webapp.pdf_service.subprocess.run", fake_run)
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    options = ConversionOptions(
        backend="pipeline",
        parse_method="ocr",
        language="latin",
        formula_enable=False,
        table_enable=True,
        start_page=2,
        end_page=4,
        server_url="http://engine.test/v1",
        latex_delimiters_type="b",
    )
    service._run_mineru(tmp_path / "input.pdf", output_dir, "pipeline", options)

    command = captured["command"]
    assert command[:7] == ["mineru", "-p", str(tmp_path / "input.pdf"), "-o", str(output_dir), "-b", "pipeline"]
    assert ["-m", "ocr"] == command[7:9]
    assert "-l" in command and "latin" in command
    assert "-s" in command and "2" in command
    assert "-e" in command and "4" in command
    assert "-u" in command and "http://engine.test/v1" in command
    assert captured["env"]["MINERU_FORMULA_ENABLE"] == "false"
    assert Path(captured["env"]["MINERU_TOOLS_CONFIG_JSON"]).exists()


def test_job_manager_reaches_completed_status(tmp_path: Path) -> None:
    class FakeService:
        def convert(self, submission: ConversionSubmission) -> ConversionResult:
            docx_path = tmp_path / "jobs" / submission.job_id / "docx" / "out.docx"
            docx_path.parent.mkdir(parents=True, exist_ok=True)
            Document().save(docx_path)
            artifact = Artifact("Word DOCX", docx_path, "docx/out.docx", "docx")
            return ConversionResult(
                job_id=submission.job_id,
                original_filename=submission.original_filename,
                docx_path=docx_path,
                output_dir=tmp_path,
                artifacts=[artifact],
                backend_used="pipeline",
                elapsed_seconds=0.01,
                page_count=1,
                source_kind="content_list",
                source_path=None,
            )

    manager = ConversionJobManager(FakeService())  # type: ignore[arg-type]
    submission = ConversionSubmission("abc123", "input.pdf", tmp_path / "input.pdf", 100)
    manager.enqueue(submission)

    snapshot = None
    deadline = time.time() + 3
    while time.time() < deadline:
        snapshot = manager.get_snapshot("abc123")
        if snapshot and snapshot["status"] == "completed":
            break
        time.sleep(0.05)

    assert snapshot is not None
    assert snapshot["status"] == "completed"
    assert snapshot["result"]["backend_used"] == "pipeline"


def test_api_rejects_non_pdf_upload() -> None:
    client = app.test_client()
    response = client.post(
        "/api/convert",
        data={"pdf": (io.BytesIO(b"not a pdf"), "input.txt")},
        content_type="multipart/form-data",
    )

    assert response.status_code == 400
    assert response.get_json()["ok"] is False


def test_readiness_blocks_python_314_env(monkeypatch, tmp_path: Path) -> None:
    if sys.version_info < (3, 14):
        return
    monkeypatch.setenv("MINERU_PYTHON_EXE", sys.executable)
    service = PDFConversionService(tmp_path)

    readiness = service.readiness()

    assert readiness.ready is False
    assert "Python 3.14" in readiness.message


def test_mineru_cli_fallback_checks_path(monkeypatch, tmp_path: Path) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    executable = bin_dir / ("mineru.exe" if sys.platform == "win32" else "mineru")
    executable.write_text("", encoding="utf-8")
    monkeypatch.setenv("PATH", str(bin_dir))

    assert _mineru_cli_from_python(tmp_path / "python") == executable
