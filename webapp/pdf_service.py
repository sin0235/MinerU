from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import time
import uuid
import html.entities
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any

from bs4 import BeautifulSoup
from docx import Document
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import nsdecls
from docx.shared import Inches, Pt
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename


class ConversionError(RuntimeError):
    """Raised when a PDF conversion cannot be completed."""


ALLOWED_BACKENDS = {
    "auto",
    "pipeline",
    "hybrid-auto-engine",
    "vlm-auto-engine",
    "hybrid-http-client",
    "vlm-http-client",
}
ALLOWED_PARSE_METHODS = {"auto", "txt", "ocr"}
ALLOWED_LANGUAGES = {
    "ch",
    "ch_server",
    "ch_lite",
    "en",
    "korean",
    "japan",
    "chinese_cht",
    "ta",
    "te",
    "ka",
    "th",
    "el",
    "latin",
    "arabic",
    "east_slavic",
    "cyrillic",
    "devanagari",
}
ALLOWED_LATEX_DELIMITER_TYPES = {"a", "b", "all"}
SKIPPED_CONTENT_TYPES = {
    "header",
    "footer",
    "page_header",
    "page_footer",
    "page_number",
    "page_aside_text",
    "aside_text",
    "page_footnote",
    "seal",
}


@dataclass(slots=True)
class ConversionOptions:
    backend: str = "auto"
    parse_method: str = "auto"
    language: str = "ch"
    formula_enable: bool = True
    table_enable: bool = True
    start_page: int = 0
    end_page: int | None = None
    server_url: str = ""
    latex_delimiters_type: str = "b"

    def to_payload(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "parse_method": self.parse_method,
            "language": self.language,
            "formula_enable": self.formula_enable,
            "table_enable": self.table_enable,
            "start_page": self.start_page,
            "end_page": self.end_page,
            "server_url": self.server_url,
            "latex_delimiters_type": self.latex_delimiters_type,
        }


@dataclass(slots=True)
class ReadinessInfo:
    ready: bool
    message: str
    command: list[str]
    backend: str
    python_version: str | None = None
    warnings: list[str] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "message": self.message,
            "command": self.command,
            "backend": self.backend,
            "python_version": self.python_version,
            "warnings": list(self.warnings),
        }


@dataclass(slots=True)
class ConversionSubmission:
    job_id: str
    original_filename: str
    input_path: Path
    input_size_bytes: int
    options: ConversionOptions = field(default_factory=ConversionOptions)


@dataclass(slots=True)
class Artifact:
    label: str
    path: Path
    relative_path: str
    kind: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "filename": self.path.name,
            "relative_path": self.relative_path.replace("\\", "/"),
            "kind": self.kind,
            "size_bytes": self.path.stat().st_size if self.path.exists() else 0,
        }


@dataclass(slots=True)
class ConversionResult:
    job_id: str
    original_filename: str
    docx_path: Path
    output_dir: Path
    artifacts: list[Artifact]
    backend_used: str
    elapsed_seconds: float
    page_count: int
    source_kind: str
    source_path: Path | None
    warnings: list[str] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "original_filename": self.original_filename,
            "download_name": self.docx_path.name,
            "backend_used": self.backend_used,
            "elapsed_seconds": round(self.elapsed_seconds, 2),
            "page_count": self.page_count,
            "source_kind": self.source_kind,
            "source_file": self.source_path.name if self.source_path else "",
            "warnings": list(self.warnings),
            "artifacts": [artifact.to_payload() for artifact in self.artifacts],
        }


@dataclass(slots=True)
class NormalizedBlock:
    kind: str
    text: str = ""
    level: int = 0
    items: list[str] = field(default_factory=list)
    table_html: str = ""
    image_path: str = ""
    caption: str = ""
    footnote: str = ""
    language: str = ""
    page_idx: int | None = None


class PDFConversionService:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.runtime_dir = self.root / "webapp" / "runtime"
        self.jobs_dir = self.runtime_dir / "jobs"
        self.jobs_dir.mkdir(parents=True, exist_ok=True)

        self.max_upload_mb = _env_int("PDF_WORD_MAX_UPLOAD_MB", default=128, minimum=1, maximum=2048)
        self.max_upload_bytes = self.max_upload_mb * 1024 * 1024
        self.keep_artifacts = _env_flag("PDF_WORD_KEEP_ARTIFACTS", default=True)
        self.model_source = (os.getenv("MINERU_MODEL_SOURCE") or "huggingface").strip() or "huggingface"
        self.vl_model_name = (
            os.getenv("MINERU_VL_MODEL_NAME") or "opendatalab/MinerU2.5-Pro-2604-1.2B"
        ).strip()
        self.api_url = (os.getenv("MINERU_API_URL") or "").strip()
        self.timeout_seconds = _env_int("MINERU_TIMEOUT_SECONDS", default=3600, minimum=60, maximum=24 * 3600)

    def create_submission(self, upload: FileStorage) -> ConversionSubmission:
        return self.create_submission_with_options(upload, ConversionOptions())

    def create_submission_with_options(
        self,
        upload: FileStorage,
        options: ConversionOptions | None = None,
    ) -> ConversionSubmission:
        if not upload or not upload.filename:
            raise ValueError("Can upload file PDF.")

        original_filename = upload.filename
        extension = Path(original_filename).suffix.lower()
        if extension != ".pdf":
            raise ValueError("Chi ho tro file PDF.")

        job_id = uuid.uuid4().hex
        job_dir = self.job_dir(job_id)
        input_dir = job_dir / "input"
        input_dir.mkdir(parents=True, exist_ok=True)

        safe_name = secure_filename(Path(original_filename).stem) or "document"
        input_path = input_dir / f"{safe_name}.pdf"
        upload.save(input_path)
        size_bytes = input_path.stat().st_size
        if size_bytes <= 0:
            shutil.rmtree(job_dir, ignore_errors=True)
            raise ValueError("File PDF rong.")
        if size_bytes > self.max_upload_bytes:
            shutil.rmtree(job_dir, ignore_errors=True)
            raise ValueError(f"File PDF qua lon. Gioi han hien tai la {self.max_upload_mb} MB.")

        return ConversionSubmission(
            job_id=job_id,
            original_filename=original_filename,
            input_path=input_path,
            input_size_bytes=size_bytes,
            options=options or ConversionOptions(),
        )

    def job_dir(self, job_id: str) -> Path:
        safe_id = re.sub(r"[^a-f0-9]", "", job_id.lower())
        if not safe_id:
            raise ConversionError("Job id khong hop le.")
        return self.jobs_dir / safe_id

    def readiness(self) -> ReadinessInfo:
        command = self._mineru_command()
        backend = self.resolve_backend()
        warnings: list[str] = []
        python_version = self._configured_python_version()

        if python_version and _version_tuple(python_version) >= (3, 14):
            return ReadinessInfo(
                ready=False,
                message=(
                    "MinerU khong ho tro Python 3.14 tren Windows. Hay cai Python 3.12, tao env rieng, "
                    "roi dat MINERU_PYTHON_EXE hoac MINERU_COMMAND tro toi env do."
                ),
                command=command,
                backend=backend,
                python_version=python_version,
                warnings=warnings,
            )

        try:
            completed = subprocess.run(
                [*command, "--help"],
                cwd=self.root,
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                timeout=20,
            )
        except FileNotFoundError:
            return ReadinessInfo(
                ready=False,
                message=(
                    "Khong tim thay lenh MinerU. Cai Python 3.12 + mineru[all], sau do dat "
                    "MINERU_PYTHON_EXE hoac MINERU_COMMAND."
                ),
                command=command,
                backend=backend,
                python_version=python_version,
                warnings=warnings,
            )
        except subprocess.TimeoutExpired:
            return ReadinessInfo(
                ready=False,
                message="Lenh MinerU --help bi timeout. Kiem tra env MinerU hoac thu chay lenh trong terminal.",
                command=command,
                backend=backend,
                python_version=python_version,
                warnings=warnings,
            )

        if completed.returncode != 0:
            detail = _compact_process_error(completed.stderr or completed.stdout)
            return ReadinessInfo(
                ready=False,
                message=f"MinerU chua san sang: {detail}",
                command=command,
                backend=backend,
                python_version=python_version,
                warnings=warnings,
            )

        if backend == "pipeline":
            warnings.append("Dang dung backend pipeline CPU; do chinh xac co the thap hon MinerU VLM.")
        if not self.api_url and backend in {"vlm-http-client", "hybrid-http-client"}:
            warnings.append("Backend HTTP client can MINERU_API_URL de ket noi MinerU API.")

        return ReadinessInfo(
            ready=True,
            message="MinerU CLI san sang.",
            command=command,
            backend=backend,
            python_version=python_version,
            warnings=warnings,
        )

    def resolve_backend(self, requested: str | None = None) -> str:
        requested = (requested or os.getenv("PDF_WORD_BACKEND") or "auto").strip() or "auto"
        if requested not in ALLOWED_BACKENDS:
            return "pipeline"
        if requested != "auto":
            return requested
        if self.api_url:
            return "vlm-http-client"
        return "hybrid-auto-engine" if self._mineru_cuda_available() else "pipeline"

    def convert(self, submission: ConversionSubmission) -> ConversionResult:
        readiness = self.readiness()
        if not readiness.ready:
            raise ConversionError(readiness.message)

        started = time.perf_counter()
        job_dir = self.job_dir(submission.job_id)
        mineru_output_dir = job_dir / "mineru"
        docx_dir = job_dir / "docx"
        mineru_output_dir.mkdir(parents=True, exist_ok=True)
        docx_dir.mkdir(parents=True, exist_ok=True)

        backend = self.resolve_backend(submission.options.backend)
        self._run_mineru(submission.input_path, mineru_output_dir, backend, submission.options)

        blocks, source_path, source_kind, page_count, warnings = self._load_normalized_blocks(mineru_output_dir)
        if not blocks:
            raise ConversionError("MinerU khong tra ve noi dung doc duoc de tao DOCX.")

        docx_path = docx_dir / f"{secure_filename(Path(submission.original_filename).stem) or 'document'}.docx"
        self._write_docx(blocks, docx_path, base_dirs=[mineru_output_dir, source_path.parent if source_path else mineru_output_dir])

        artifacts = self._collect_artifacts(job_dir, docx_path)
        if not self.keep_artifacts:
            self._remove_non_download_artifacts(job_dir, artifacts)
            artifacts = self._collect_artifacts(job_dir, docx_path)

        return ConversionResult(
            job_id=submission.job_id,
            original_filename=submission.original_filename,
            docx_path=docx_path,
            output_dir=mineru_output_dir,
            artifacts=artifacts,
            backend_used=backend,
            elapsed_seconds=time.perf_counter() - started,
            page_count=page_count,
            source_kind=source_kind,
            source_path=source_path,
            warnings=[*readiness.warnings, *warnings],
        )

    def resolve_download(self, job_id: str, relative_path: str) -> Path:
        job_dir = self.job_dir(job_id).resolve()
        candidate = (job_dir / relative_path).resolve()
        if not _is_relative_to(candidate, job_dir) or not candidate.exists() or not candidate.is_file():
            raise FileNotFoundError(relative_path)
        return candidate

    def _run_mineru(self, pdf_path: Path, output_dir: Path, backend: str, options: ConversionOptions) -> None:
        command = [
            *self._mineru_command(),
            "-p",
            str(pdf_path),
            "-o",
            str(output_dir),
            "-b",
            backend,
            "-m",
            options.parse_method,
            "-l",
            options.language,
            "-f",
            _cli_bool(options.formula_enable),
            "-t",
            _cli_bool(options.table_enable),
        ]
        if options.start_page > 0:
            command.extend(["-s", str(options.start_page)])
        if options.end_page is not None:
            command.extend(["-e", str(options.end_page)])
        if options.server_url:
            command.extend(["-u", options.server_url])
        if self.api_url:
            command.extend(["--api-url", self.api_url])

        env = os.environ.copy()
        env.setdefault("MINERU_MODEL_SOURCE", self.model_source)
        env["MINERU_FORMULA_ENABLE"] = _env_bool(options.formula_enable)
        env["MINERU_TABLE_ENABLE"] = _env_bool(options.table_enable)
        env["MINERU_TOOLS_CONFIG_JSON"] = str(_write_mineru_config(output_dir, options))
        if self.vl_model_name:
            env.setdefault("MINERU_VL_MODEL_NAME", self.vl_model_name)

        completed = subprocess.run(
            command,
            cwd=self.root,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=self.timeout_seconds,
            env=env,
        )
        (output_dir / "mineru_stdout.log").write_text(completed.stdout or "", encoding="utf-8")
        (output_dir / "mineru_stderr.log").write_text(completed.stderr or "", encoding="utf-8")
        if completed.returncode != 0:
            detail = _compact_process_error(completed.stderr or completed.stdout)
            raise ConversionError(f"MinerU xu ly that bai: {detail}")

    def _mineru_command(self) -> list[str]:
        raw_command = (os.getenv("MINERU_COMMAND") or "").strip()
        if raw_command:
            return _split_command(raw_command)

        python_exe = (os.getenv("MINERU_PYTHON_EXE") or "").strip()
        if python_exe:
            cli_path = _mineru_cli_from_python(Path(python_exe))
            if cli_path is not None:
                return [str(cli_path)]
            return [python_exe, "-m", "mineru"]

        return ["mineru"]

    def _configured_python_version(self) -> str | None:
        python_exe = (os.getenv("MINERU_PYTHON_EXE") or "").strip()
        if not python_exe:
            return None
        try:
            completed = subprocess.run(
                [python_exe, "-c", "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')"],
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                timeout=10,
            )
        except Exception:
            return None
        if completed.returncode != 0:
            return None
        return (completed.stdout or "").strip() or None

    def _mineru_cuda_available(self) -> bool:
        python_exe = (os.getenv("MINERU_PYTHON_EXE") or "").strip()
        if python_exe:
            try:
                completed = subprocess.run(
                    [
                        python_exe,
                        "-c",
                        "import torch; print('1' if torch.cuda.is_available() else '0')",
                    ],
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    capture_output=True,
                    timeout=10,
                )
                return completed.returncode == 0 and completed.stdout.strip() == "1"
            except Exception:
                return False

        try:
            import torch

            return bool(torch.cuda.is_available())
        except Exception:
            return False

    def _load_normalized_blocks(self, output_dir: Path) -> tuple[list[NormalizedBlock], Path | None, str, int, list[str]]:
        warnings: list[str] = []
        v2_path = _latest_file(output_dir, "*_content_list_v2.json")
        if v2_path:
            try:
                data = json.loads(v2_path.read_text(encoding="utf-8"))
                blocks, page_count = self._normalize_content_list_v2(data)
                if blocks:
                    return blocks, v2_path, "content_list_v2", page_count, warnings
                warnings.append("content_list_v2.json ton tai nhung khong co block doc duoc.")
            except Exception as exc:
                warnings.append(f"Khong doc duoc content_list_v2.json: {exc}")

        legacy_path = _latest_file(output_dir, "*_content_list.json")
        if legacy_path:
            try:
                data = json.loads(legacy_path.read_text(encoding="utf-8"))
                blocks, page_count = self._normalize_content_list_legacy(data)
                if blocks:
                    return blocks, legacy_path, "content_list", page_count, warnings
                warnings.append("content_list.json ton tai nhung khong co block doc duoc.")
            except Exception as exc:
                warnings.append(f"Khong doc duoc content_list.json: {exc}")

        markdown_path = _latest_file(output_dir, "*.md")
        if markdown_path:
            text = markdown_path.read_text(encoding="utf-8", errors="replace")
            blocks = self._normalize_markdown(text)
            if blocks:
                warnings.append("Da fallback sang Markdown vi khong co structured JSON phu hop.")
                return blocks, markdown_path, "markdown", 0, warnings

        return [], None, "none", 0, warnings

    def _normalize_content_list_v2(self, data: Any) -> tuple[list[NormalizedBlock], int]:
        blocks: list[NormalizedBlock] = []
        pages = _as_pages(data)
        for page_idx, page_items in enumerate(pages):
            for item in page_items:
                if not isinstance(item, dict):
                    continue
                kind = str(item.get("type") or "").strip()
                if not kind or kind in SKIPPED_CONTENT_TYPES:
                    continue
                content = item.get("content") if isinstance(item.get("content"), dict) else {}
                normalized = self._normalize_v2_item(kind, content, item, page_idx)
                if normalized is not None:
                    blocks.append(normalized)
        return blocks, len(pages)

    def _normalize_v2_item(
        self,
        kind: str,
        content: dict[str, Any],
        raw_item: dict[str, Any],
        page_idx: int,
    ) -> NormalizedBlock | None:
        if kind == "title":
            return NormalizedBlock(
                kind="title",
                text=_rich_text_to_plain(content.get("title_content")),
                level=_clamp_heading_level(content.get("level")),
                page_idx=page_idx,
            )
        if kind == "paragraph":
            text = _rich_text_to_plain(content.get("paragraph_content"))
            return NormalizedBlock(kind="paragraph", text=text, page_idx=page_idx) if text else None
        if kind in {"list", "index"}:
            items = _to_string_list(content.get("list_items"))
            if not items:
                text = _rich_text_to_plain(content)
                items = [text] if text else []
            return NormalizedBlock(kind="list", items=items, page_idx=page_idx) if items else None
        if kind in {"equation_interline", "equation"}:
            text = _rich_text_to_plain(_first(content, "math_content", "equation_content", "text", "content"))
            return NormalizedBlock(kind="equation", text=text, page_idx=page_idx) if text else None
        if kind in {"image", "chart"}:
            caption = _rich_text_to_plain(_first(content, f"{kind}_caption", "caption"))
            footnote = _rich_text_to_plain(_first(content, f"{kind}_footnote", "footnote"))
            image_path = str(_first(content, "image_path", "img_path", "path") or raw_item.get("img_path") or "")
            extracted = _rich_text_to_plain(_first(content, f"{kind}_content", "content"))
            return NormalizedBlock(
                kind=kind,
                text=extracted,
                image_path=image_path,
                caption=caption,
                footnote=footnote,
                page_idx=page_idx,
            )
        if kind == "table":
            caption = _rich_text_to_plain(_first(content, "table_caption", "caption"))
            footnote = _rich_text_to_plain(_first(content, "table_footnote", "footnote"))
            table_html = str(_first(content, "table_body", "table_html", "html") or "")
            image_path = str(_first(content, "image_path", "img_path", "path") or raw_item.get("img_path") or "")
            fallback_text = _rich_text_to_plain(_first(content, "table_content", "content"))
            return NormalizedBlock(
                kind="table",
                text=fallback_text,
                table_html=table_html,
                image_path=image_path,
                caption=caption,
                footnote=footnote,
                page_idx=page_idx,
            )
        if kind in {"code", "algorithm"}:
            code = _rich_text_to_plain(_first(content, "code_content", "algorithm_content", "code_body", "content"))
            caption = _rich_text_to_plain(_first(content, "code_caption", "algorithm_caption", "caption"))
            footnote = _rich_text_to_plain(_first(content, "code_footnote", "algorithm_footnote", "footnote"))
            language = str(_first(content, "code_language", "language") or "")
            return NormalizedBlock(
                kind="code",
                text=code,
                caption=caption,
                footnote=footnote,
                language=language,
                page_idx=page_idx,
            ) if code else None

        text = _rich_text_to_plain(content or raw_item.get("content"))
        return NormalizedBlock(kind="paragraph", text=text, page_idx=page_idx) if text else None

    def _normalize_content_list_legacy(self, data: Any) -> tuple[list[NormalizedBlock], int]:
        items = data if isinstance(data, list) else []
        blocks: list[NormalizedBlock] = []
        max_page_idx = -1
        for item in items:
            if not isinstance(item, dict):
                continue
            raw_kind = str(item.get("type") or "").strip()
            if not raw_kind or raw_kind in SKIPPED_CONTENT_TYPES:
                continue
            page_idx = item.get("page_idx") if isinstance(item.get("page_idx"), int) else None
            if page_idx is not None:
                max_page_idx = max(max_page_idx, page_idx)

            if raw_kind == "text":
                text = _rich_text_to_plain(item.get("text"))
                level = _clamp_heading_level(item.get("text_level"))
                blocks.append(
                    NormalizedBlock(
                        kind="title" if level else "paragraph",
                        text=text,
                        level=level,
                        page_idx=page_idx,
                    )
                )
            elif raw_kind == "list":
                items_text = _to_string_list(item.get("list_items"))
                if not items_text:
                    text = _rich_text_to_plain(item.get("text"))
                    items_text = [line.strip() for line in text.splitlines() if line.strip()]
                if items_text:
                    blocks.append(NormalizedBlock(kind="list", items=items_text, page_idx=page_idx))
            elif raw_kind in {"image", "chart"}:
                blocks.append(
                    NormalizedBlock(
                        kind=raw_kind,
                        text=_rich_text_to_plain(item.get("content")),
                        image_path=str(item.get("img_path") or item.get("image_path") or ""),
                        caption=_rich_text_to_plain(item.get(f"{raw_kind}_caption")),
                        footnote=_rich_text_to_plain(item.get(f"{raw_kind}_footnote")),
                        page_idx=page_idx,
                    )
                )
            elif raw_kind == "table":
                blocks.append(
                    NormalizedBlock(
                        kind="table",
                        table_html=str(item.get("table_body") or ""),
                        image_path=str(item.get("img_path") or item.get("image_path") or ""),
                        caption=_rich_text_to_plain(item.get("table_caption")),
                        footnote=_rich_text_to_plain(item.get("table_footnote")),
                        text=_rich_text_to_plain(item.get("content")),
                        page_idx=page_idx,
                    )
                )
            elif raw_kind == "equation":
                text = _rich_text_to_plain(item.get("text") or item.get("content"))
                if text:
                    blocks.append(NormalizedBlock(kind="equation", text=text, page_idx=page_idx))
            elif raw_kind == "code":
                code = _rich_text_to_plain(item.get("code_body") or item.get("content"))
                if code:
                    blocks.append(
                        NormalizedBlock(
                            kind="code",
                            text=code,
                            caption=_rich_text_to_plain(item.get("code_caption")),
                            footnote=_rich_text_to_plain(item.get("code_footnote")),
                            page_idx=page_idx,
                        )
                    )

        page_count = max_page_idx + 1 if max_page_idx >= 0 else 0
        return [block for block in blocks if _block_has_content(block)], page_count

    def _normalize_markdown(self, markdown: str) -> list[NormalizedBlock]:
        blocks: list[NormalizedBlock] = []
        paragraph_lines: list[str] = []
        list_items: list[str] = []
        table_lines: list[str] = []
        code_lines: list[str] = []
        in_code = False

        def flush_paragraph() -> None:
            nonlocal paragraph_lines
            text = " ".join(line.strip() for line in paragraph_lines if line.strip()).strip()
            if text:
                blocks.append(NormalizedBlock(kind="paragraph", text=text))
            paragraph_lines = []

        def flush_list() -> None:
            nonlocal list_items
            if list_items:
                blocks.append(NormalizedBlock(kind="list", items=list_items))
            list_items = []

        def flush_table() -> None:
            nonlocal table_lines
            if table_lines:
                blocks.append(NormalizedBlock(kind="table", table_html=_markdown_table_to_html(table_lines)))
            table_lines = []

        def flush_code() -> None:
            nonlocal code_lines
            code = "\n".join(code_lines).strip("\n")
            if code:
                blocks.append(NormalizedBlock(kind="code", text=code))
            code_lines = []

        for line in markdown.splitlines():
            stripped = line.strip()
            if stripped.startswith("```"):
                if in_code:
                    flush_code()
                    in_code = False
                else:
                    flush_paragraph()
                    flush_list()
                    flush_table()
                    in_code = True
                continue
            if in_code:
                code_lines.append(line)
                continue
            if not stripped:
                flush_paragraph()
                flush_list()
                flush_table()
                continue
            heading = re.match(r"^(#{1,6})\s+(.+)$", stripped)
            if heading:
                flush_paragraph()
                flush_list()
                flush_table()
                blocks.append(
                    NormalizedBlock(kind="title", text=heading.group(2).strip(), level=min(len(heading.group(1)), 6))
                )
                continue
            if _is_display_math_line(stripped):
                flush_paragraph()
                flush_list()
                flush_table()
                blocks.append(NormalizedBlock(kind="equation", text=_strip_math_delimiters(stripped)))
                continue
            list_match = re.match(r"^([-*+]|\d+[.)])\s+(.+)$", stripped)
            if list_match:
                flush_paragraph()
                flush_table()
                list_items.append(list_match.group(2).strip())
                continue
            if stripped.startswith("|") and stripped.endswith("|"):
                flush_paragraph()
                flush_list()
                table_lines.append(stripped)
                continue
            flush_list()
            flush_table()
            paragraph_lines.append(stripped)

        flush_code()
        flush_paragraph()
        flush_list()
        flush_table()
        return [block for block in blocks if _block_has_content(block)]

    def _write_docx(self, blocks: list[NormalizedBlock], output_path: Path, *, base_dirs: list[Path]) -> None:
        document = Document()
        styles = document.styles
        normal = styles["Normal"]
        normal.font.name = "Arial"
        normal.font.size = Pt(11)

        for block in blocks:
            if block.kind == "title":
                document.add_heading(block.text, level=max(1, min(block.level or 1, 4)))
            elif block.kind == "paragraph":
                self._add_text_paragraph(document, block.text)
            elif block.kind == "list":
                for item in block.items:
                    if item.strip():
                        paragraph = document.add_paragraph(style="List Bullet")
                        _append_text_with_math(paragraph, item.strip())
            elif block.kind == "table":
                self._add_table_block(document, block, base_dirs)
            elif block.kind in {"image", "chart"}:
                self._add_visual_block(document, block, base_dirs)
            elif block.kind == "equation":
                self._add_equation_block(document, block.text)
            elif block.kind == "code":
                if block.caption:
                    document.add_paragraph(block.caption).runs[0].italic = True
                paragraph = document.add_paragraph()
                run = paragraph.add_run(block.text)
                run.font.name = "Consolas"
                run.font.size = Pt(9)
            if block.footnote:
                footnote = document.add_paragraph(block.footnote)
                if footnote.runs:
                    footnote.runs[0].italic = True

        output_path.parent.mkdir(parents=True, exist_ok=True)
        document.save(output_path)

    def _add_text_paragraph(self, document: Document, text: str) -> None:
        paragraph = document.add_paragraph()
        _append_text_with_math(paragraph, text)

    def _add_equation_block(self, document: Document, latex: str) -> None:
        paragraph = document.add_paragraph()
        _append_math(paragraph, _strip_math_delimiters(latex), display=True)

    def _add_table_block(self, document: Document, block: NormalizedBlock, base_dirs: list[Path]) -> None:
        if block.caption:
            document.add_paragraph(block.caption).runs[0].italic = True

        matrix = _html_table_to_matrix(block.table_html)
        if matrix:
            col_count = max(len(row) for row in matrix)
            table = document.add_table(rows=len(matrix), cols=col_count)
            table.style = "Table Grid"
            for row_idx, row in enumerate(matrix):
                for col_idx in range(col_count):
                    table.cell(row_idx, col_idx).text = row[col_idx] if col_idx < len(row) else ""
            document.add_paragraph()
            return

        if block.text:
            self._add_text_paragraph(document, block.text)
            return

        self._add_visual_block(document, block, base_dirs)

    def _add_visual_block(self, document: Document, block: NormalizedBlock, base_dirs: list[Path]) -> None:
        if block.caption:
            document.add_paragraph(block.caption).runs[0].italic = True

        image_path = _resolve_artifact_path(block.image_path, base_dirs)
        if image_path and image_path.exists():
            try:
                document.add_picture(str(image_path), width=Inches(6.0))
            except Exception:
                document.add_paragraph(f"[Khong the chen anh: {image_path.name}]")
        elif block.image_path:
            document.add_paragraph(f"[Khong tim thay anh: {block.image_path}]")

        if block.text:
            self._add_text_paragraph(document, block.text)

    def _collect_artifacts(self, job_dir: Path, docx_path: Path) -> list[Artifact]:
        candidates: list[Path] = [docx_path]
        patterns = [
            "*.md",
            "*_content_list_v2.json",
            "*_content_list.json",
            "*_layout.pdf",
            "*_span.pdf",
            "*_middle.json",
            "*_model.json",
            "mineru_stdout.log",
            "mineru_stderr.log",
        ]
        for pattern in patterns:
            candidates.extend(job_dir.rglob(pattern))

        artifacts: list[Artifact] = []
        seen: set[Path] = set()
        for path in candidates:
            if not path.exists() or not path.is_file():
                continue
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            artifacts.append(
                Artifact(
                    label=_artifact_label(path),
                    path=path,
                    relative_path=str(path.resolve().relative_to(job_dir.resolve())),
                    kind=_artifact_kind(path),
                )
            )
        return artifacts

    def _remove_non_download_artifacts(self, job_dir: Path, artifacts: list[Artifact]) -> None:
        keep = {artifact.path.resolve() for artifact in artifacts if artifact.kind in {"docx", "markdown", "json", "layout"}}
        for path in job_dir.rglob("*"):
            if path.is_file() and path.resolve() not in keep:
                path.unlink(missing_ok=True)


class ConversionJobManager:
    def __init__(self, service: PDFConversionService, *, max_workers: int = 1, retention_seconds: int = 24 * 60 * 60) -> None:
        self._service = service
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="pdf-word-job")
        self._retention_seconds = retention_seconds
        self._jobs: dict[str, dict[str, Any]] = {}
        self._lock = Lock()

    def enqueue(self, submission: ConversionSubmission) -> str:
        self._purge_expired()
        now = time.time()
        snapshot = {
            "id": submission.job_id,
            "status": "queued",
            "created_at": now,
            "updated_at": now,
            "message": "Da nhan file PDF. Job dang cho worker xu ly.",
            "original_filename": submission.original_filename,
            "input_size_bytes": submission.input_size_bytes,
            "result": None,
            "error": None,
        }
        with self._lock:
            self._jobs[submission.job_id] = snapshot
        self._executor.submit(self._run_job, submission)
        return submission.job_id

    def get_snapshot(self, job_id: str) -> dict[str, Any] | None:
        self._purge_expired()
        with self._lock:
            snapshot = self._jobs.get(job_id)
            return dict(snapshot) if snapshot else None

    def recent_results(self, *, limit: int = 8) -> list[dict[str, Any]]:
        self._purge_expired()
        with self._lock:
            completed = [
                dict(snapshot)
                for snapshot in self._jobs.values()
                if snapshot.get("status") == "completed" and isinstance(snapshot.get("result"), dict)
            ]
        completed.sort(key=lambda item: float(item.get("updated_at", 0)), reverse=True)
        return completed[:limit]

    def _run_job(self, submission: ConversionSubmission) -> None:
        self._update_job(submission.job_id, status="running", message="MinerU dang phan tich PDF va tao file Word.")
        try:
            result = self._service.convert(submission)
        except ConversionError as exc:
            self._update_job(submission.job_id, status="failed", error=str(exc), message=str(exc))
            return
        except Exception as exc:
            self._update_job(
                submission.job_id,
                status="failed",
                error=f"Loi khong mong muon: {exc}",
                message=f"Loi khong mong muon: {exc}",
            )
            return

        self._update_job(
            submission.job_id,
            status="completed",
            message="Da tao DOCX thanh cong.",
            result=result.to_payload(),
            error=None,
        )

    def _update_job(self, job_id: str, **updates: Any) -> None:
        with self._lock:
            snapshot = self._jobs.get(job_id)
            if snapshot is None:
                return
            snapshot.update(updates)
            snapshot["updated_at"] = time.time()

    def _purge_expired(self) -> None:
        now = time.time()
        with self._lock:
            expired = [
                job_id
                for job_id, snapshot in self._jobs.items()
                if now - float(snapshot.get("updated_at", snapshot.get("created_at", now))) > self._retention_seconds
            ]
            for job_id in expired:
                self._jobs.pop(job_id, None)


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, *, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else default
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def _cli_bool(value: bool) -> str:
    return "true" if value else "false"


def _env_bool(value: bool) -> str:
    return "true" if value else "false"


def _write_mineru_config(output_dir: Path, options: ConversionOptions) -> Path:
    config: dict[str, Any] = {}
    configured_path = (os.getenv("MINERU_TOOLS_CONFIG_JSON") or "").strip()
    candidates = [Path(configured_path)] if configured_path else [Path.home() / "mineru.json"]
    for candidate in candidates:
        try:
            if candidate.exists():
                loaded = json.loads(candidate.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    config = loaded
                break
        except Exception:
            config = {}

    config["latex-delimiter-config"] = _latex_delimiter_config(options.latex_delimiters_type)
    path = output_dir / "mineru_config.json"
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _latex_delimiter_config(delimiter_type: str) -> dict[str, dict[str, str]]:
    if delimiter_type == "a":
        return {
            "display": {"left": "$$", "right": "$$"},
            "inline": {"left": "$", "right": "$"},
        }
    return {
        "display": {"left": "\\[", "right": "\\]"},
        "inline": {"left": "\\(", "right": "\\)"},
    }


def _split_command(raw_command: str) -> list[str]:
    parts = shlex.split(raw_command, posix=os.name != "nt")
    return [part.strip('"') for part in parts if part.strip('"')]


def _mineru_cli_from_python(python_exe: Path) -> Path | None:
    candidates = [
        python_exe.parent / "mineru.exe",
        python_exe.parent / "mineru",
        python_exe.parent / "Scripts" / "mineru.exe",
        python_exe.parent / "Scripts" / "mineru",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    path_candidate = shutil.which("mineru")
    if path_candidate:
        return Path(path_candidate)
    return None


def _version_tuple(version: str) -> tuple[int, int, int]:
    parts = [int(match) for match in re.findall(r"\d+", version)[:3]]
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts[:3])  # type: ignore[return-value]


def _compact_process_error(text: str, *, limit: int = 1200) -> str:
    compact = re.sub(r"\s+", " ", text or "").strip()
    if not compact:
        return "khong co stderr/stdout."
    return compact[:limit] + ("..." if len(compact) > limit else "")


def _as_pages(data: Any) -> list[list[dict[str, Any]]]:
    if not isinstance(data, list):
        return []
    if not data:
        return []
    if all(isinstance(item, list) for item in data):
        return [[entry for entry in page if isinstance(entry, dict)] for page in data]
    return [[entry for entry in data if isinstance(entry, dict)]]


def _first(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, "", []):
            return value
    return None


def _rich_text_to_plain(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return re.sub(r"\s+", " ", value).strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        pieces = [_rich_text_to_plain(item) for item in value]
        return re.sub(r"\s+", " ", " ".join(piece for piece in pieces if piece)).strip()
    if isinstance(value, dict):
        for key in (
            "content",
            "text",
            "paragraph_content",
            "title_content",
            "math_content",
            "table_content",
            "code_content",
            "code_body",
            "algorithm_content",
            "caption",
            "value",
        ):
            if key in value:
                text = _rich_text_to_plain(value.get(key))
                if text:
                    return text
        pieces = [_rich_text_to_plain(item) for item in value.values()]
        return re.sub(r"\s+", " ", " ".join(piece for piece in pieces if piece)).strip()
    return str(value).strip()


def _to_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [text for text in (_rich_text_to_plain(item) for item in value) if text]
    text = _rich_text_to_plain(value)
    return [text] if text else []


def _clamp_heading_level(value: Any) -> int:
    try:
        level = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, min(6, level))


def _block_has_content(block: NormalizedBlock) -> bool:
    return bool(block.text or block.items or block.table_html or block.image_path or block.caption)


def _append_text_with_math(paragraph: Any, text: str) -> None:
    for is_math, value, display in _split_math_segments(text):
        if not value:
            continue
        if is_math:
            _append_math(paragraph, value, display=display)
        else:
            paragraph.add_run(value)


def _append_math(paragraph: Any, latex: str, *, display: bool) -> None:
    latex = _strip_math_delimiters(latex)
    if not latex:
        return
    omml_xml = _latex_to_omml_xml(latex)
    if omml_xml:
        try:
            paragraph._p.append(parse_xml(_ensure_omml_namespaces(omml_xml)))
            return
        except Exception:
            pass
    paragraph._p.append(_fallback_omml(latex, display=display))


def _latex_to_omml_xml(latex: str) -> str | None:
    try:
        import latex2mathml.converter
        import mathml2omml
    except Exception:
        return None
    try:
        mathml = latex2mathml.converter.convert(latex)
        convert = getattr(mathml2omml, "convert", None)
        if convert is None:
            return None
        return str(convert(mathml, html.entities.name2codepoint))
    except Exception:
        return None


def _ensure_omml_namespaces(omml_xml: str) -> str:
    if "xmlns:m=" in omml_xml:
        return omml_xml
    for root in ("m:oMathPara", "m:oMath"):
        prefix = f"<{root}"
        if omml_xml.startswith(prefix):
            return omml_xml.replace(prefix, f"{prefix} {nsdecls('m')}", 1)
    return omml_xml


def _fallback_omml(latex: str, *, display: bool) -> Any:
    math = OxmlElement("m:oMath")
    run = OxmlElement("m:r")
    text = OxmlElement("m:t")
    text.text = latex
    run.append(text)
    math.append(run)
    if not display:
        return math

    math_para = OxmlElement("m:oMathPara")
    math_para.append(math)
    return math_para


def _split_math_segments(text: str) -> list[tuple[bool, str, bool]]:
    segments: list[tuple[bool, str, bool]] = []
    index = 0
    while index < len(text):
        start = _find_next_math_start(text, index)
        if start is None:
            segments.append((False, text[index:], False))
            break
        start_index, left, right, display = start
        if start_index > index:
            segments.append((False, text[index:start_index], False))
        end_index = _find_unescaped(text, right, start_index + len(left))
        if end_index is None:
            segments.append((False, text[start_index:], False))
            break
        latex = text[start_index + len(left) : end_index].strip()
        segments.append((True, latex, display))
        index = end_index + len(right)
    return segments


def _find_next_math_start(text: str, start_at: int) -> tuple[int, str, str, bool] | None:
    candidates: list[tuple[int, str, str, bool]] = []
    for left, right, display in (("$$", "$$", True), ("\\[", "\\]", True), ("\\(", "\\)", False), ("$", "$", False)):
        index = _find_unescaped(text, left, start_at)
        if index is not None:
            candidates.append((index, left, right, display))
    if not candidates:
        return None
    return min(candidates, key=lambda candidate: candidate[0])


def _find_unescaped(text: str, needle: str, start_at: int) -> int | None:
    index = text.find(needle, start_at)
    while index != -1:
        if not _is_escaped(text, index):
            return index
        index = text.find(needle, index + len(needle))
    return None


def _is_escaped(text: str, index: int) -> bool:
    slash_count = 0
    cursor = index - 1
    while cursor >= 0 and text[cursor] == "\\":
        slash_count += 1
        cursor -= 1
    return slash_count % 2 == 1


def _is_display_math_line(text: str) -> bool:
    return (
        (text.startswith("$$") and text.endswith("$$") and len(text) > 4)
        or (text.startswith("\\[") and text.endswith("\\]") and len(text) > 4)
    )


def _strip_math_delimiters(text: str) -> str:
    stripped = text.strip()
    for left, right in (("$$", "$$"), ("\\[", "\\]"), ("\\(", "\\)"), ("$", "$")):
        if stripped.startswith(left) and stripped.endswith(right) and len(stripped) > len(left) + len(right):
            return stripped[len(left) : -len(right)].strip()
    return stripped


def _html_table_to_matrix(html: str) -> list[list[str]]:
    if not html or "<table" not in html.lower():
        return []
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        return []
    matrix: list[list[str]] = []
    for tr in table.find_all("tr"):
        row: list[str] = []
        for cell in tr.find_all(["td", "th"], recursive=False):
            text = cell.get_text(" ", strip=True)
            try:
                colspan = max(1, int(cell.get("colspan") or 1))
            except ValueError:
                colspan = 1
            row.extend([text] * colspan)
        if row:
            matrix.append(row)
    return matrix


def _markdown_table_to_html(lines: list[str]) -> str:
    rows: list[list[str]] = []
    for line in lines:
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if cells and all(re.fullmatch(r":?-{3,}:?", cell) for cell in cells):
            continue
        if cells:
            rows.append(cells)
    if not rows:
        return ""
    html_rows = []
    for row in rows:
        cells = "".join(f"<td>{_escape_html(cell)}</td>" for cell in row)
        html_rows.append(f"<tr>{cells}</tr>")
    return f"<table>{''.join(html_rows)}</table>"


def _escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _resolve_artifact_path(raw_path: str, base_dirs: list[Path]) -> Path | None:
    if not raw_path:
        return None
    path = Path(raw_path)
    if path.is_absolute():
        return path
    normalized = raw_path.replace("\\", "/").lstrip("/")
    for base_dir in base_dirs:
        candidate = base_dir / normalized
        if candidate.exists():
            return candidate
        matches = list(base_dir.rglob(Path(normalized).name))
        if matches:
            return matches[0]
    return None


def _latest_file(root: Path, pattern: str) -> Path | None:
    files = [path for path in root.rglob(pattern) if path.is_file()]
    if not files:
        return None
    return max(files, key=lambda path: path.stat().st_mtime)


def _artifact_kind(path: Path) -> str:
    name = path.name.lower()
    if name.endswith(".docx"):
        return "docx"
    if name.endswith(".md"):
        return "markdown"
    if name.endswith("_layout.pdf"):
        return "layout"
    if name.endswith(".json"):
        return "json"
    if name.endswith(".log"):
        return "log"
    return "artifact"


def _artifact_label(path: Path) -> str:
    kind = _artifact_kind(path)
    labels = {
        "docx": "Word DOCX",
        "markdown": "Markdown",
        "layout": "Layout PDF",
        "json": "Structured JSON",
        "log": "MinerU log",
    }
    return labels.get(kind, path.name)


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False
