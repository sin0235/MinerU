from __future__ import annotations

import logging
import mimetypes
import os
import zipfile
from pathlib import Path

from flask import Flask, abort, jsonify, render_template, request, send_file, url_for
from docx import Document
from docx.table import Table
from docx.text.paragraph import Paragraph
from markupsafe import escape
from werkzeug.exceptions import HTTPException, RequestEntityTooLarge

try:
    from webapp.pdf_service import (
        ALLOWED_BACKENDS,
        ALLOWED_LANGUAGES,
        ALLOWED_LATEX_DELIMITER_TYPES,
        ALLOWED_LLM_MODES,
        ALLOWED_PARSE_METHODS,
        DEFAULT_NVIDIA_LLM_MODEL,
        LLM_MODEL_OPTIONS,
        ConversionJobManager,
        ConversionOptions,
        PDFConversionService,
    )
except ImportError:  # pragma: no cover
    from pdf_service import (
        ALLOWED_BACKENDS,
        ALLOWED_LANGUAGES,
        ALLOWED_LATEX_DELIMITER_TYPES,
        ALLOWED_LLM_MODES,
        ALLOWED_PARSE_METHODS,
        DEFAULT_NVIDIA_LLM_MODEL,
        LLM_MODEL_OPTIONS,
        ConversionJobManager,
        ConversionOptions,
        PDFConversionService,
    )


ROOT = Path(__file__).resolve().parent.parent
CSS_PATH = ROOT / "webapp" / "static" / "css" / "style.css"
CSS_BUNDLE_VERSION = int(CSS_PATH.stat().st_mtime) if CSS_PATH.exists() else 1

app = Flask(__name__, static_folder="static", template_folder="templates")
app.logger.setLevel(logging.INFO)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

converter = PDFConversionService(ROOT)
app.config["MAX_CONTENT_LENGTH"] = converter.max_upload_bytes
job_manager = ConversionJobManager(converter)


def _base_context(active: str = "converter") -> dict:
    return {
        "active": active,
        "static_version": int(CSS_PATH.stat().st_mtime) if CSS_PATH.exists() else CSS_BUNDLE_VERSION,
        "max_upload_mb": converter.max_upload_mb,
        "readiness": converter.readiness().to_payload(),
        "recent_results": [snapshot["result"] for snapshot in job_manager.recent_results()],
        "converter_options": _converter_options_payload(),
    }


def _is_api_request() -> bool:
    return request.path.startswith("/api/")


def _converter_options_payload() -> dict:
    return {
        "default": ConversionOptions(backend=converter.resolve_backend()).to_payload(),
        "backends": ["auto", "pipeline", "hybrid-auto-engine", "vlm-auto-engine", "hybrid-http-client", "vlm-http-client"],
        "parse_methods": ["auto", "ocr", "txt"],
        "languages": [
            ("ch", "Chinese + English"),
            ("en", "English"),
            ("latin", "Latin/Vietnamese"),
            ("ch_lite", "Chinese Lite"),
            ("ch_server", "Chinese Server"),
            ("korean", "Korean"),
            ("japan", "Japanese"),
            ("chinese_cht", "Traditional Chinese"),
            ("arabic", "Arabic"),
            ("cyrillic", "Cyrillic"),
            ("east_slavic", "East Slavic"),
            ("devanagari", "Devanagari"),
            ("ta", "Tamil"),
            ("te", "Telugu"),
            ("ka", "Kannada"),
            ("th", "Thai"),
            ("el", "Greek"),
        ],
        "latex_delimiters": [
            ("b", r"\(...\) / \[...\]"),
            ("a", "$...$ / $$...$$"),
            ("all", "All in DOCX parser"),
        ],
        "llm_modes": [
            ("off", "Tắt"),
            ("review", "Chỉ kiểm tra"),
            ("correct", "Tự sửa lỗi rõ ràng"),
        ],
        "llm_api_configured": bool((os.getenv("NVIDIA_API_KEY") or os.getenv("OPENROUTER_API_KEY") or "").strip()),
        "default_llm_model": DEFAULT_NVIDIA_LLM_MODEL,
        "llm_models": LLM_MODEL_OPTIONS,
    }


def _conversion_options_from_request() -> ConversionOptions:
    form = request.form
    backend = _choice("backend", ALLOWED_BACKENDS, "auto")
    parse_method = _choice("parse_method", ALLOWED_PARSE_METHODS, "auto")
    language = _choice("language", ALLOWED_LANGUAGES, "ch")
    latex_delimiters_type = _choice("latex_delimiters_type", ALLOWED_LATEX_DELIMITER_TYPES, "b")
    llm_mode = _choice("llm_mode", ALLOWED_LLM_MODES, "off")
    llm_model = (form.get("llm_model") or DEFAULT_NVIDIA_LLM_MODEL).strip() or DEFAULT_NVIDIA_LLM_MODEL
    start_page_ui = _optional_int(form.get("start_page"), default=1, minimum=1, maximum=99999)
    end_page_ui = _optional_int(form.get("end_page"), default=None, minimum=1, maximum=99999)
    start_page = max(0, start_page_ui - 1)
    end_page = end_page_ui - 1 if end_page_ui is not None else None
    if end_page is not None and end_page < start_page:
        raise ValueError("Trang ket thuc phai lon hon hoac bang trang bat dau.")
    if llm_mode != "off" and not _llm_api_key_configured(llm_model):
        missing_key = "OPENROUTER_API_KEY" if llm_model.lower().startswith("openrouter/") else "NVIDIA_API_KEY"
        raise ValueError(f"Chua cau hinh {missing_key} nen khong the bat LLM review.")
    return ConversionOptions(
        backend=backend,
        parse_method=parse_method,
        language=language,
        formula_enable=_form_bool("formula_enable", default=True),
        table_enable=_form_bool("table_enable", default=True),
        start_page=start_page,
        end_page=end_page,
        server_url=(form.get("server_url") or "").strip(),
        latex_delimiters_type=latex_delimiters_type,
        exam_format=_form_bool("exam_format", default=False),
        llm_mode=llm_mode,
        llm_model=llm_model,
        llm_reasoning=_form_bool("llm_reasoning", default=False),
    )


def _choice(name: str, allowed: set[str], default: str) -> str:
    value = (request.form.get(name) or default).strip()
    return value if value in allowed else default


def _form_bool(name: str, *, default: bool) -> bool:
    raw = request.form.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _llm_api_key_configured(model: str) -> bool:
    if model.strip().lower().startswith("openrouter/"):
        return bool((os.getenv("OPENROUTER_API_KEY") or "").strip())
    return bool((os.getenv("NVIDIA_API_KEY") or "").strip())


def _optional_int(raw: str | None, *, default: int | None, minimum: int, maximum: int) -> int | None:
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError("Gia tri trang phai la so nguyen.") from exc
    return max(minimum, min(maximum, value))


def _artifact_payload_with_urls(job_id: str, result: dict) -> dict:
    payload = dict(result)
    artifacts = []
    for artifact in payload.get("artifacts", []):
        item = dict(artifact)
        item["download_url"] = url_for(
            "download_file",
            job_id=job_id,
            filename=item["relative_path"],
        )
        suffix = Path(item.get("filename") or "").suffix.lower()
        if suffix == ".docx":
            item["preview_url"] = url_for("preview_docx", job_id=job_id, filename=item["relative_path"])
            item["preview_kind"] = "docx"
        elif suffix == ".pdf":
            item["preview_url"] = url_for("preview_file_inline", job_id=job_id, filename=item["relative_path"])
            item["preview_kind"] = "pdf"
        artifacts.append(item)
    payload["artifacts"] = artifacts
    docx = next((artifact for artifact in artifacts if artifact.get("kind") == "docx"), None)
    payload["docx_url"] = docx["download_url"] if docx else ""
    payload["artifacts_zip_url"] = url_for("download_artifacts_zip", job_id=job_id)
    return payload


@app.errorhandler(RequestEntityTooLarge)
def handle_request_entity_too_large(exc: RequestEntityTooLarge):
    if not _is_api_request():
        return exc
    return jsonify({"ok": False, "error": f"File PDF qua lon. Gioi han la {converter.max_upload_mb} MB."}), 413


@app.errorhandler(HTTPException)
def handle_api_http_exception(exc: HTTPException):
    if not _is_api_request():
        return exc
    return jsonify({"ok": False, "error": exc.description or exc.name}), exc.code or 500


@app.route("/")
def home():
    return render_template("pdf_to_word.html", **_base_context())


@app.route("/api/status")
def api_status():
    readiness = converter.readiness().to_payload()
    return jsonify(
        {
            "ok": True,
            "readiness": readiness,
            "max_upload_mb": converter.max_upload_mb,
            "recent_results": [
                _artifact_payload_with_urls(snapshot["id"], snapshot["result"])
                for snapshot in job_manager.recent_results()
            ],
        }
    )


@app.route("/api/convert", methods=["POST"])
def api_convert():
    upload = request.files.get("pdf")
    try:
        options = _conversion_options_from_request()
        submission = converter.create_submission_with_options(upload, options)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        app.logger.exception("Failed to save upload")
        return jsonify({"ok": False, "error": f"Khong the luu file upload: {exc}"}), 500

    job_id = job_manager.enqueue(submission)
    return (
        jsonify(
            {
                "ok": True,
                "queued": True,
                "job_id": job_id,
                "status": "queued",
                "message": "Da tao job chuyen doi PDF sang Word.",
            }
        ),
        202,
    )


@app.route("/api/jobs/<job_id>")
def api_job_status(job_id: str):
    snapshot = job_manager.get_snapshot(job_id.strip())
    if snapshot is None:
        return jsonify({"ok": False, "error": "Khong tim thay job hoac job da het han."}), 404

    status = snapshot["status"]
    if status in {"queued", "running"}:
        return jsonify(
            {
                "ok": True,
                "done": False,
                "job_id": snapshot["id"],
                "status": status,
                "stage": snapshot.get("stage") or status,
                "progress": snapshot.get("progress") or (3 if status == "queued" else 10),
                "terminal_lines": snapshot.get("terminal_lines") or [],
                "message": snapshot["message"],
                "elapsed_seconds": snapshot.get("elapsed_seconds"),
                "eta_seconds": snapshot.get("eta_seconds"),
                "updated_age_seconds": snapshot.get("updated_age_seconds"),
                "original_filename": snapshot["original_filename"],
            }
        )

    if status == "completed":
        result = _artifact_payload_with_urls(snapshot["id"], snapshot["result"])
        return jsonify(
            {
                "ok": True,
                "done": True,
                "job_id": snapshot["id"],
                "status": status,
                "stage": snapshot.get("stage") or status,
                "progress": snapshot.get("progress") or 100,
                "terminal_lines": snapshot.get("terminal_lines") or [],
                "message": snapshot["message"],
                "elapsed_seconds": snapshot.get("elapsed_seconds"),
                "eta_seconds": snapshot.get("eta_seconds"),
                "updated_age_seconds": snapshot.get("updated_age_seconds"),
                "result": result,
            }
        )

    return jsonify(
        {
            "ok": False,
            "done": True,
            "job_id": snapshot["id"],
            "status": status,
            "stage": snapshot.get("stage") or status,
            "progress": snapshot.get("progress") or 0,
            "terminal_lines": snapshot.get("terminal_lines") or [],
            "error": snapshot["error"] or snapshot["message"],
            "message": snapshot["message"],
            "elapsed_seconds": snapshot.get("elapsed_seconds"),
            "eta_seconds": snapshot.get("eta_seconds"),
            "updated_age_seconds": snapshot.get("updated_age_seconds"),
        }
    )


@app.route("/downloads/<job_id>/<path:filename>")
def download_file(job_id: str, filename: str):
    if filename == "artifacts.zip":
        return download_artifacts_zip(job_id)
    try:
        path = converter.resolve_download(job_id, filename)
    except FileNotFoundError:
        abort(404, description="Khong tim thay file tai xuong.")
    guessed_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    return send_file(path, mimetype=guessed_type, as_attachment=True, download_name=path.name)


@app.route("/previews/<job_id>/<path:filename>")
def preview_file_inline(job_id: str, filename: str):
    try:
        path = converter.resolve_download(job_id, filename)
    except FileNotFoundError:
        abort(404, description="Khong tim thay file preview.")
    if path.suffix.lower() != ".pdf":
        abort(400, description="Chi preview inline PDF qua route nay.")
    guessed_type = mimetypes.guess_type(path.name)[0] or "application/pdf"
    return send_file(path, mimetype=guessed_type, as_attachment=False, download_name=path.name)


@app.route("/api/previews/<job_id>/<path:filename>")
def preview_docx(job_id: str, filename: str):
    try:
        path = converter.resolve_download(job_id, filename)
    except FileNotFoundError:
        abort(404, description="Khong tim thay file preview.")
    if path.suffix.lower() != ".docx":
        abort(400, description="Chi ho tro preview DOCX.")
    return jsonify({"ok": True, "filename": path.name, "html": _docx_preview_html(path)})


@app.route("/downloads/<job_id>/artifacts.zip")
def download_artifacts_zip(job_id: str):
    snapshot = job_manager.get_snapshot(job_id.strip())
    if snapshot is None or snapshot.get("status") != "completed":
        abort(404, description="Khong tim thay job da hoan thanh.")
    job_dir = converter.job_dir(job_id.strip()).resolve()
    if not job_dir.exists():
        abort(404, description="Khong tim thay thu muc artifact.")

    zip_path = job_dir / "artifacts_without_docx.zip"
    _write_job_artifacts_zip(job_dir, zip_path)

    return send_file(
        zip_path,
        mimetype="application/zip",
        as_attachment=True,
        download_name="artifacts_without_docx.zip",
    )


def _write_job_artifacts_zip(job_dir: Path, zip_path: Path) -> None:
    job_dir = job_dir.resolve()
    zip_path = zip_path.resolve()
    archive_root = Path("runtime") / "jobs" / job_dir.name

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in job_dir.rglob("*"):
            resolved = path.resolve()
            if not path.is_file() or resolved == zip_path or path.suffix.lower() == ".docx":
                continue
            if not _is_relative_to(resolved, job_dir):
                continue
            relative_path = resolved.relative_to(job_dir)
            archive.write(resolved, (archive_root / relative_path).as_posix())


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _docx_preview_html(path: Path, *, max_blocks: int = 220) -> str:
    document = Document(path)
    pieces: list[str] = []
    block_count = 0
    for block in _iter_docx_blocks(document):
        if block_count >= max_blocks:
            pieces.append('<p class="doc-preview-note">Preview da rut gon de giu giao dien nhe.</p>')
            break
        if isinstance(block, Paragraph):
            text = block.text.strip()
            if not text:
                continue
            style_name = (block.style.name if block.style is not None else "").lower()
            tag = "h3" if "heading" in style_name or "title" in style_name else "p"
            pieces.append(f"<{tag}>{escape(text)}</{tag}>")
        elif isinstance(block, Table):
            rows: list[str] = []
            for row in block.rows[:40]:
                cells = "".join(f"<td>{escape(cell.text.strip())}</td>" for cell in row.cells[:12])
                rows.append(f"<tr>{cells}</tr>")
            if rows:
                pieces.append(f"<table><tbody>{''.join(rows)}</tbody></table>")
        block_count += 1
    if not pieces:
        return '<p class="doc-preview-note">DOCX khong co van ban preview duoc.</p>'
    return "".join(pieces)


def _iter_docx_blocks(document: Document):
    for child in document.element.body.iterchildren():
        if child.tag.endswith("}p"):
            yield Paragraph(child, document)
        elif child.tag.endswith("}tbl"):
            yield Table(child, document)


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    host = (os.getenv("PDF_WORD_WEBAPP_HOST") or "0.0.0.0").strip() or "0.0.0.0"
    port_raw = (os.getenv("PDF_WORD_WEBAPP_PORT") or "8386").strip()
    try:
        port = int(port_raw)
    except ValueError:
        app.logger.warning("Invalid PDF_WORD_WEBAPP_PORT=%r, fallback to 8386.", port_raw)
        port = 8386

    debug = _env_flag("PDF_WORD_WEBAPP_DEBUG", default=False)
    use_reloader = _env_flag("PDF_WORD_WEBAPP_RELOADER", default=False)
    app.run(debug=debug, host=host, port=port, use_reloader=use_reloader)
