from __future__ import annotations

import logging
import mimetypes
import os
from pathlib import Path

from flask import Flask, abort, jsonify, render_template, request, send_file, url_for
from werkzeug.exceptions import HTTPException, RequestEntityTooLarge

try:
    from webapp.pdf_service import (
        ALLOWED_BACKENDS,
        ALLOWED_LANGUAGES,
        ALLOWED_LATEX_DELIMITER_TYPES,
        ALLOWED_PARSE_METHODS,
        ConversionJobManager,
        ConversionOptions,
        PDFConversionService,
    )
except ImportError:  # pragma: no cover
    from pdf_service import (
        ALLOWED_BACKENDS,
        ALLOWED_LANGUAGES,
        ALLOWED_LATEX_DELIMITER_TYPES,
        ALLOWED_PARSE_METHODS,
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
    }


def _conversion_options_from_request() -> ConversionOptions:
    form = request.form
    backend = _choice("backend", ALLOWED_BACKENDS, "auto")
    parse_method = _choice("parse_method", ALLOWED_PARSE_METHODS, "auto")
    language = _choice("language", ALLOWED_LANGUAGES, "ch")
    latex_delimiters_type = _choice("latex_delimiters_type", ALLOWED_LATEX_DELIMITER_TYPES, "b")
    start_page_ui = _optional_int(form.get("start_page"), default=1, minimum=1, maximum=99999)
    end_page_ui = _optional_int(form.get("end_page"), default=None, minimum=1, maximum=99999)
    start_page = max(0, start_page_ui - 1)
    end_page = end_page_ui - 1 if end_page_ui is not None else None
    if end_page is not None and end_page < start_page:
        raise ValueError("Trang ket thuc phai lon hon hoac bang trang bat dau.")
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
    )


def _choice(name: str, allowed: set[str], default: str) -> str:
    value = (request.form.get(name) or default).strip()
    return value if value in allowed else default


def _form_bool(name: str, *, default: bool) -> bool:
    raw = request.form.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


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
        artifacts.append(item)
    payload["artifacts"] = artifacts
    docx = next((artifact for artifact in artifacts if artifact.get("kind") == "docx"), None)
    payload["docx_url"] = docx["download_url"] if docx else ""
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
                "message": snapshot["message"],
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
                "message": snapshot["message"],
                "result": result,
            }
        )

    return jsonify(
        {
            "ok": False,
            "done": True,
            "job_id": snapshot["id"],
            "status": status,
            "error": snapshot["error"] or snapshot["message"],
            "message": snapshot["message"],
        }
    )


@app.route("/downloads/<job_id>/<path:filename>")
def download_file(job_id: str, filename: str):
    try:
        path = converter.resolve_download(job_id, filename)
    except FileNotFoundError:
        abort(404, description="Khong tim thay file tai xuong.")
    guessed_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    return send_file(path, mimetype=guessed_type, as_attachment=True, download_name=path.name)


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
