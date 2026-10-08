from __future__ import annotations

from hashlib import sha256
from io import BytesIO
import json
import math
import os
from pathlib import Path
import re
from typing import Literal

from .config import ApiSettings
from .knowledge_models import KnowledgeChunkInput, ParsedDocument, ParsedSection


MAX_FILE_BYTES = 10 * 1024 * 1024
MAX_PDF_PAGES = 200
MODEL_NAME = "BAAI/bge-small-zh-v1.5"
EMBEDDING_DIM = 512
_HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*#*\s*$")
_CJK = re.compile(r"[\u4e00-\u9fff]+")
_TECH_WORD = re.compile(r"[A-Za-z][A-Za-z0-9_]*(?:[-.][A-Za-z0-9_]+)*")


class EmbeddingUnavailable(RuntimeError):
    pass


def _paragraphs(filename: str, text: str, markdown: bool) -> tuple[ParsedSection, ...]:
    heading = Path(filename).stem
    sections: list[ParsedSection] = []
    paragraph: list[str] = []

    def flush() -> None:
        if paragraph:
            body = "\n".join(paragraph).strip()
            if body:
                sections.append(ParsedSection(f"{heading} / 段落 {len(sections) + 1}", None, body))
            paragraph.clear()

    for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        match = _HEADING.match(line) if markdown else None
        if match:
            flush()
            heading = match.group(1).strip()
        elif not line.strip():
            flush()
        else:
            paragraph.append(line.rstrip())
    flush()
    return tuple(sections)


def parse_document(filename: str, data: bytes) -> ParsedDocument:
    if not data or len(data) > MAX_FILE_BYTES:
        raise ValueError("document must be nonempty and no larger than 10 MiB")
    extension = Path(filename).suffix.lower()
    if extension not in {".md", ".txt", ".pdf"}:
        raise ValueError("only MD, TXT and text-layer PDF files are supported")
    if extension == ".pdf":
        if not data.startswith(b"%PDF-"):
            raise ValueError("invalid PDF signature")
        try:
            from pypdf import PdfReader

            reader = PdfReader(BytesIO(data), strict=True)
            if reader.is_encrypted:
                raise ValueError("encrypted PDF is not supported")
            if len(reader.pages) > MAX_PDF_PAGES:
                raise ValueError("PDF exceeds 200 pages")
            sections = tuple(
                ParsedSection(f"第 {number} 页", number, extracted.strip())
                for number, page in enumerate(reader.pages, 1)
                if (extracted := (page.extract_text() or "")).strip()
            )
        except ValueError:
            raise
        except Exception as exc:
            raise ValueError("invalid or unreadable PDF") from exc
        if not sections:
            raise ValueError("PDF has no text layer")
    else:
        if data.startswith(b"%PDF-") or b"\x00" in data:
            raise ValueError("PDF or binary content disguised as text")
        try:
            decoded = data.decode("utf-8-sig")
        except UnicodeError as exc:
            raise ValueError("text documents must be UTF-8") from exc
        sections = _paragraphs(filename, decoded, extension == ".md")
        if not sections:
            raise ValueError("document has no extractable text")
    return ParsedDocument(filename, data, sections)


def _terms(text: str) -> tuple[str, ...]:
    words = {word.lower() for word in _TECH_WORD.findall(text)}
    for run in _CJK.findall(text):
        words.update(run[index:index + 2] for index in range(len(run) - 1))
        if len(run) == 1:
            words.add(run)
    return tuple(sorted(words))


def chunk_document(parsed: ParsedDocument) -> list[KnowledgeChunkInput]:
    chunks: list[KnowledgeChunkInput] = []
    for section in parsed.sections:
        text = section.text.strip()
        start = 0
        while start < len(text):
            end = min(start + 400, len(text))
            body = text[start:end]
            chunks.append(KnowledgeChunkInput(
                ordinal=len(chunks), section=section.section, page=section.page,
                text=body, sha256=sha256(body.encode("utf-8")).hexdigest(),
                terms=_terms(body),
            ))
            if end == len(text):
                break
            start = end - 40
    return chunks


def metric_definition_sections(
    domain: Literal["behavior", "orders"], settings: ApiSettings,
) -> ParsedDocument:
    if domain == "behavior":
        source = settings.behavior_metric_definitions_path
    elif domain == "orders":
        source = settings.order_metric_definitions_path
    else:
        raise ValueError("unsupported metric domain")
    original = source.read_bytes()
    definition = json.loads(original)
    if definition.get("domain") != domain or not isinstance(definition.get("definitions"), list):
        raise ValueError("metric definition domain or format is invalid")
    version = definition.get("metric_version", domain)
    sections = tuple(
        ParsedSection(f"{version} / {item['metric_name']}", None,
                      json.dumps(item, ensure_ascii=False, sort_keys=True))
        for item in definition["definitions"]
    )
    if not sections:
        raise ValueError("metric definition file has no metrics")
    return ParsedDocument(source.name, original, sections)


class Embedder:
    def __init__(self, *, cache_dir: Path | None = None, allow_download: bool = False, model=None):
        default = os.environ.get("G4_EMBED_CACHE_DIR")
        if cache_dir is None:
            if default:
                cache_dir = Path(default)
            elif os.name == "nt":
                cache_dir = Path("D:/EcommerceDev/cache/fastembed")
            else:
                raise ValueError("G4_EMBED_CACHE_DIR must point to a D-drive-backed mount")
        self.cache_dir = Path(cache_dir)
        if not self.cache_dir.is_absolute():
            raise ValueError("embedding cache path must be absolute")
        if os.name == "nt" and self.cache_dir.drive.upper() != "D:":
            raise ValueError("embedding cache must be on D:")
        self.allow_download = allow_download
        self._model = model

    def embed_many(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        if self._model is None:
            try:
                from fastembed import TextEmbedding

                self._model = TextEmbedding(
                    model_name=MODEL_NAME, cache_dir=str(self.cache_dir),
                    threads=2, local_files_only=not self.allow_download,
                )
            except (ImportError, OSError, ValueError) as exc:
                raise EmbeddingUnavailable("embedding model is not prepared") from exc
        vectors = []
        for vector in self._model.embed(texts):
            values = [float(value) for value in vector]
            if len(values) != EMBEDDING_DIM or not all(math.isfinite(value) for value in values):
                raise ValueError("embedding must have 512 finite values")
            vectors.append(values)
        if len(vectors) != len(texts):
            raise ValueError("embedding count does not match text count")
        return vectors


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--prepare-model", action="store_true")
    options = parser.parse_args()
    if not options.prepare_model:
        parser.error("pass --prepare-model to explicitly download the embedding model")
    embedder = Embedder(allow_download=True)
    embedder.embed_many(["电商指标口径"])
    print(f"Prepared {MODEL_NAME} in {embedder.cache_dir}")
