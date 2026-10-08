import hashlib
from io import BytesIO
import json
import math
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from pypdf import PdfWriter


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "api"))

from app.config import ApiSettings  # noqa: E402
from app.knowledge_ingest import (  # noqa: E402
    Embedder, EmbeddingUnavailable, chunk_document, metric_definition_sections, parse_document,
)
from app.knowledge_models import DocumentMetadata  # noqa: E402


def text_pdf() -> bytes:
    stream = b"BT /F1 12 Tf 20 200 Td (Order metric guide) Tj ET\n"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Count 1 /Kids [3 0 R] >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 300] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
        f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"endstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    data = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for index, obj in enumerate(objects, 1):
        offsets.append(len(data))
        data.extend(f"{index} 0 obj\n".encode())
        data.extend(obj + b"\nendobj\n")
    xref = len(data)
    data.extend(f"xref\n0 {len(offsets)}\n0000000000 65535 f \n".encode())
    for offset in offsets[1:]:
        data.extend(f"{offset:010d} 00000 n \n".encode())
    data.extend(f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF".encode())
    return bytes(data)


class G4KnowledgeIngestTest(unittest.TestCase):
    def test_markdown_and_text_keep_section_and_paragraph(self):
        markdown = parse_document("guide.md", "# 口径说明\n\n第一段。\n\n第二段。\n\n## 质量\n\n无效行。".encode())
        self.assertEqual(len(markdown.sections), 3)
        self.assertIn("口径说明", markdown.sections[0].section)
        self.assertIn("段落 2", markdown.sections[1].section)
        self.assertIn("质量", markdown.sections[2].section)
        plain = parse_document("notes.txt", "标题\n\n正文一。\n\n正文二。".encode())
        self.assertEqual(len(plain.sections), 3)
        self.assertIn("段落 3", plain.sections[-1].section)

    def test_pdf_keeps_page_and_rejects_non_text_pdf(self):
        parsed = parse_document("guide.pdf", text_pdf())
        self.assertEqual(parsed.sections[0].page, 1)
        self.assertIn("Order metric guide", parsed.sections[0].text)
        writer = PdfWriter()
        writer.add_blank_page(width=100, height=100)
        stream = BytesIO()
        writer.write(stream)
        with self.assertRaisesRegex(ValueError, "text layer"):
            parse_document("scan.pdf", stream.getvalue())
        writer.encrypt("secret")
        stream = BytesIO()
        writer.write(stream)
        with self.assertRaisesRegex(ValueError, "encrypted"):
            parse_document("locked.pdf", stream.getvalue())

    def test_rejects_oversize_too_many_pages_and_spoofed_pdf(self):
        with self.assertRaisesRegex(ValueError, "10 MiB"):
            parse_document("huge.txt", b"a" * (10 * 1024 * 1024 + 1))
        with self.assertRaisesRegex(ValueError, "PDF"):
            parse_document("fake.pdf", b"plain text")
        with self.assertRaisesRegex(ValueError, "PDF"):
            parse_document("fake.txt", text_pdf())
        writer = PdfWriter()
        for _ in range(201):
            writer.add_blank_page(width=100, height=100)
        stream = BytesIO()
        writer.write(stream)
        with self.assertRaisesRegex(ValueError, "200"):
            parse_document("long.pdf", stream.getvalue())

    def test_chunks_are_bounded_and_keep_chinese_and_identifier_terms(self):
        parsed = parse_document("metrics.txt", ("口径说明 metric_run_id " * 60).encode())
        chunks = chunk_document(parsed)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(0 < len(chunk.text) <= 400 for chunk in chunks))
        self.assertTrue(all(chunk.sha256 == hashlib.sha256(chunk.text.encode()).hexdigest() for chunk in chunks))
        self.assertIn("口径", chunks[0].terms)
        self.assertIn("metric_run_id", chunks[0].terms)
        for previous, current in zip(chunks, chunks[1:]):
            overlap = max((n for n in range(1, 51) if previous.text[-n:] == current.text[:n]), default=0)
            self.assertLessEqual(overlap, 50)

    def test_metric_json_is_split_by_real_definition_and_sources_are_explicit(self):
        settings = ApiSettings()
        for domain in ("behavior", "orders"):
            parsed = metric_definition_sections(domain, settings)
            source = settings.behavior_metric_definitions_path if domain == "behavior" else settings.order_metric_definitions_path
            definitions = json.loads(source.read_text(encoding="utf-8"))["definitions"]
            self.assertEqual(len(parsed.sections), len(definitions))
            self.assertEqual(parsed.original_bytes, source.read_bytes())
            self.assertIn(definitions[0]["metric_name"], parsed.sections[0].text)
        whitelist = json.loads((ROOT / "configs/knowledge/g4_sources.json").read_text(encoding="utf-8"))
        self.assertEqual({item["path"] for item in whitelist["sources"]}, {
            "configs/metrics/behavior-v1.json", "configs/metrics/orders-v1.json",
            "docs/graduation/olist-order-domain-runbook.md",
            "docs/graduation/behavior-metrics-api-runbook.md",
            "docs/graduation/real-event-quality-runbook.md",
        })
        self.assertTrue(all(item["label"] == "项目文档" for item in whitelist["sources"]))

    def test_metadata_rejects_unsafe_source_and_role(self):
        base = dict(title="订单口径", category="指标", source_type="project_doc",
                    source_ref="configs/metrics/orders-v1.json", visibility_roles=("admin",))
        DocumentMetadata(**base)
        with self.assertRaises(ValueError):
            DocumentMetadata(**{**base, "source_ref": "javascript:alert(1)"})
        with self.assertRaises(ValueError):
            DocumentMetadata(**{**base, "visibility_roles": ("superuser",)})

    def test_embedder_rejects_wrong_dimensions_and_nan(self):
        class FakeModel:
            def __init__(self, vector):
                self.vector = vector

            def embed(self, texts):
                return (self.vector for _ in texts)

        for vector in ([0.0] * 511, [math.nan] + [0.0] * 511):
            with self.subTest(vector_length=len(vector)):
                with self.assertRaisesRegex(ValueError, "512|finite"):
                    Embedder(model=FakeModel(vector)).embed_many(["test"])
        good = Embedder(model=FakeModel([0.0] * 512)).embed_many(["x", "y"])
        self.assertEqual(len(good), 2)

    def test_model_missing_does_not_download_during_normal_use(self):
        def fake_constructor(**kwargs):
            self.assertTrue(kwargs["local_files_only"])
            self.assertEqual(kwargs["model_name"], "BAAI/bge-small-zh-v1.5")
            raise FileNotFoundError("cache empty")

        cache = Path("D:/EcommerceDev/cache/fastembed") if os.name == "nt" else Path("/tmp/g4-fastembed")
        with patch.dict(sys.modules, {"fastembed": SimpleNamespace(TextEmbedding=fake_constructor)}):
            with self.assertRaisesRegex(EmbeddingUnavailable, "not prepared"):
                Embedder(cache_dir=cache).embed_many(["test"])

    def test_missing_fastembed_package_is_explicitly_unavailable(self):
        cache = Path("D:/EcommerceDev/cache/fastembed") if os.name == "nt" else Path("/tmp/g4-fastembed")
        with patch.dict(sys.modules, {"fastembed": None}):
            with self.assertRaises(EmbeddingUnavailable):
                Embedder(cache_dir=cache).embed_many(["test"])


if __name__ == "__main__":
    unittest.main()
