from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID


Role = Literal["admin", "analyst", "viewer"]
SourceType = Literal["project_doc", "external"]
_ROLES = {"admin", "analyst", "viewer"}


@dataclass(frozen=True)
class DocumentMetadata:
    title: str
    category: str
    source_type: SourceType
    source_ref: str
    visibility_roles: tuple[Role, ...]
    document_id: UUID | None = None

    def __post_init__(self) -> None:
        if not self.title.strip() or not self.category.strip():
            raise ValueError("title and category are required")
        roles = tuple(self.visibility_roles)
        if not roles or len(roles) != len(set(roles)) or set(roles) - _ROLES:
            raise ValueError("visibility_roles must be distinct known roles")
        object.__setattr__(self, "visibility_roles", roles)
        if self.source_type == "project_doc":
            path = PurePosixPath(self.source_ref)
            if (not self.source_ref or path.is_absolute() or ".." in path.parts
                    or ":" in self.source_ref or "\\" in self.source_ref):
                raise ValueError("project source_ref must be a safe relative path")
        elif self.source_type == "external":
            url = urlsplit(self.source_ref)
            if url.scheme != "https" or not url.hostname or url.username or url.password:
                raise ValueError("external source_ref must be an HTTPS URL")
        else:
            raise ValueError("unsupported source_type")


@dataclass(frozen=True)
class ParsedSection:
    section: str
    page: int | None
    text: str


@dataclass(frozen=True)
class ParsedDocument:
    filename: str
    original_bytes: bytes
    sections: tuple[ParsedSection, ...]

    @property
    def extracted_text(self) -> str:
        return "\n\n".join(section.text for section in self.sections)


@dataclass(frozen=True)
class KnowledgeChunkInput:
    ordinal: int
    section: str
    page: int | None
    text: str
    sha256: str
    terms: tuple[str, ...]


@dataclass(frozen=True)
class EmbeddedChunk(KnowledgeChunkInput):
    embedding: tuple[float, ...]


@dataclass(frozen=True)
class DocumentVersion:
    id: UUID
    document_id: UUID
    version_no: int
    status: str
    created_at: datetime
    published_at: datetime | None


@dataclass(frozen=True)
class KnowledgeDocument:
    id: UUID
    title: str
    category: str
    source_type: SourceType
    source_ref: str
    visibility_roles: tuple[Role, ...]
    published_version_id: UUID | None


@dataclass(frozen=True)
class KnowledgeChunkPreview:
    id: UUID
    ordinal: int
    section: str
    page: int | None
    text: str


@dataclass(frozen=True)
class DocumentPreview:
    document_id: UUID
    version_id: UUID
    extracted_text: str
    chunks: tuple[KnowledgeChunkPreview, ...]


@dataclass(frozen=True)
class Citation:
    chunk_id: UUID
    document_id: UUID
    version_id: UUID
    section: str
    page: int | None
    text: str
    source_ref: str


@dataclass(frozen=True)
class RevokedCitation:
    chunk_id: UUID
    status: Literal["revoked"] = "revoked"


@dataclass(frozen=True)
class KnowledgeHit:
    chunk_id: UUID
    document_id: UUID
    version_id: UUID
    section: str
    page: int | None
    text: str
    source_label: str
    source_ref: str
    keyword_rank: int | None
    vector_rank: int | None
    score: float
    locator: str


@dataclass(frozen=True)
class SearchResult:
    hits: list[KnowledgeHit]
    mode: Literal["hybrid", "keyword_only", "vector_only"]
    elapsed_ms: float
