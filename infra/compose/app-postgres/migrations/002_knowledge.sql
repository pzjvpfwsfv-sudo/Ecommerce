CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS knowledge;

CREATE TABLE IF NOT EXISTS knowledge.documents (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  title TEXT NOT NULL,
  category TEXT NOT NULL,
  source_type TEXT NOT NULL CHECK (source_type IN ('project_doc', 'external')),
  source_ref TEXT NOT NULL,
  visibility_roles TEXT[] NOT NULL
    CHECK (cardinality(visibility_roles) BETWEEN 1 AND 3
      AND visibility_roles <@ ARRAY['admin','analyst','viewer']::TEXT[]
      AND array_position(visibility_roles, NULL) IS NULL),
  owner_id BIGINT NOT NULL REFERENCES app_users(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  withdrawn_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS knowledge.versions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id UUID NOT NULL REFERENCES knowledge.documents(id) ON DELETE CASCADE,
  version_no INTEGER NOT NULL CHECK (version_no > 0),
  status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'published', 'withdrawn')),
  original_filename TEXT NOT NULL,
  original_bytes BYTEA NOT NULL,
  extracted_text TEXT NOT NULL,
  content_sha256 CHAR(64) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ,
  UNIQUE (document_id, version_no)
);

CREATE UNIQUE INDEX IF NOT EXISTS knowledge_one_published_per_document
  ON knowledge.versions (document_id) WHERE status = 'published';

CREATE TABLE IF NOT EXISTS knowledge.chunks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  version_id UUID NOT NULL REFERENCES knowledge.versions(id) ON DELETE CASCADE,
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  section TEXT NOT NULL,
  page INTEGER CHECK (page > 0),
  content TEXT NOT NULL,
  terms TEXT[] NOT NULL DEFAULT '{}',
  sha256 CHAR(64) NOT NULL,
  embedding vector(512) NOT NULL,
  UNIQUE (version_id, ordinal)
);

CREATE INDEX IF NOT EXISTS knowledge_chunks_terms_idx
  ON knowledge.chunks USING GIN (terms);
