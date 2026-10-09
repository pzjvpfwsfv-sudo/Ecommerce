CREATE TABLE IF NOT EXISTS agent_answers (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  owner_id BIGINT NOT NULL REFERENCES app_users(id),
  question TEXT NOT NULL CHECK (char_length(trim(question)) BETWEEN 1 AND 500),
  answer JSONB NOT NULL CHECK (jsonb_typeof(answer) = 'object'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (id, owner_id)
);

CREATE INDEX IF NOT EXISTS agent_answers_owner_recent_idx
  ON agent_answers (owner_id, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS agent_reports (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  owner_id BIGINT NOT NULL REFERENCES app_users(id),
  answer_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (answer_id, owner_id) REFERENCES agent_answers(id, owner_id)
);

CREATE INDEX IF NOT EXISTS agent_reports_owner_recent_idx
  ON agent_reports (owner_id, created_at DESC, id DESC);
