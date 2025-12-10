-- New York Regulatory Analysis Database Schema
-- For storing regulations and policy documents with LLM analysis results

-- Generic table for regulations and policy documents
CREATE TABLE regulatory_documents (
  id BIGSERIAL PRIMARY KEY,
  source_index INTEGER,
  type VARCHAR(50) NOT NULL, -- 'regulation' or 'policy_document'
  state VARCHAR(2), -- 'NY', 'CA', etc.
  title TEXT NOT NULL,
  url TEXT,
  url_type VARCHAR(50),
  content TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  -- Compound unique constraint per pull
  CONSTRAINT unique_per_pull UNIQUE(state, source_index, type)
);

-- Analysis runs (applies to both regulations and policy docs)
CREATE TABLE analyses (
  id BIGSERIAL PRIMARY KEY,
  document_id BIGINT NOT NULL REFERENCES regulatory_documents(id) ON DELETE CASCADE,

  -- Analysis metadata
  analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  model_version VARCHAR(100),
  has_implementation_issues BOOLEAN DEFAULT FALSE,
  overall_complexity VARCHAR(10), -- HIGH, MEDIUM, LOW, NULL
  summary TEXT,
  requires_technical_review BOOLEAN DEFAULT FALSE,
  has_reporting_requirement BOOLEAN DEFAULT FALSE,

  -- Summary metrics
  max_severity INTEGER,
  num_flags INTEGER DEFAULT 0,

  -- Mark most recent analysis
  is_current BOOLEAN DEFAULT TRUE,

  FOREIGN KEY (document_id) REFERENCES regulatory_documents(id) ON DELETE CASCADE
);

-- Individual red flags
CREATE TABLE red_flags (
  id BIGSERIAL PRIMARY KEY,
  analysis_id BIGINT NOT NULL REFERENCES analyses(id) ON DELETE CASCADE,

  category VARCHAR(100) NOT NULL,
  explanation TEXT NOT NULL,
  severity INTEGER NOT NULL CHECK (severity >= 1 AND severity <= 10),
  complexity VARCHAR(10) NOT NULL,
  matched_phrases TEXT[] DEFAULT '{}',
  implementation_approach TEXT NOT NULL,
  effort_estimate VARCHAR(255),
  text_examples TEXT[] DEFAULT '{}'
);

-- Statute references (mainly for regulations, optional for policies)
CREATE TABLE statute_references (
  id BIGSERIAL PRIMARY KEY,
  document_id BIGINT NOT NULL REFERENCES regulatory_documents(id) ON DELETE CASCADE,

  usc_citations TEXT,
  cfr_citations TEXT,
  public_laws TEXT,
  acts TEXT,
  state_title VARCHAR(100),
  state_section VARCHAR(100)
);

-- Indexes for common queries
CREATE INDEX idx_documents_state ON regulatory_documents(state);
CREATE INDEX idx_documents_type ON regulatory_documents(type);
CREATE INDEX idx_documents_state_type ON regulatory_documents(state, type);
CREATE INDEX idx_documents_title ON regulatory_documents(title);
CREATE INDEX idx_documents_url_type ON regulatory_documents(url_type);
CREATE INDEX idx_analyses_document ON analyses(document_id);
CREATE INDEX idx_analyses_analyzed_at ON analyses(analyzed_at);
CREATE INDEX idx_analyses_is_current ON analyses(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_red_flags_analysis ON red_flags(analysis_id);
CREATE INDEX idx_red_flags_category ON red_flags(category);
CREATE INDEX idx_red_flags_severity ON red_flags(severity);
CREATE INDEX idx_statute_references_document ON statute_references(document_id);

-- View for quick access to latest analyses
CREATE VIEW latest_analyses AS
SELECT
  d.id,
  d.source_index,
  d.type,
  d.state,
  d.title,
  d.url,
  d.url_type,
  a.id as analysis_id,
  a.analyzed_at,
  a.has_implementation_issues,
  a.overall_complexity,
  a.requires_technical_review,
  a.has_reporting_requirement,
  a.num_flags,
  a.max_severity,
  a.summary
FROM regulatory_documents d
LEFT JOIN analyses a ON d.id = a.document_id AND a.is_current = TRUE;
