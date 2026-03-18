-- Create Schema
CREATE SCHEMA IF NOT EXISTS jobranking;

-- Rule Extraction DB Schema
CREATE TABLE IF NOT EXISTS jobranking.site_extract_rules (
  id           SERIAL PRIMARY KEY,
  site_name    TEXT NOT NULL,
  field_name   TEXT NOT NULL,
  selector     TEXT NOT NULL,
  selector_type TEXT NOT NULL CHECK (selector_type IN ('css', 'xpath')),
  version      INT NOT NULL DEFAULT 1,
  status       TEXT NOT NULL DEFAULT 'verified' 
               CHECK (status IN ('verified', 'candidate', 'deprecated')),
  confidence   FLOAT,
  source       TEXT CHECK (source IN ('manual', 'ai_recovery')),
  last_verified TIMESTAMPTZ,
  fail_count   INT DEFAULT 0,
  created_at   TIMESTAMPTZ DEFAULT now(),
  UNIQUE (site_name, field_name, version)
);

-- Main Jobs Table
CREATE TABLE IF NOT EXISTS jobranking.jobs (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_title    TEXT NOT NULL,
  company_name TEXT NOT NULL,
  location     TEXT,
  description  TEXT,
  salary_raw   TEXT,
  salary_min   FLOAT,
  salary_max   FLOAT,
  currency     TEXT DEFAULT 'VND',
  posted_date  TIMESTAMPTZ,
  contract_type TEXT,
  experience_level TEXT,
  industry     TEXT,
  job_function TEXT,
  skills       TEXT[], -- Array of extracted skills (NER)
  dedup_hash   TEXT UNIQUE, -- MD5/SHA256 of normalized JD
  created_at   TIMESTAMPTZ DEFAULT now(),
  updated_at   TIMESTAMPTZ DEFAULT now()
);

-- Job Sources (for multi-link jobs)
CREATE TABLE IF NOT EXISTS jobranking.job_sources (
  id           SERIAL PRIMARY KEY,
  job_id       UUID REFERENCES jobranking.jobs(id) ON DELETE CASCADE,
  site_name    TEXT NOT NULL,
  source_url   TEXT NOT NULL UNIQUE,
  crawled_at   TIMESTAMPTZ DEFAULT now()
);

-- Indices for search and dedup
CREATE INDEX IF NOT EXISTS idx_jobs_dedup_hash ON jobranking.jobs(dedup_hash);
CREATE INDEX IF NOT EXISTS idx_jobs_company_title ON jobranking.jobs(company_name, job_title);
CREATE INDEX IF NOT EXISTS idx_rules_site_field ON jobranking.site_extract_rules(site_name, field_name);
