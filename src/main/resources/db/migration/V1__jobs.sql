CREATE TABLE job_schedules (
    id UUID PRIMARY KEY,
    type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    cron_expression VARCHAR(100) NOT NULL,
    zone_id VARCHAR(80) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    timeout_seconds INTEGER NOT NULL DEFAULT 30,
    next_run_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_schedules_due ON job_schedules(next_run_at, id) WHERE enabled;

CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    schedule_id UUID REFERENCES job_schedules(id),
    scheduled_for TIMESTAMPTZ,
    type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(24) NOT NULL CHECK (status IN ('QUEUED','RUNNING','FAILED','SUCCEEDED','DEAD_LETTERED','CANCELLED')),
    priority INTEGER NOT NULL DEFAULT 0,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_retries INTEGER NOT NULL CHECK (max_retries >= 0),
    timeout_seconds INTEGER NOT NULL CHECK (timeout_seconds > 0),
    available_at TIMESTAMPTZ NOT NULL,
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    current_execution_id UUID,
    result JSONB,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(schedule_id, scheduled_for)
);
CREATE INDEX ix_jobs_claim ON jobs(priority DESC, available_at, created_at) WHERE status IN ('QUEUED','FAILED');
CREATE INDEX ix_jobs_expired ON jobs(lease_until) WHERE status='RUNNING';
CREATE INDEX ix_jobs_schedule ON jobs(schedule_id, scheduled_for);
CREATE INDEX ix_jobs_created ON jobs(created_at DESC);

CREATE TABLE job_executions (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id),
    attempt_number INTEGER NOT NULL,
    worker_id VARCHAR(180) NOT NULL,
    lease_token UUID NOT NULL,
    status VARCHAR(24) NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED','TIMED_OUT','LOST')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    result JSONB,
    UNIQUE(job_id, attempt_number)
);
CREATE INDEX ix_executions_job ON job_executions(job_id, attempt_number);

CREATE TABLE dead_letters (
    job_id UUID PRIMARY KEY REFERENCES jobs(id),
    last_error TEXT NOT NULL,
    attempt_count INTEGER NOT NULL,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE queue_control (
    id INTEGER PRIMARY KEY CHECK (id=1),
    draining BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO queue_control(id, draining) VALUES (1, FALSE);
