# DigitalOcean Deployment Guide

Step-by-step instructions for deploying the Async Job Service to DigitalOcean App Platform with independently scalable API, worker, and scheduler components.

---

## Architecture Overview

The same Docker image runs as three independently scalable roles, controlled by environment variables:

| Role | API_ENABLED | WORKER_ENABLED | SCHEDULER_ENABLED | Scales With |
|---|---|---|---|---|
| **API** (public web service) | `true` | `false` | `false` | Request throughput |
| **Worker** (background processor) | `false` | `true` | `false` | Job queue depth |
| **Scheduler** (cron materializer) | `false` | `false` | `true` | Schedule count (1 instance sufficient) |

All three connect to the **same PostgreSQL database**. Workers compete for jobs using `SELECT ... FOR UPDATE SKIP LOCKED`, so scaling workers simply adds more concurrent job processors.

---

## Prerequisites

1. **DigitalOcean account** with App Platform access (you should already be a team member)
2. **GitHub repository** with the code pushed (public or private)
3. **GitHub CLI** (`gh`) configured with your account
4. Check your team's billing/quotas before provisioning resources

---

## Step 1: Push Code to GitHub

```bash
# From your project directory
cd /path/to/async-job-service

# Create a new repository (if not already done)
gh repo create your-username/async-job-service --private --clone .

# Or push to an existing repository
git remote add origin git@github.com:your-username/async-job-service.git
git push -u origin main
```

---

## Step 2: Provision Managed PostgreSQL

### Option A: DigitalOcean Managed Database (Recommended)

1. Navigate to **Databases** → **Create Database** in DigitalOcean control panel
2. Select **Production** cluster (or Basic for assessment)
3. Choose PostgreSQL 16, smallest plan (e.g., `db-s-1vcpu-1gb`)
4. Name it `async-job-db` (or any name)
5. Wait for the database to be provisioned (~2-3 minutes)
6. Once ready, note the **connection details**:
   - Host: `db-xxx-do-user-xxx.db.ondigitalocean.com`
   - Port: `25060`
   - Default user: `doadmin`
   - Default password: (shown during creation, or reset from console)
   - Default database: `defaultdb`

### Option B: Use Existing Team Database

If your team already has a PostgreSQL cluster, ask for:
- Connection string
- Database name to use
- Username and password with CREATE TABLE privileges

---

## Step 3: Create the Database

Connect to your managed PostgreSQL and create the application database:

```bash
# Connect using the connection details from Step 2
psql "host=db-xxx-do-user-xxx.db.ondigitalocean.com port=25060 dbname=defaultdb user=doadmin sslmode=require"

# Inside psql:
CREATE DATABASE jobs;
\q
```

---

## Step 4: Create App Platform Application

### Via DigitalOcean Control Panel (Recommended for Interview Demo)

1. Navigate to **Apps** → **Create App**
2. Select **GitHub** as the source
3. Authorize GitHub and select your `async-job-service` repository
4. Select the `main` branch

### Adding Components

You'll add **three components** from the same repository. For each:

#### Component 1: API (Web Service)

1. Click **Add Component** → **Web Service**
2. Configuration:
   - **Name**: `api`
   - **Git Branch**: `main`
   - **Build Command**: (leave blank, uses Dockerfile)
   - **Dockerfile path**: `Dockerfile`
   - **Environment**: `Production`
   - **Instance size**: Basic ($5/mo) or Professional depending on budget
   - **Instance count**: `1`
3. **Environment Variables** (click "Add Variable"):

| Variable | Value |
|---|---|
| `API_ENABLED` | `true` |
| `WORKER_ENABLED` | `false` |
| `SCHEDULER_ENABLED` | `false` |
| `DATABASE_URL` | `jdbc:postgresql://<host>:25060/jobs?sslmode=require` |
| `DATABASE_USER` | `doadmin` (or your app user) |
| `DATABASE_PASSWORD` | `<your-password>` |
| `API_KEY` | `your-secret-api-key-here` |
| `DB_POOL_SIZE` | `10` |

4. **Health Check** (critical for App Platform):
   - **Type**: HTTP
   - **Path**: `/actuator/health`
   - **Interval**: 10s
   - **Timeout**: 5s
   - **Unhealthy threshold**: 3

5. Save and continue

#### Component 2: Worker (Background Worker)

1. Click **Add Component** → **Worker**
2. Configuration:
   - **Name**: `worker`
   - **Git Branch**: `main`
   - **Dockerfile path**: `Dockerfile`
   - **Environment**: `Production`
   - **Instance size**: Basic ($5/mo)
   - **Instance count**: `1` (start with 1, scale to 3+ to demonstrate)
3. **Environment Variables**:

| Variable | Value |
|---|---|
| `API_ENABLED` | `false` |
| `WORKER_ENABLED` | `true` |
| `SCHEDULER_ENABLED` | `false` |
| `DATABASE_URL` | `jdbc:postgresql://<host>:25060/jobs?sslmode=require` |
| `DATABASE_USER` | `doadmin` |
| `DATABASE_PASSWORD` | `<your-password>` |
| `WORKER_CONCURRENCY` | `4` |
| `WORKER_POLL_MS` | `250` |

4. Save and continue

#### Component 3: Scheduler (Background Worker)

1. Click **Add Component** → **Worker**
2. Configuration:
   - **Name**: `scheduler`
   - **Git Branch**: `main`
   - **Dockerfile path**: `Dockerfile`
   - **Environment**: `Production`
   - **Instance size**: Basic ($5/mo)
   - **Instance count**: `1`
3. **Environment Variables**:

| Variable | Value |
|---|---|
| `API_ENABLED` | `false` |
| `WORKER_ENABLED` | `false` |
| `SCHEDULER_ENABLED` | `true` |
| `DATABASE_URL` | `jdbc:postgresql://<host>:25060/jobs?sslmode=require` |
| `DATABASE_USER` | `doadmin` |
| `DATABASE_PASSWORD` | `<your-password>` |

4. Save and deploy

### Via App Spec YAML (Alternative, Faster)

Create `app.yaml` in your repository root:

```yaml
name: async-job-service
region: nyc
services:
- name: api
  source_dir: /
  github:
    branch: main
    repo: your-username/async-job-service
  dockerfile_path: Dockerfile
  instance_count: 1
  instance_size_slug: basic-xxs
  health_check:
    http_path: /actuator/health
  envs:
  - key: API_ENABLED
    value: "true"
  - key: WORKER_ENABLED
    value: "false"
  - key: SCHEDULER_ENABLED
    value: "false"
  - key: DATABASE_URL
    scope: RUN_TIME
    type: SECRET
    value: jdbc:postgresql://<host>:25060/jobs?sslmode=require
  - key: DATABASE_USER
    scope: RUN_TIME
    value: doadmin
  - key: DATABASE_PASSWORD
    scope: RUN_TIME
    type: SECRET
    value: <your-password>
  - key: API_KEY
    scope: RUN_TIME
    type: SECRET
    value: your-secret-api-key-here
workers:
- name: worker
  source_dir: /
  github:
    branch: main
    repo: your-username/async-job-service
  dockerfile_path: Dockerfile
  instance_count: 1
  instance_size_slug: basic-xxs
  envs:
  - key: API_ENABLED
    value: "false"
  - key: WORKER_ENABLED
    value: "true"
  - key: SCHEDULER_ENABLED
    value: "false"
  - key: DATABASE_URL
    scope: RUN_TIME
    value: jdbc:postgresql://<host>:25060/jobs?sslmode=require
  - key: DATABASE_USER
    scope: RUN_TIME
    value: doadmin
  - key: DATABASE_PASSWORD
    scope: RUN_TIME
    type: SECRET
    value: <your-password>
  - key: WORKER_CONCURRENCY
    value: "4"
- name: scheduler
  source_dir: /
  github:
    branch: main
    repo: your-username/async-job-service
  dockerfile_path: Dockerfile
  instance_count: 1
  instance_size_slug: basic-xxs
  envs:
  - key: API_ENABLED
    value: "false"
  - key: WORKER_ENABLED
    value: "false"
  - key: SCHEDULER_ENABLED
    value: "true"
  - key: DATABASE_URL
    scope: RUN_TIME
    value: jdbc:postgresql://<host>:25060/jobs?sslmode=require
  - key: DATABASE_USER
    scope: RUN_TIME
    value: doadmin
  - key: DATABASE_PASSWORD
    scope: RUN_TIME
    type: SECRET
    value: <your-password>
```

Then deploy:
```bash
doctl apps create-app spec app.yaml
```

---

## Step 5: Wait for Deployment

1. App Platform builds the Docker image (~3-5 minutes first build)
2. All three components deploy in parallel
3. Monitor deployment status in the App Platform console
4. Once all components show "Live", note the API URL (e.g., `https://async-job-service-api-xxx.ondigitalocean.app`)

---

## Step 6: Verify Deployment

### Health Check

```bash
API_URL="https://async-job-service-api-xxx.ondigitalocean.app"

# Health endpoint (no auth required)
curl $API_URL/actuator/health
# Expected: {"status":"UP"}
```

### Submit a Job

```bash
# Submit a simple sum job
curl -X POST $API_URL/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-api-key-here" \
  -d '{"type":"sum","payload":{"numbers":[1,2,3,4,5]}}'

# Expected response (HTTP 202):
# {"id":"uuid-here","type":"sum","status":"QUEUED",...}
```

### Check Job Status

```bash
JOB_ID="<id-from-above>"

# Poll until SUCCEEDED
curl -H "X-API-Key: your-secret-api-key-here" $API_URL/jobs/$JOB_ID
# Expected: {"status":"SUCCEEDED","result":{"sum":15},...}

# Check execution history
curl -H "X-API-Key: your-secret-api-key-here" $API_URL/jobs/$JOB_ID/executions
```

### Test Retry with Flaky Job

```bash
# Submit a job that fails until attempt 3
curl -X POST $API_URL/jobs \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-api-key-here" \
  -d '{"type":"flaky","payload":{"failUntilAttempt":2},"maxRetries":5}'

# Wait ~30 seconds for retries, then check
curl -H "X-API-Key: your-secret-api-key-here" $API_URL/jobs/<flaky-job-id>
# Expected: status=SUCCEEDED, attemptCount=3
```

### Check Queue Depth

```bash
curl -H "X-API-Key: your-secret-api-key-here" $API_URL/queue/depth
# Expected: {"ready":0,"running":0,"succeeded":N,...}
```

---

## Step 7: Demonstrate Independent Worker Scaling

This is the **key demonstration** for the interview.

### Before Scaling

```bash
# Submit 10 long-running jobs
for i in $(seq 1 10); do
  curl -s -X POST $API_URL/jobs \
    -H "Content-Type: application/json" \
    -H "X-API-Key: your-secret-api-key-here" \
    -d '{"type":"sleep","payload":{"millis":5000}}'
done

# Check queue depth - jobs are processing
curl -H "X-API-Key: your-secret-api-key-here" $API_URL/queue/depth
# You'll see "running": 4 (one worker with concurrency=4)
```

### Scale Workers

1. In DigitalOcean App Platform console
2. Find the **worker** component
3. Change **Instance Count** from `1` to `3`
4. Click **Deploy** (takes ~1-2 minutes)

### After Scaling

```bash
# Submit another batch of 10 jobs
for i in $(seq 1 10); do
  curl -s -X POST $API_URL/jobs \
    -H "Content-Type: application/json" \
    -H "X-API-Key: your-secret-api-key-here" \
    -d '{"type":"sleep","payload":{"millis":5000}}'
done

# Check queue depth - now processing faster
curl -H "X-API-Key: your-secret-api-key-here" $API_URL/queue/depth
# You'll see "running": 12 (3 workers × 4 concurrency)
```

### Key Talking Points

- **API is untouched**: The API component continues serving requests while workers scale
- **Zero-downtime**: Scaling workers doesn't affect running jobs or API availability
- **Database coordination**: Workers use `SKIP LOCKED` to safely compete for jobs — no two workers process the same job
- **Independent cost**: You can scale workers up during peak load and down during off-peak without touching the API tier

---

## Step 8: Demonstrate Scheduled Jobs

### Create a Cron Schedule

```bash
# Create a schedule that runs every minute
curl -X POST $API_URL/schedules \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-api-key-here" \
  -d '{"type":"sum","payload":{"numbers":[100,200]},"cronExpression":"0 * * * * *","timezone":"UTC"}'

# Expected: {"id":"schedule-uuid","nextRunAt":"2024-..."}
```

### Verify Scheduled Jobs Are Created

```bash
# Wait ~60 seconds, then check queue depth
curl -H "X-API-Key: your-secret-api-key-here" $API_URL/queue/depth
# You should see new succeeded jobs from the scheduler

# Check execution history to see schedule_id
curl -H "X-API-Key: your-secret-api-key-here" $API_URL/jobs/<scheduled-job-id>/executions
```

---

## Cost Estimate

| Resource | Plan | Monthly Cost |
|---|---|---|
| PostgreSQL Managed DB | Basic 1 vCPU, 1 GB | ~$15/mo |
| API Web Service | Basic XXS | ~$5/mo |
| Worker (1 instance) | Basic XXS | ~$5/mo |
| Worker (3 instances) | Basic XXS × 3 | ~$15/mo |
| Scheduler (1 instance) | Basic XXS | ~$5/mo |
| **Total (1 worker)** | | **~$35/mo** |
| **Total (3 workers)** | | **~$45/mo** |

For assessment purposes, use the smallest instances and tear down after the demo.

---

## Cleanup

After the interview/demo, tear down resources to avoid charges:

```bash
# Delete the App Platform application
doctl apps delete <app-id> --force

# Delete the managed database
doctl databases delete <db-id> --force
```

Or manually delete from the DigitalOcean control panel.

---

## Troubleshooting

### Flyway Migration Conflicts

If multiple instances start simultaneously, Flyway coordinates via PostgreSQL locks. If you see migration errors:
- Check that the database user has `CREATE` privilege on the schema
- Verify only one Flyway version runs against the database

### Worker Not Processing Jobs

- Check worker logs in App Platform console
- Verify `WORKER_ENABLED=true` is set
- Confirm database connectivity: `DATABASE_URL` must include `?sslmode=require` for managed DBs

### API Returns 401 Unauthorized

- Verify `X-API-Key` header matches the `API_KEY` environment variable
- Health endpoint (`/actuator/health`) does not require authentication

### Health Check Failing

- Ensure `/actuator/health` is exposed in `management.endpoints.web.exposure.include`
- Check that Flyway migration completes before the health check starts (start-period accounts for this)

---

## Local Development Equivalent

Before deploying, test the same architecture locally:

```bash
# Start all components
docker compose up --build -d

# Scale workers independently
docker compose up -d --scale worker=3

# Verify
curl http://localhost:8080/actuator/health
curl http://localhost:8080/queue/depth
```

This mirrors the DigitalOcean setup: same Dockerfile, same environment variables, same database.
