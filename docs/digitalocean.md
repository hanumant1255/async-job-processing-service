# DigitalOcean deployment guide

Deployment is an optional assessment extension; GitHub submission and a functioning local system remain the first priorities.

## Recommended App Platform deployment

1. Push the repository to a **personal GitHub repository**.
2. In the authorized DigitalOcean team, provision a managed PostgreSQL database (or use an approved existing cluster). Do not modify other candidates' resources. Check billing/quotas first.
3. Create one App Platform application from the repository. Define two independently scalable components pointing at the **same Dockerfile/repository**:
   - **Web service** (public): `API_ENABLED=true`, `WORKER_ENABLED=false`, `SCHEDULER_ENABLED=false`.
   - **Worker** (private background service): `API_ENABLED=false`, `WORKER_ENABLED=true`, `SCHEDULER_ENABLED=false`. Set `WORKER_CONCURRENCY`, and scale worker instance count independently.
   - **Scheduler** (private worker): `API_ENABLED=false`, `WORKER_ENABLED=false`, `SCHEDULER_ENABLED=true`. Use one instance initially. Multiple instances are safe because schedule rows use `SKIP LOCKED`.
4. Inject `DATABASE_URL` as a JDBC URL (`jdbc:postgresql://.../jobs`) plus `DATABASE_USER` and `DATABASE_PASSWORD` using App Platform environment variables or bound secrets. Use SSL in production (`?sslmode=require`) where supported by your managed database. Set `API_KEY` for the web service.
5. Expose the API service on port 8080. Route external HTTP only to the API. Keep database and internal components private. Actuator metrics should not be public.
6. Verify `GET /actuator/health`, submit a `sum` job, poll for `SUCCEEDED`, simulate retries with `flaky`, and inspect the execution history.
7. Demonstrate worker scale-out by increasing worker instances independently of API instances and submitting a batch of `sleep` jobs.

**Flyway note:** Every process starts Flyway migrations by default. Flyway uses PostgreSQL coordination, but for stricter production control run schema migrations as a dedicated deployment step and disable Flyway in application instances. Ensure the app database user has the appropriate schema privileges only during migration.

**Deployment verification:** Not performed in this deliverable. The final deployed app URL and any cloud costs depend on the user's team resources.
