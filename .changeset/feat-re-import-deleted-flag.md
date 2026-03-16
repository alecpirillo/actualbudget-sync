---
"@tim-smart/actualbudget-sync": minor
---

feat: add --re-import-deleted flag

Adds a `--re-import-deleted` CLI flag that allows transactions previously
deleted in Actual Budget to be re-imported on the next sync. By default,
deleted transactions remain excluded.
