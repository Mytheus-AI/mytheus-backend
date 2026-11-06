"""The per-tenant reversible PII token vault (ADR-0020), owned by identity-tenant-service.

- `vault.PiiVault`        mint / resolve against `pii_tokens` (envelope-encrypted values,
                          keyed blind index, audited resolve).
- `routes.router`         `POST /pii-tokens/mint` (internal) and `POST /pii-tokens/resolve`
                          (RBAC-gated, `admin`). Mounted in `main.py`.
- `policy`                the DECIDED unavailable-NER rule (fail closed: hold the final).
"""
