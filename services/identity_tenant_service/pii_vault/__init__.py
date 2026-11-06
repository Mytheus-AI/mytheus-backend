"""The per-tenant reversible PII token vault (ADR-0020), owned by identity-tenant-service.

- `vault.PiiVault`        mint / resolve against `pii_tokens` (envelope-encrypted values,
                          keyed blind index, audited resolve).
- `routes.router`         `POST /pii-tokens/mint` (internal) and `POST /pii-tokens/resolve`
                          (RBAC-gated). Not yet mounted on the app.
- `policy`                the PROPOSED unavailable-NER default (fail closed: hold the final).
"""
