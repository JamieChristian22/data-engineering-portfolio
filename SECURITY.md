# Security Policy

## Scope

This is an independent, educational data engineering portfolio. It contains pipeline demonstrations, analytics models, and example configurations rather than a managed production data service.

## Reporting security concerns

**Do not open a public issue** containing credentials, private datasets, identifiable personal information, exploitable secrets, or sensitive vulnerability details.

If GitHub provides a **Report a vulnerability** option on the repository's Security tab, use it to make a private report. Otherwise, contact the repository maintainer privately using the professional contact information linked in [README.md](README.md).

A useful report includes:

- Affected file, project, configuration, or dependency.
- Observed behavior and why it may expose data or services.
- A safe reproduction using non-sensitive sample data.
- Suggested mitigations, if available.

Do not perform unauthorized security testing or submit real credentials or third-party records.

## Security practices

- Keep tokens, cloud keys, and database passwords outside source control.
- Prefer least-privilege access and environment-based secret management.
- Use synthetic or properly licensed public datasets for demonstrations.
- Validate external dependencies and inspect SQL/database operations before running them.
- Revoke or rotate any credential that is accidentally exposed.

## Maintenance

Security reports are reviewed as maintainer availability permits. This portfolio does not offer a guaranteed response time, hosted production support, or security SLA. Local pipeline validation does not establish production security or compliance.
