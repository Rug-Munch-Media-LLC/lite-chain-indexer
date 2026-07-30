# Security Policy

## Reporting a Vulnerability
Do NOT open a public issue. Email admin@rugmunch.io.

## Supported Versions
| Version | Supported |
|---------|-----------|
| latest  | ✅        |

## Security Practices
- All secrets via environment variables or secrets manager
- No hardcoded credentials in source code
- Regular dependency auditing via `pip-audit`
- Pre-commit hooks enforce gitleaks + methodology checks
