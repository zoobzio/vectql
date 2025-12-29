# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| Latest  | :white_check_mark: |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security issue, please follow these steps:

1. **DO NOT** create a public GitHub issue
2. Email security details to the maintainers
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if available)

## Security Best Practices

When using VECTQL:

1. **Query Validation**: Always validate generated queries before executing against production databases.

2. **Parameter Handling**: VECTQL uses parameterized queries - never interpolate user input directly into query strings.

3. **Provider Credentials**: Store vector database credentials securely; never commit them to version control.

4. **Schema Validation**: When using VDML integration, validate schemas before deployment.

## Security Features

VECTQL is designed with security in mind:

- Parameterized queries prevent injection attacks
- No direct database connections (rendering only)
- Minimal external dependencies (reduces supply chain risks)
- No network operations beyond go module fetching
- Pure Go implementation
- Thread-safe operations

## Acknowledgments

We appreciate responsible disclosure of security vulnerabilities.
