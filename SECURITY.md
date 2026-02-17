# Security Guidelines for Open Data Platform

## Overview
This document outlines security best practices for this repository to prevent accidental exposure of sensitive credentials and secrets.

## Critical Rules

### 1. **NEVER Commit Credentials**
- ✗ FORBIDDEN: `.env` files with real credentials
- ✗ FORBIDDEN: Hard-coded API keys, passwords, tokens
- ✗ FORBIDDEN: AWS/Azure/GCP credentials
- ✗ FORBIDDEN: Database passwords
- ✓ ALLOWED: `.env.template` with placeholder values

### 2. **Credential Sources**
Use **one of these methods** for credentials:

#### Option A: Local `.env` File (Development)
```bash
# 1. Copy the template
cp .env.template .env

# 2. Fill in REAL values in .env (local only)
# 3. .env is gitignored and never committed
```

#### Option B: Environment Variables (CI/CD & Production)
```bash
# Set directly in CI/CD platform (GitHub, GitLab, etc.)
export MINIO_SECRET_KEY="actual_secret_value"
```

#### Option C: Secrets Manager (Production Recommended)
```bash
# Use Docker secrets, Kubernetes secrets, or managed services:
# - AWS Secrets Manager
# - Azure Key Vault
# - HashiCorp Vault
# - GitHub Secrets (for CI/CD)
```

## Files Protected by `.gitignore`

### Automatically Ignored:
```
.env                          # Local credentials file
.env.*                        # Environment-specific configs
secrets.json                  # Credential JSON files
airflow_secrets.json          # Airflow-specific secrets
*_secrets.*                   # Any file with "_secrets" in name
credentials.json              # Generic credentials
config.local.*                # Local config overrides
__pycache__/                  # Python cache
logs/                         # Airflow execution logs
frontend/node_modules/        # NPM dependencies
```

### Exception:
```
.env.template                 # SAFE: Contains only placeholder values
```

## Pre-Commit Hook Protection

A **pre-commit hook** prevents accidental commits of secrets:

### What It Checks:
- Detects common secret patterns (password=, secret=, api_key=, etc.)
- Blocks commits containing `.env` files
- Scans for private keys and certificates

### When It Triggers:
```bash
git add sensitive_file.py
git commit -m "Add new feature"  # ← Hook runs here
# ✗ BLOCKED: If file contains "password=realvalue"
```

### If Hook Blocks Commit:
```bash
# 1. Remove the sensitive data from the file
# 2. Use .env or environment variables instead
# 3. Re-stage: git add <file>
# 4. Try commit again
```

## Common Mistakes & Solutions

### ❌ Mistake #1: Hard-coded Credentials in Code
```python
# WRONG - Never do this!
db_password = "admin123"
api_key = "sk_live_jd93jdj29dj"

# CORRECT - Use environment variables
import os
db_password = os.getenv("DB_PASSWORD")
api_key = os.getenv("API_KEY")
```

### ❌ Mistake #2: Committing `.env` File
```bash
# WRONG
git add .env          # ← This will be blocked by pre-commit hook

# CORRECT
git add .env.template # ← Only this is allowed
```

### ❌ Mistake #3: Credentials in Docker Compose
```yaml
# WRONG - Hardcoded in docker-compose.yml
services:
  postgres:
    environment:
      POSTGRES_PASSWORD: admin123  # ← Visible to all

# CORRECT - Use .env file
services:
  postgres:
    environment:
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}  # ← Loaded from .env
```

## Setup Instructions for New Developers

### Initial Setup:
```bash
# 1. Clone the repository
git clone <repo>
cd ai_trial

# 2. Copy environment template
cp .env.template .env

# 3. Fill in YOUR local values in .env
# - Get credentials from team lead or secrets manager
# - NEVER check .env into git
nano .env

# 4. Verify pre-commit hook is active
ls -la .git/hooks/pre-commit  # Should exist and be executable

# 5. Test the hook (should be blocked)
echo "test_password = 'secret123'" > test.py
git add test.py
git commit -m "Test"  # ← Should be blocked by hook
rm test.py           # Clean up
```

## If a Secret Was Already Committed

### ⚠️ Emergency Action Required:

```bash
# Option 1: Remove from git history (Recommended for small repos)
git filter-branch --tree-filter 'rm -f <file_with_secret>' HEAD

# Option 2: Use BFG Repo-Cleaner (faster for large repos)
bfg --delete-files <file_with_secret>

# Option 3: If on dev branch only (safest)
git reset --soft HEAD~1         # Undo last commit
rm .env                         # Remove secret file
git add -A
git commit -m "Remove sensitive data"
```

### Then Immediately:
1. **Rotate all exposed credentials** (change passwords, regenerate API keys)
2. **Alert team members** about the exposure
3. **Review git history** for other exposed secrets
4. **Notify security team** if production credentials were exposed

## Tools for Secret Detection

### Local Development:
```bash
# Install git-secrets
brew install git-secrets

# Initialize for this repo
git secrets --install

# Check committed files
git secrets --scan
```

### CI/CD Pipeline:
- **GitHub**: Use "Secret Scanning" (org-level feature)
- **GitLab**: Use "Secret Detection" in CI pipeline
- **General**: Use `detect-secrets` or `truffleHog3`

## Team Policies

1. **Code Review**: All PRs reviewed before merge
2. **Secrets Manager**: Use external service for production
3. **Rotation**: Rotate credentials every 90 days
4. **Access Control**: Limit who has access to credentials
5. **Auditing**: Log who accessed what credentials and when

## Questions or Issues?

If you accidentally commit a secret:
1. **DO NOT PANIC** - It's recoverable
2. Alert the security team immediately
3. Follow the "Emergency Action" steps above
4. Document what happened to prevent future incidents

---

**Last Updated**: February 2026  
**Maintained By**: Security Team  
**Policy Version**: 1.0
