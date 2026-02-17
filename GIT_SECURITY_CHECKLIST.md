# Git Security Checklist

## Pre-Commit Checklist

Before running `git commit`, verify:

- [ ] No `.env` files with real credentials are staged
- [ ] No hard-coded passwords/API keys in code
- [ ] No `secrets.json` or credential files staged
- [ ] All sensitive data uses `${VARIABLE_NAME}` placeholders instead
- [ ] Only `.env.template` is staged (not `.env`)
- [ ] Pre-commit hook runs without errors

## Pre-Push Checklist

Before running `git push origin <branch>`, verify:

- [ ] Run `git log -p --all -S 'password' -S 'secret' -S 'api_key'` to check history
- [ ] No sensitive data in commit messages
- [ ] All files have been reviewed for secrets
- [ ] `.gitignore` includes all sensitive file patterns

## Repository Audit (Monthly)

```bash
# Check for secrets in committed history
git log --all -p | grep -E 'password|secret|api_key|api_secret' | head -20

# Verify .gitignore effectiveness
git check-ignore -v .env
git check-ignore -v secrets.json
git check-ignore -v .env.template  # Should NOT be ignored

# List all environment variables used
grep -r '\${[A-Z_]*}' --include='*.py' --include='*.js' \
  --include='*.yaml' --include='*.yml' \
  --include='*.json' dags/ pipelines/ scripts/ | cut -d: -f2 | sort -u
```

## If You Accidentally Push Secrets

### IMMEDIATE ACTIONS (Within 2 hours):
1. [ ] Create GitHub/GitLab issue: "URGENT: Secrets exposed in commit XXXXX"
2. [ ] Notify team lead and security engineer
3. [ ] Rotate all exposed credentials immediately
4. [ ] Revoke any API keys/tokens from remote services

### RECOVERY (Next 24 hours):
1. [ ] Use `git filter-branch` or `bfg` to remove secrets
2. [ ] Force push corrected history: `git push --force-with-lease`
3. [ ] Update all team members to pull the cleaned history
4. [ ] Document incident and prevention measures
5. [ ] Schedule post-incident review

## Resources

- **Pre-commit Hook**: `.git/hooks/pre-commit` (auto-blocks secrets)
- **Security Guide**: [SECURITY.md](SECURITY.md)
- **Environment Template**: [.env.template](.env.template)
- **Git Ignore Rules**: [.gitignore](.gitignore)

## Quick Commands

```bash
# Test pre-commit hook
.git/hooks/pre-commit

# Check if file would be ignored
git check-ignore -v <filename>

# Remove file from git cache (keep local copy)
git rm --cached <filename>

# View git ignored files
git status --ignored

# Search git history for credentials
git log --all -p | grep -i "password\|secret\|key" | head -50
```

---

**Last Check**: Run this before every commit:
```bash
git diff --cached | grep -iE 'password|secret|api_key|token' && echo "⚠️ SECRETS FOUND!" || echo "✓ No secrets detected"
```
