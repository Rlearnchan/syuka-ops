# Repo Checklist

Use this checklist right before the first push to GitHub.

## Secrets and data

- Confirm `.env`, `.env.company`, and `.env.workspace` are not staged.
- Confirm `data/db`, `data/logs`, `data/reports`, `data/batches`, `data/scripts/raw`, and `data/thumbnails` are not staged.
- Confirm no Slack tokens, OpenAI keys, or internal URLs are hardcoded in source or docs.

## Repository hygiene

- Review `README.md` so the current scheduler times, Slack behavior, and patch notes are accurate.
- Review `docs/GITHUB_SETUP.md` for the exact first-push steps.
- Review `.gitignore` and `.gitattributes`.
- Decide whether the repository should start as private.

## Recommended first commands

```powershell
git init
git branch -M main
git add .
git status
git diff --cached
git commit -m "Initial commit"
git remote add origin <YOUR_GITHUB_REPO_URL>
git push -u origin main
```
