$ErrorActionPreference = "Stop"

git config core.hooksPath .githooks
Write-Host "Configured git hooks path to .githooks"
Write-Host "The Copilot guard hook will now run on pre-commit."
