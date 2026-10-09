$ErrorActionPreference = 'Stop'

Write-Host 'Telegram Migrator - Docker startup' -ForegroundColor Cyan

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error 'Docker Desktop/CLI not found. Install and start Docker Desktop, then re-run this script.'
}

docker info *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Error 'Docker daemon is not running. Start Docker Desktop and wait until it is ready.'
}

if (-not (Test-Path '.env')) {
    Copy-Item '.env.example' '.env'
    Write-Host 'Created .env from .env.example.' -ForegroundColor Yellow
    Write-Host 'Now edit .env: set TELEGRAM_API_ID, TELEGRAM_API_HASH, generated secrets, and matching DATABASE_URL password.' -ForegroundColor Yellow
    Write-Host 'Generate secrets with: python -m pip install cryptography; python backend/scripts/generate_secrets.py'
    exit 1
}

$envContent = Get-Content '.env' -Raw
if ($envContent -match 'replace-me|TELEGRAM_API_ID=0|TELEGRAM_API_HASH=replace') {
    Write-Error 'The .env file still contains example placeholders. Fill the secrets and Telegram API credentials before continuing.'
}

docker compose build
docker compose up -d
docker compose ps
Write-Host ''
Write-Host 'Frontend: http://localhost:3000' -ForegroundColor Green
Write-Host 'API docs: http://localhost:3000/docs' -ForegroundColor Green
$open = Read-Host 'Open the frontend in your browser? (Y/N)'
if ($open -match '^(Y|YES)$') { Start-Process 'http://localhost:3000' }
