#!/usr/bin/env python3
"""Print fresh secrets for .env. Usage: python backend/scripts/generate_secrets.py"""
import secrets

from cryptography.fernet import Fernet

print(f"POSTGRES_PASSWORD={secrets.token_urlsafe(24)}")
print(f"SESSION_ENCRYPTION_KEYS={Fernet.generate_key().decode()}")
print(f"JWT_SECRET={secrets.token_urlsafe(48)}")
print("\n# Remember: DATABASE_URL must use the same POSTGRES_PASSWORD.")
