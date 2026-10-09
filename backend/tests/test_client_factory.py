"""The REAL Telethon client factory (constructed, never connected): guards the policy settings."""
from telethon.crypto import AuthKey
from telethon.sessions import StringSession

from app.telegram.client import TelegramClientFactory


def _factory(**kw):
    return TelegramClientFactory(api_id=12345, api_hash="0123456789abcdef0123456789abcdef", **kw)


def test_client_never_auto_sleeps_on_flood_wait_and_ignores_updates():
    client = _factory(device_name="TG Migrator", timeout=7, connection_retries=3).create(None)
    assert client.flood_sleep_threshold == 0      # FloodWait is raised to us, never silently slept through
    assert client._no_updates is True            # receive_updates=False
    assert client.api_id == 12345
    assert client._timeout == 7


def test_client_restores_a_saved_session_without_connecting():
    original = StringSession()
    original.set_dc(2, "149.154.167.51", 443)
    original.auth_key = AuthKey(bytes(range(256)))
    saved = original.save()

    client = _factory().create(saved)
    assert client.session.dc_id == 2 and client.session.auth_key.key == bytes(range(256))
    assert client.session.save() == saved
    assert not client.is_connected()


def test_new_session_is_unauthenticated():
    client = _factory().create(None)
    assert client.session.auth_key is None
