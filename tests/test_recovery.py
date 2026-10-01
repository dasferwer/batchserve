import subprocess

import pytest

from scripts import staging_recovery


def test_recovery_waits_until_preregistered_object_is_available(monkeypatch):
    attempts = []

    def delayed_object(*args):
        attempts.append(args[-1])
        if len(attempts) < 3:
            raise subprocess.CalledProcessError(75, args)

    monkeypatch.setattr(staging_recovery, "docker", delayed_object)
    monkeypatch.setattr(staging_recovery.time, "sleep", lambda _: None)
    staging_recovery.wait_object_exists("inputs/stage/0")
    assert attempts == ["inputs/stage/0"] * 3


def test_recovery_does_not_hide_unexpected_head_failure(monkeypatch):
    def forbidden(*args):
        raise subprocess.CalledProcessError(1, args, stderr=b"AccessDenied")

    monkeypatch.setattr(staging_recovery, "docker", forbidden)
    with pytest.raises(subprocess.CalledProcessError, match="exit status 1"):
        staging_recovery.wait_object_exists("inputs/stage/0")
