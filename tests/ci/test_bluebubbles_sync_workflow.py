"""Privilege-separation contract for the downstream BlueBubbles sync."""

from pathlib import Path

import yaml


def _workflow():
    path = Path(__file__).parents[2] / ".github/workflows/sync-upstream-bluebubbles.yml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_candidate_verification_has_read_only_contents_and_no_checkout_credentials():
    workflow = _workflow()
    prepare = workflow["jobs"]["prepare"]
    verify = workflow["jobs"]["verify"]

    assert prepare["permissions"] == {"contents": "read"}
    assert any(
        str(step.get("uses", "")).startswith("actions/checkout@")
        and step.get("with", {}).get("persist-credentials") is False
        for step in prepare["steps"]
    )
    assert verify["permissions"] == {"contents": "read"}
    assert any(
        str(step.get("uses", "")).startswith("actions/download-artifact@")
        for step in verify["steps"]
    )
    for step in verify["steps"]:
        if str(step.get("uses", "")).startswith("actions/checkout@"):
            assert step.get("with", {}).get("persist-credentials") is False
    commands = "\n".join(str(step.get("run", "")) for step in verify["steps"])
    assert "git clone candidate-artifact/candidate.bundle verified-tree" in commands
    assert 'test "$(git -C verified-tree rev-parse HEAD)" = "$candidate_sha"' in commands


def test_only_minimal_publish_job_can_write_contents_and_it_uses_verified_artifact():
    workflow = _workflow()
    writers = [
        name
        for name, job in workflow["jobs"].items()
        if job.get("permissions", {}).get("contents") == "write"
    ]

    assert writers == ["publish"]
    publish = workflow["jobs"]["publish"]
    assert set(publish["needs"]) == {"prepare", "verify"}
    assert any(
        str(step.get("uses", "")).startswith("actions/download-artifact@")
        for step in publish["steps"]
    )
    assert not any(
        str(step.get("uses", "")).startswith("actions/checkout@")
        and step.get("with", {}).get("persist-credentials", True)
        for step in publish["steps"]
    )
    commands = "\n".join(str(step.get("run", "")) for step in publish["steps"])
    assert 'test "$(git -C publish-tree rev-parse HEAD)" = "$candidate_sha"' in commands
    assert '--force-with-lease="refs/heads/main:$start_sha"' in commands
    assert '"$candidate_sha:refs/heads/main"' in commands
