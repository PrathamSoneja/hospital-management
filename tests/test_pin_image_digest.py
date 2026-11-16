"""CI digest pinning keeps comments and refuses to leave an image unpinned."""
import pytest

from scripts.pin_image_digest import pin

KUSTOMIZATION = """# keep this comment
images:
  - name: bed-allocation
    newName: asia-south1-docker.pkg.dev/p/r/bed-allocation
    digest: sha256:{a}
  - name: triage   # triage image
    newName: asia-south1-docker.pkg.dev/p/r/triage
    digest: sha256:{b}
"""


def test_pins_only_the_named_image_and_keeps_comments(tmp_path):
    (tmp_path / "kustomization.yaml").write_text(KUSTOMIZATION.format(a="1" * 64, b="2" * 64))
    pin(tmp_path, "triage", "sha256:" + "f" * 64)
    text = (tmp_path / "kustomization.yaml").read_text()
    assert "# keep this comment" in text and "# triage image" in text
    assert "sha256:" + "1" * 64 in text and "sha256:" + "f" * 64 in text and "2" * 64 not in text


def test_missing_image_entry_fails(tmp_path):
    (tmp_path / "kustomization.yaml").write_text(KUSTOMIZATION.format(a="1" * 64, b="2" * 64))
    with pytest.raises(SystemExit, match="demo-ui"):
        pin(tmp_path, "demo-ui", "sha256:" + "f" * 64)
