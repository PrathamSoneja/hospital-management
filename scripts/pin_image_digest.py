"""
Pin one repository image to a digest in kustomization.yaml files, preserving their comments.

    python scripts/pin_image_digest.py <image-name> sha256:<64 hex> <kustomize-dir> [<kustomize-dir> ...]

Rewrites the `digest:` line of the `images:` entry whose `name:` is <image-name> (the newName stays as it is).
Fails if a directory has no such entry, so a renamed image cannot silently stay unpinned. Used by the
publish-gke-images CI job; `kustomize edit set image` is avoided because it drops the explanatory comments.
"""
import re
import sys
from pathlib import Path

DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


def pin(directory: Path, name: str, digest: str) -> None:
    path = directory / "kustomization.yaml"
    text = path.read_text(encoding="utf-8")
    entry = re.compile(
        rf"(^[ \t]*-[ \t]+name:[ \t]+{re.escape(name)}[ \t]*(?:#[^\n]*)?\n(?:[ \t]+(?!-)[^\n]*\n)*?[ \t]+digest:[ \t]+)sha256:[0-9a-f]{{64}}",
        re.M)
    updated, count = entry.subn(rf"\g<1>{digest}", text)
    if count != 1:
        raise SystemExit(f"{path}: expected exactly one digest-pinned images entry for {name}, found {count}")
    path.write_text(updated, encoding="utf-8", newline="\n")


def main(argv) -> int:
    if len(argv) < 4 or not DIGEST.match(argv[2]):
        raise SystemExit(__doc__)
    for directory in argv[3:]:
        pin(Path(directory), argv[1], argv[2])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
