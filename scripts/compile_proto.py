"""
Compiles Protocol Buffer (.proto) definitions into Python gRPC code in shared/generated/.
Ensures imports work reliably across tests and container packages.
"""
import os
import re
import shutil
import sys
from pathlib import Path
from grpc_tools import protoc

PROJECT_ROOT = Path(__file__).parent.parent.resolve()
PROTO_DIR = PROJECT_ROOT / "contracts" / "proto"
OUTPUT_DIR = PROJECT_ROOT / "shared" / "generated"

def compile_protos():
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "__init__.py").touch(exist_ok=True)
    
    proto_files = list(PROTO_DIR.glob("*.proto"))
    if not proto_files:
        print(f"No proto files found in {PROTO_DIR}")
        sys.exit(1)

    print(f"Compiling {len(proto_files)} proto files to {OUTPUT_DIR}...")
    
    args = [
        "protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={OUTPUT_DIR}",
        f"--grpc_python_out={OUTPUT_DIR}",
    ] + [str(p.name) for p in proto_files]
    
    # Run protoc with cwd=PROTO_DIR so relative imports resolve directly
    old_cwd = os.getcwd()
    os.chdir(str(PROTO_DIR))
    try:
        exit_code = protoc.main(args)
    finally:
        os.chdir(old_cwd)
        
    if exit_code != 0:
        print(f"protoc failed with exit code {exit_code}")
        sys.exit(exit_code)
        
    # Post-process generated files to fix imports so they work both locally and in packages
    for generated_file in OUTPUT_DIR.glob("*_pb2*.py"):
        content = generated_file.read_text(encoding="utf-8")
        # Replace 'import common_pb2 as common__pb2' or 'import <name>_pb2 as ...'
        # with 'from shared.generated import <name>_pb2 as ...'
        content = re.sub(
            r'^(import\s+([a-zA-Z0-9_]+_pb2)\s+as\s+)',
            r'from shared.generated \1',
            content,
            flags=re.MULTILINE
        )
        # Also relative imports inside grpc files: 'import foo_pb2 as foo__pb2'
        content = re.sub(
            r'^(import\s+([a-zA-Z0-9_]+_pb2))',
            r'from shared.generated \1',
            content,
            flags=re.MULTILINE
        )
        generated_file.write_text(content, encoding="utf-8")

    print(f"Successfully compiled and patched protos in {OUTPUT_DIR}")

if __name__ == "__main__":
    compile_protos()
