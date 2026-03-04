#!/usr/bin/env python3
import subprocess
import sys

result = subprocess.run(
    ["git", "diff", "--cached", "--name-only"],
    capture_output=True,
    text=True
)

if result.returncode != 0:
    sys.exit(1)

files = result.stdout.strip().split("\n")

blocked = False

for file in files:
    if not file or not file.endswith(".py"):
        continue

    if file.startswith("scripts/hooks/"):
        continue

    try:
        with open(file, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
    except FileNotFoundError:
        continue

    if content.strip() == "":
        print(f"Empty file detected: {file}")
        blocked = True
        continue

    lines = content.splitlines()

    for lineno, line in enumerate(lines, start=1):
        if "TODO" in line:
            print(f"TODO found in {file}:{lineno}")
            blocked = True

        if line.endswith(" ") or line.endswith("\t"):
            print(f"Trailing whitespace in {file}:{lineno}")
            blocked = True

if blocked:
    sys.exit(1)

print("Pre-commit check passed")
sys.exit(0)
