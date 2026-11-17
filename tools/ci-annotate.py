"""Turns failed and hanging tests in a `dotnet test` log into GitHub Actions annotations.

    python3 tools/ci-annotate.py test-output.txt

Annotations show up on the run's summary page (and in the public API), so a failure can be read
without digging through the full log.
"""
import re
import sys


def annotate(title, text):
    message = text.replace("%", "%25").replace("\r", "").replace("\n", "%0A")
    print(f"::error title={title}::{message[:3500]}")


def main():
    lines = open(sys.argv[1], encoding="utf-8", errors="replace").read().splitlines()
    reported = 0
    for i, line in enumerate(lines):
        failed = re.match(r"\s*Failed (NetSync\.Tests\.\S+)", line)
        if failed:
            details = []
            for follow in lines[i + 1:i + 14]:
                if re.match(r"\s*(Passed|Failed|Skipped) NetSync\.", follow):
                    break
                details.append(follow.strip())
            annotate("Failed " + failed.group(1), "\n".join(details))
            reported += 1
        if "The test running when the crash occurred" in line:
            running = []
            for follow in lines[i + 1:i + 20]:
                if not follow.strip().startswith("NetSync."):
                    break
                running.append(follow.strip())
            context = " ".join(l.strip() for l in lines[max(0, i - 12):i] if "inactivity" in l or "crashed" in l)
            annotate("Hang or crash", (context + "\nRunning at that moment:\n" + "\n".join(running)).strip())
            reported += 1
    if reported == 0:
        annotate("Tests failed", "no failed test found in the log; see the step output")


if __name__ == "__main__":
    main()
