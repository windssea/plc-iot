"""Run the example conformance corpus or validate one bounded payload."""
import argparse
from dataclasses import asdict
import json
from pathlib import Path

from tools.contract_validation import CONTRACT_DIR, topic_rules, validate_message


def read_payload(path: Path, kind: str) -> bytes:
    # Read at most limit+1 even for a huge file; decoder rejects the extra byte.
    with path.open("rb") as stream:
        return stream.read(topic_rules()[kind]["maxPayloadBytes"] + 1)


def validate_examples() -> int:
    root = CONTRACT_DIR / "examples"
    cases = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    failures = 0
    for case in cases:
        issues = validate_message(case["kind"], read_payload(root / case["file"], case["kind"]))
        codes = {issue.code for issue in issues}
        passed = (not issues) == case["valid"]
        if not case["valid"]:
            passed = passed and case["code"] in codes
        failures += not passed
        print(f"{'PASS' if passed else 'FAIL'} {case['file']}")
    print(f"{len(cases)} examples, {failures} failures")
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=sorted(topic_rules()))
    parser.add_argument("--file", type=Path)
    parser.add_argument("--gateway", help="Trusted PLC identity expected for the topic/session")
    args = parser.parse_args()
    if bool(args.kind) != bool(args.file) or (args.gateway is not None and args.kind is None):
        parser.error("--kind and --file must be used together; --gateway requires them")
    try:
        if args.kind is None:
            return validate_examples()
        issues = validate_message(args.kind, read_payload(args.file, args.kind),
                                  expected_gateway_id=args.gateway)
    except OSError:
        print(json.dumps({"valid": False, "error": "INPUT_READ_ERROR"}))
        return 2
    print(json.dumps({"valid": not issues, "issues": [asdict(i) for i in issues]}, ensure_ascii=True))
    return 1 if issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
