"""Run isolated fault experiments and verify saved evidence reports."""

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from src.faults.contracts import SCENARIOS, ScenarioReport


def build_cli_parser() -> argparse.ArgumentParser:
    """Keep help/list independent of brokers and database connections."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="List the ten fault scenarios")
    run = commands.add_parser("run", help="Execute an isolated experiment")
    run.add_argument("scenario_id", choices=[*SCENARIOS, "all"])
    run.add_argument("--output-dir", type=Path, default=Path("data/fault_runs"))
    verify = commands.add_parser("assert", help="Check a saved report (does not rerun a fault)")
    verify.add_argument("report", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Return 0 for success, 1 for failed checks, 2 for invalid input."""
    args = build_cli_parser().parse_args(argv)
    if args.command == "list":
        for name, description in SCENARIOS.items():
            print(f"{name}: {description}")
        return 0
    try:
        if args.command == "assert":
            data = json.loads(args.report.read_text(encoding="utf-8"))
            data.pop("passed", None)  # Never trust the serialized summary flag.
            report = ScenarioReport(**data)
            print(json.dumps(report.to_dict(), indent=2))
            return 0 if report.passed else 1
        from src.faults.harness import run_scenario

        names = list(SCENARIOS) if args.scenario_id == "all" else [args.scenario_id]
        success = True
        for name in names:
            report, path = run_scenario(name, output_dir=args.output_dir)
            print(f"{'PASS' if report.passed else 'FAIL'} {name}: {path}")
            success = success and report.passed
        return 0 if success else 1
    except (OSError, ValueError, TypeError, AttributeError) as exc:
        print(f"Invalid request/report: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
