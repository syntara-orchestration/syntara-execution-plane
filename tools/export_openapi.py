"""Export the Execution Plane OpenAPI specification without starting the server.

Instantiates the FastAPI application from api/main.py and writes the
OpenAPI spec to a file. No database or external services are required.

Usage:
    make openapi
    uv run python -m tools.export_openapi.py [--output PATH] [--format {json,yaml}]

The committed openapi.yaml is kept in sync by the `openapi-drift` CI job.
Run this script locally and commit the result whenever the API changes.
"""

import argparse
import json
import sys
from pathlib import Path

import yaml

from execution_plane.api.main import create_app


def main() -> None:
    """Export the service OpenAPI specification."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "openapi.yaml",
        help="Output path (default: openapi.yaml at repo root)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "yaml"],
        default="yaml",
        help="Output format (default: yaml)",
    )
    args = parser.parse_args()

    app = create_app()
    spec = app.openapi()

    output: Path = args.output
    output.parent.mkdir(parents=True, exist_ok=True)

    if args.format == "json":
        output.write_text(json.dumps(spec, indent=2) + "\n")
    else:
        output.write_text(yaml.dump(spec, default_flow_style=False, allow_unicode=True, sort_keys=False))

    print(f"Written to {output}", file=sys.stderr)


if __name__ == "__main__":
    main()
