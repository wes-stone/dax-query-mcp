"""Prepare a unique PyPI release version for automated publishing."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10/3.11 fallback
    import tomli as tomllib  # type: ignore[no-redef]


SEMVER_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
PROJECT_HEADER_RE = re.compile(r"^\s*\[project]\s*(?:#.*)?$")
TABLE_HEADER_RE = re.compile(r"^\s*\[[^\]]+]\s*(?:#.*)?$")
PROJECT_VERSION_RE = re.compile(
    r'^(?P<prefix>\s*version\s*=\s*)"(?P<version>[^"]+)"'
    r"(?P<suffix>[^\r\n]*)(?P<newline>\r?\n?)$"
)


@dataclass(frozen=True, order=True)
class SimpleVersion:
    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, value: str) -> "SimpleVersion":
        match = SEMVER_RE.fullmatch(value)
        if match is None:
            raise ValueError(f"Expected a simple major.minor.patch version, got {value!r}")
        return cls(*(int(part) for part in match.groups()))

    def bump_patch(self) -> "SimpleVersion":
        return SimpleVersion(self.major, self.minor, self.patch + 1)

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


@dataclass(frozen=True)
class ProjectMetadata:
    name: str
    version: str


def read_project_metadata(pyproject_path: Path) -> ProjectMetadata:
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    project = pyproject.get("project")
    if not isinstance(project, dict):
        raise ValueError(f"{pyproject_path} does not contain a [project] table")

    name = project.get("name")
    version = project.get("version")
    if not isinstance(name, str) or not name:
        raise ValueError(f"{pyproject_path} does not define project.name")
    if not isinstance(version, str) or not version:
        raise ValueError(f"{pyproject_path} does not define project.version")

    return ProjectMetadata(name=name, version=version)


def fetch_pypi_version(package_name: str) -> str | None:
    url = f"https://pypi.org/pypi/{package_name}/json"
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise

    version = payload.get("info", {}).get("version")
    if version is not None and not isinstance(version, str):
        raise ValueError(f"PyPI returned a non-string version for {package_name!r}")
    return version


def choose_release_version(local_version: str, pypi_version: str | None) -> str:
    local = SimpleVersion.parse(local_version)
    if pypi_version is None:
        return str(local)

    remote = SimpleVersion.parse(pypi_version)
    if local > remote:
        return str(local)
    return str(remote.bump_patch())


def replace_project_version(pyproject_text: str, new_version: str) -> str:
    lines = pyproject_text.splitlines(keepends=True)
    in_project = False

    for index, line in enumerate(lines):
        if TABLE_HEADER_RE.match(line):
            in_project = PROJECT_HEADER_RE.match(line) is not None
            continue

        if not in_project:
            continue

        match = PROJECT_VERSION_RE.match(line)
        if match is not None:
            lines[index] = (
                f'{match.group("prefix")}"{new_version}"'
                f'{match.group("suffix")}{match.group("newline")}'
            )
            return "".join(lines)

    raise ValueError("Could not find project.version in [project]")


def write_github_outputs(output_file: TextIO, outputs: dict[str, str]) -> None:
    for key, value in outputs.items():
        print(f"{key}={value}", file=output_file)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-file",
        type=Path,
        default=Path("pyproject.toml"),
        help="Path to pyproject.toml.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write the selected release version back to pyproject.toml.",
    )
    parser.add_argument(
        "--github-output",
        type=Path,
        default=Path(os.environ["GITHUB_OUTPUT"]) if "GITHUB_OUTPUT" in os.environ else None,
        help="Optional GitHub Actions output file.",
    )
    args = parser.parse_args(argv)

    metadata = read_project_metadata(args.project_file)
    pypi_version = fetch_pypi_version(metadata.name)
    release_version = choose_release_version(metadata.version, pypi_version)

    if args.write:
        original_text = args.project_file.read_text(encoding="utf-8")
        args.project_file.write_text(
            replace_project_version(original_text, release_version),
            encoding="utf-8",
        )

    outputs = {
        "package_name": metadata.name,
        "local_version": metadata.version,
        "pypi_version": pypi_version or "",
        "release_version": release_version,
    }

    write_github_outputs(sys.stdout, outputs)
    if args.github_output is not None:
        with args.github_output.open("a", encoding="utf-8") as output_file:
            write_github_outputs(output_file, outputs)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
