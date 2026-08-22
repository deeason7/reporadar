"""The documentation must not describe behaviour the code does not have.

Prose is where a changed constant survives. Code refers to a value through a
symbol: rename it and the type checker objects, misuse it and a test goes red.
Documentation spells the value out as a literal, and nothing renames a literal in
prose, nothing type-checks it, and nothing else reads it — so the docs are exactly
where a superseded exit code lives on, and they are also the first thing a
stranger reads.

So the prose is checked against the source it describes, and **the source stays
the only copy of the truth**: exit codes are read out of ``cli.py`` with the AST
and resolved through the ``*_EXIT_CODE`` constants, never from a table kept
alongside. A table here would just be a third place the fact lives, rotting the
same way the documentation does.

``README.md`` is checked in full. ``CHANGELOG.md`` is checked only *above* its
first released heading: released entries are frozen history and are supposed to
name values that have since changed, while ``[Unreleased]`` describes the code as
it stands and is held to the same standard as the README.

Every check here has a companion that plants the defect it looks for and asserts
it is reported. A check nobody has watched fail is not a check.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
CLI = REPO / "src/reporadar/cli.py"
SRC = REPO / "src"

#: Tokens meaning "a number nobody has measured yet". An explicit list rather
#: than a bracket pattern on purpose: the README's architecture diagram uses
#: `[Kafka]`, `[Parquet lake]` and seven more as box labels, so a bracket check
#: would flag nine correct lines the first time it ran.
PLACEHOLDERS = ("[TBD]", "[N]", "[NN]", "[X]", "[XX]", "[?]", "[x%]", "[N%]", "[measure]")

#: "`verify` exits `3`" / "marts-status exits 3 for stale"
CLAIM = re.compile(r"exits?\s+`?(\d+)`?")
#: A backticked command name: `marts-status`, `repair-lake`, `verify`
BACKTICKED = re.compile(r"`([a-z][a-z-]*)`")
#: An invocation in prose or a shell block: "reporadar marts-status"
INVOCATION = re.compile(r"reporadar ([a-z][a-z-]+)")


@dataclass(frozen=True)
class Claim:
    """One "<command> exits N" sentence, attributed to the command it describes."""

    document: str
    line: int
    command: str
    code: int
    text: str


def _exit_code_constants() -> dict[str, int]:
    """Every ``NAME_EXIT_CODE: Final = N`` in the package, by name."""
    found: dict[str, int] = {}
    for path in sorted(SRC.rglob("*.py")):
        for node in ast.parse(path.read_text(encoding="utf-8")).body:
            if (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.target.id.endswith("EXIT_CODE")
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, int)
            ):
                found[node.target.id] = node.value.value
    return found


def _command_name(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str | None:
    """The name a user types, taking the decorator's override when there is one."""
    for decorator in node.decorator_list:
        if not isinstance(decorator, ast.Call):
            # Bare @app.command with no name= override.
            if isinstance(decorator, ast.Attribute) and decorator.attr == "command":
                return node.name.replace("_", "-")
            continue
        if isinstance(decorator.func, ast.Attribute) and decorator.func.attr == "command":
            for keyword in decorator.keywords:
                if keyword.arg == "name" and isinstance(keyword.value, ast.Constant):
                    return str(keyword.value.value)
            return node.name.replace("_", "-")
    return None


def exit_codes_from_code() -> dict[str, set[int]]:
    """Which exit codes each command can raise, read from the source.

    Keys are the names a user *invokes* (``marts-status``), not the names the
    functions are spelled with (``marts_status``), because the documentation
    names the former and this has to compare like with like.

    An exit code that cannot be resolved raises rather than being skipped: it
    would make every claim about that command unverifiable, and a silent skip
    reports "nothing wrong" for a command nothing checked.
    """
    constants = _exit_code_constants()
    commands: dict[str, set[int]] = {}
    for node in ast.parse(CLI.read_text(encoding="utf-8")).body:
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        name = _command_name(node)
        if name is None:
            continue
        codes: set[int] = set()
        for inner in ast.walk(node):
            if not (
                isinstance(inner, ast.Call)
                and isinstance(inner.func, ast.Attribute)
                and inner.func.attr == "Exit"
            ):
                continue
            for keyword in inner.keywords:
                if keyword.arg != "code":
                    continue
                value = keyword.value
                if isinstance(value, ast.Constant) and isinstance(value.value, int):
                    codes.add(value.value)
                elif isinstance(value, ast.Name) and value.id in constants:
                    codes.add(constants[value.id])
                else:
                    raise AssertionError(
                        f"cannot resolve the exit code at {CLI.name}:{value.lineno} "
                        f"({ast.unparse(value)}); teach this test to read it rather than "
                        "letting the command go unchecked"
                    )
        commands[name] = codes
    return commands


def scanned_documents() -> list[tuple[str, str]]:
    """The prose held to current behaviour, as ``(label, text)`` pairs."""
    changelog = (REPO / "CHANGELOG.md").read_text(encoding="utf-8")
    # Split off everything from the first *released* heading downward. The
    # unreleased section is the second element because the file opens with a
    # preamble before "## [Unreleased]".
    parts = changelog.split("\n## [", 2)
    unreleased = "## [" + parts[1] if len(parts) > 1 else changelog
    return [
        ("README.md", (REPO / "README.md").read_text(encoding="utf-8")),
        ("CHANGELOG.md", unreleased),
    ]


def claims_in(document: str, text: str, known: set[str]) -> list[Claim]:
    """Every "<command> exits N" in one document, attributed to a command.

    Attribution is to the **nearest preceding backticked command name**, which is
    how the prose actually reads: "``verify`` compares … It exits ``3``".
    """
    claims: list[Claim] = []
    for match in CLAIM.finditer(text):
        before = text[: match.start()]
        names = [name for name in BACKTICKED.findall(before) if name in known]
        if not names:
            continue
        context = text[max(0, match.start() - 60) : match.end() + 20].replace("\n", " ")
        claims.append(
            Claim(document, before.count("\n") + 1, names[-1], int(match.group(1)), context.strip())
        )
    return claims


def findings_for(documents: list[tuple[str, str]], commands: dict[str, set[int]]) -> list[str]:
    """Every claim in these documents the code does not support.

    Takes the documents as text rather than reading them, so a companion test can
    plant a defect without writing to a real file — a check that mutates the
    working tree to prove itself is one that can corrupt whatever is prepared
    there.
    """
    findings: list[str] = []
    for label, text in documents:
        for claim in claims_in(label, text, set(commands)):
            real = commands.get(claim.command, set())
            # 0 is always defensible: every command can succeed.
            if claim.code != 0 and claim.code not in real:
                findings.append(
                    f"{label}:{claim.line} says `{claim.command}` exits {claim.code}, but the "
                    f"code can only exit {sorted(real) or 'nothing non-zero'} — …{claim.text}…"
                )
        for match in INVOCATION.finditer(text):
            if match.group(1) not in commands:
                findings.append(f"{label} names `reporadar {match.group(1)}`, not a command.")
        for token in PLACEHOLDERS:
            if token in text:
                findings.append(f"{label} still contains the placeholder {token}.")
    return findings


def test_the_documentation_describes_the_exit_codes_the_code_has() -> None:
    findings = findings_for(scanned_documents(), exit_codes_from_code())
    assert findings == [], "\n".join(findings)


def test_exit_code_claims_are_actually_being_found() -> None:
    # Without this, the check above passes just as cleanly when the pattern stops
    # matching anything at all: a check that silently finds nothing to check is
    # indistinguishable from a passing one.
    commands = exit_codes_from_code()
    claims = [
        c for label, text in scanned_documents() for c in claims_in(label, text, set(commands))
    ]
    assert claims, "no exit-code claims were found in the documentation; the pattern has rotted"


def test_the_commands_the_documentation_names_all_exist() -> None:
    commands = exit_codes_from_code()
    assert "serve" in commands and "verify" in commands  # the reader found real commands
    named = {
        match.group(1) for _, text in scanned_documents() for match in INVOCATION.finditer(text)
    }
    assert named, "the documentation invokes no commands at all; the pattern has rotted"
    assert named <= set(commands), f"documented but not registered: {sorted(named - set(commands))}"


def test_a_contradicted_exit_code_is_reported() -> None:
    # The negative control for the first test. 99 is a code no command raises, so
    # a clean run on planted text would mean the comparison never happens.
    commands = exit_codes_from_code()
    planted = [("planted", "`verify` compares the record with the store. It exits `99`.")]
    findings = findings_for(planted, commands)
    assert len(findings) == 1
    assert "`verify` exits 99" in findings[0]


def test_an_invented_command_is_reported() -> None:
    commands = exit_codes_from_code()
    planted = [("planted", "Run `reporadar teleport` to move the lake.")]
    findings = findings_for(planted, commands)
    assert any("not a command" in finding for finding in findings)


def test_a_placeholder_is_reported() -> None:
    commands = exit_codes_from_code()
    planted = [("planted", "The capture ratio is [TBD] per cent.")]
    findings = findings_for(planted, commands)
    assert any("[TBD]" in finding for finding in findings)


def test_a_bracketed_diagram_label_is_not_reported() -> None:
    # The README's architecture diagram is full of `[Kafka]`-style box labels, and
    # a check that flagged those would fire on the content it exists to protect.
    commands = exit_codes_from_code()
    planted = [("planted", "[GH Archive hourly] -> [Parquet lake] -> [Kafka] -> [TimescaleDB]")]
    assert findings_for(planted, commands) == []


# --- The gate figures in the README ------------------------------------------
#
# The README states the size of the gate — file counts, test count, lines of
# source and test. Those are the most drift-prone claims in the whole document:
# every feature moves all four at once, and nothing was reading them. They went
# stale by 8 source files, 109 tests and 1,950 lines before anyone noticed, and
# the reason is structural rather than careless — a number in prose has no
# referent that anything dereferences.
#
# So they are re-derived here from the tree, and the directories are read out of
# `pyproject.toml`'s own mypy scope rather than listed again. A list here would be
# a second copy of "what mypy checks", which is precisely the rot this file exists
# to prevent.

README_TEXT = (REPO / "README.md").read_text(encoding="utf-8")


def _mypy_dirs() -> list[str]:
    """The directories mypy checks, read from the config that makes it true."""
    config = (REPO / "pyproject.toml").read_text(encoding="utf-8")
    block = re.search(r"\[tool\.mypy\](.*?)(?=\n\[|\Z)", config, re.DOTALL)
    assert block, "no [tool.mypy] section — the derivation below has no source"
    files = re.search(r"files\s*=\s*\[([^\]]+)\]", block.group(1))
    assert files, "[tool.mypy] has no `files` — cannot derive what is checked"
    return re.findall(r'"([^"]+)"', files.group(1))


def _py_files(*dirs: str) -> list[Path]:
    return sorted(p for d in dirs for p in (REPO / d).rglob("*.py"))


def _line_count(*dirs: str) -> int:
    r"""Count newlines, not ``splitlines()`` — and the difference is not pedantry.

    ``splitlines()`` also breaks on U+2028 and seven other characters that are
    legal *unescaped* inside a Python string literal, so one source file carrying
    one would be counted as two lines. That is the same defect
    ``test_ndjson_lines.py`` guards against for records, and that guard caught
    this function when it was first written the obvious way — in a file whose own
    docstring is about prose drifting from the code it describes.

    ``count("\n")`` is also exactly what ``wc -l`` reports, which is what a reader
    checking this figure by hand will actually run.
    """
    return sum(p.read_text(encoding="utf-8").count("\n") for p in _py_files(*dirs))


def _claimed(pattern: str) -> int:
    """One figure out of the README, failing loudly if the sentence was reworded."""
    match = re.search(pattern, README_TEXT)
    assert match, (
        f"the README no longer contains a figure matching {pattern!r}. If the "
        "sentence was reworded, update this pattern — a check that silently stops "
        "matching is worse than no check, because it keeps reporting success."
    )
    return int(match.group(1).replace(",", ""))


def test_the_readme_source_file_count_matches_what_mypy_checks() -> None:
    assert _claimed(r"`mypy --strict` over ([\d,]+) source files") == len(_py_files(*_mypy_dirs()))


def test_the_readme_line_counts_match_the_tree() -> None:
    assert _claimed(r"([\d,]+) lines of source against") == _line_count("src")
    assert _claimed(r"lines of source against ([\d,]+) lines of test") == _line_count("tests")


def test_the_readme_test_count_matches_the_suite(request: pytest.FixtureRequest) -> None:
    """Counted from this run's own collection, which is the only number that
    cannot disagree with the suite.

    Skipped under a filtered run (`-k`, a single file), because there the
    collection is deliberately partial and asserting against it would fail for a
    reason that has nothing to do with the README.
    """
    collected = len(request.session.items)
    if collected < 100:
        pytest.skip(f"filtered run ({collected} collected) — not the whole suite")
    # One test in the suite is skipped, and the README says so separately; the
    # claim is about how many pass, so the skipped one is not in the figure.
    assert _claimed(r"([\d,]+) passing tests with one skipped") == collected - 1


def test_a_stale_figure_is_actually_reported() -> None:
    """The control. Every check above compares a number parsed from prose against
    one derived from the tree — and if the parse quietly returned the derived
    value, or the comparison were inverted, all three would pass forever."""
    stale = README_TEXT.replace("lines of source against", "999 lines of source against")
    match = re.search(r"([\d,]+) lines of source against", stale)
    assert match and int(match.group(1)) == 999
    assert int(match.group(1)) != _line_count("src"), "control: a planted wrong figure matched"
