"""What the packaged command prints when something an operator can fix goes wrong.

Every message tested here was already correct before these tests existed. What was
missing is that nobody had stood where the reader stands: run from a clean clone, the
sentence naming ``REPORADAR_POSTGRES_DSN`` and ``.env.example`` arrived on line 104 of
107, under eight source frames and four copies of the author's filesystem path.

The load-bearing test is not the pretty one. It is ``test_designed_exit_codes_survive``:
``typer.Exit`` subclasses ``RuntimeError``, so a handler broad enough to catch operator
errors is, by the type hierarchy, broad enough to swallow every exit code this CLI
designs. It does not, because click's standalone mode absorbs ``Exit`` first -- a fact
about a dependency's internals, which is exactly the kind of thing that changes under an
upgrade without anyone noticing. Pinned here so that it fails the gate instead.
"""

from __future__ import annotations

import sys
from typing import Any

import pytest
import typer

from reporadar import cli


@pytest.fixture
def run_main(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Invoke ``cli.main()`` with a stubbed ``app``, returning the exit code it produced."""

    def _run(raiser: Any) -> int:
        monkeypatch.setattr(cli, "app", raiser)
        try:
            cli.main()
        except SystemExit as exc:  # what a console script actually ends with
            return int(exc.code or 0)
        return 0

    return _run


def _raises(exc: BaseException) -> Any:
    def _app() -> None:
        raise exc

    return _app


@pytest.mark.parametrize(
    ("exc", "expected_fragment"),
    [
        # The three families measured from a clean clone. They share no base below
        # Exception, which is why the handler does not enumerate them.
        (RuntimeError("no database configured: set REPORADAR_POSTGRES_DSN"), "RuntimeError"),
        (ConnectionRefusedError(61, "Connect call failed"), "ConnectionRefusedError"),
        (FileNotFoundError(2, "No such file or directory"), "FileNotFoundError"),
    ],
)
def test_operator_errors_are_one_line_on_stderr(
    run_main: Any,
    capsys: pytest.CaptureFixture[str],
    exc: BaseException,
    expected_fragment: str,
) -> None:
    code = run_main(_raises(exc))
    captured = capsys.readouterr()

    assert code == 1
    assert captured.out == ""  # a failure is not output
    # split("\n"), not splitlines(): the repo-wide ban in test_ndjson_lines.py is
    # about U+2028 in real payloads, and an exemption here would cost a sentence to
    # buy nothing -- this output is ours and is "\n"-terminated.
    lines = [line for line in captured.err.split("\n") if line.strip()]
    assert len(lines) == 1, f"expected one line, got {len(lines)}:\n{captured.err}"
    assert lines[0].startswith("reporadar: ")
    assert expected_fragment in lines[0]
    assert "Traceback" not in captured.err


def test_the_message_itself_is_not_swallowed(
    run_main: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """The presented line must carry the text, not just the type.

    Losing the message would be a worse outcome than the traceback: a hundred ugly lines
    that tell you what to do beat one tidy line that does not.
    """
    run_main(_raises(RuntimeError("set REPORADAR_POSTGRES_DSN (see .env.example)")))
    assert "set REPORADAR_POSTGRES_DSN (see .env.example)" in capsys.readouterr().err


def test_designed_exit_codes_survive(run_main: Any, capsys: pytest.CaptureFixture[str]) -> None:
    """``typer.Exit`` must pass through untouched, and it is a ``RuntimeError`` subclass.

    If this breaks, ``verify``'s unbacked code, ``backfill``'s unconverged ``3`` and
    ``marts-status``'s stale code all silently become ``1``, and every caller branching on
    them reads a distinct outcome as a generic failure.
    """
    assert issubclass(typer.Exit, RuntimeError), "the hazard this test exists for is gone"

    for code in (1, 3, 7):
        with pytest.raises(typer.Exit) as raised:
            run_main(_raises(typer.Exit(code=code)))
        assert raised.value.exit_code == code

    assert capsys.readouterr().err == ""  # and nothing is printed over them


def test_a_successful_run_is_untouched(run_main: Any, capsys: pytest.CaptureFixture[str]) -> None:
    """Control: the handler must not be able to report success as failure."""

    def _ok() -> None:
        sys.stdout.write("done\n")

    assert run_main(_ok) == 0
    captured = capsys.readouterr()
    assert captured.out == "done\n"
    assert captured.err == ""
