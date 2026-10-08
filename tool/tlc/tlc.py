"""Model-check a TLA+ spec under every combination of its design constants.

A matrix file (JSON, next to the spec) names the spec, the constants that stay
fixed, the constants that vary with their candidate values, the invariants and
temporal properties to check, and the expected verdict for each combination.
Expectations are an ordered rule list: the first rule whose `when` matches a
combination gives the property TLC must report violated, or null for a pass.

`bazel run //tool/tlc -- <matrix.json>...` prints one line per combination.
`--check` additionally fails when any verdict differs from its expectation;
`tlc_matrix_test` runs that under `bazel test`.
"""

import argparse
import concurrent.futures
import dataclasses
import glob
import itertools
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

from python.runfiles import runfiles

TLC_LAUNCHER = "_main/tool/tlc/tlc_java"

INVARIANT_VIOLATED = re.compile(r"Error: Invariant (\w+) is violated")
TEMPORAL_VIOLATED = "Temporal properties were violated"
PASSED = "No error has been found"
DISTINCT_STATES = re.compile(r"(\d+) distinct states found")


@dataclasses.dataclass(frozen=True)
class Verdict:
    # Property TLC reported violated; None when every property held.
    violated: str | None
    # Distinct states explored, as TLC reported them.
    states: int
    # TLC's output tail when it neither passed nor reported a violation.
    error: str | None


@dataclasses.dataclass(frozen=True)
class Row:
    constants: dict
    verdict: Verdict
    expected: str | None

    @property
    def matches(self) -> bool:
        return self.verdict.error is None and self.verdict.violated == self.expected


def load_matrix(path: str) -> dict:
    with open(path) as f:
        matrix = json.load(f)
    matrix["dir"] = os.path.dirname(os.path.abspath(path))
    return matrix


def combinations(matrix: dict) -> list[dict]:
    names = list(matrix["vary"])
    return [dict(zip(names, values)) for values in itertools.product(*(matrix["vary"][n] for n in names))]


def expected_violation(matrix: dict, combination: dict) -> str | None:
    for rule in matrix["expect"]:
        if all(combination[k] == v for k, v in rule["when"].items()):
            return rule["violates"]
    raise ValueError(f"no expectation matches {combination}")


def render_config(matrix: dict, constants: dict, properties: list[str]) -> str:
    lines = [f"SPECIFICATION {matrix['specification']}", "CONSTANTS"]
    lines += [f"    {name} = {value}" for name, value in {**matrix["fixed"], **constants}.items()]
    lines += ["INVARIANTS"] + [f"    {name}" for name in matrix["invariants"]]
    if properties:
        lines += ["PROPERTIES"] + [f"    {name}" for name in properties]
    return "\n".join(lines) + "\n"


def run_tlc(launcher: str, env: dict, matrix: dict, constants: dict, properties: list[str]) -> str:
    tmp = os.environ.get("TEST_TMPDIR")
    with tempfile.TemporaryDirectory(dir=os.path.abspath(tmp) if tmp else None) as work:
        # TLC resolves the config, and any module the spec extends, against the spec's
        # own directory, so the spec's modules are copied next to a generated config.
        for module in glob.glob(os.path.join(matrix["dir"], "*.tla")):
            shutil.copy(module, work)
        with open(os.path.join(work, "model.cfg"), "w") as f:
            f.write(render_config(matrix, constants, properties))
        # A private java.io.tmpdir per run: TLC unpacks its standard modules there, and
        # concurrent runs sharing one directory intermittently fail to parse the spec.
        # -deadlock: a settled system legitimately has no next step.
        result = subprocess.run(
            [launcher, f"--jvm_flag=-Djava.io.tmpdir={work}", "--jvm_flag=-Xmx1g",
             "-deadlock", "-workers", "1", "-metadir", "states", "-config", "model.cfg", matrix["spec"]],
            cwd=work, env=env, capture_output=True, text=True,
        )
        return result.stdout + result.stderr


def classify(output: str, temporal: str | None) -> Verdict:
    states_match = DISTINCT_STATES.search(output)
    states = int(states_match.group(1)) if states_match else 0
    if invariant := INVARIANT_VIOLATED.search(output):
        return Verdict(invariant.group(1), states, None)
    if TEMPORAL_VIOLATED in output:
        return Verdict(temporal or "temporal", states, None)
    if PASSED in output:
        return Verdict(None, states, None)
    return Verdict(None, states, "\n".join(output.strip().splitlines()[-8:]))


def check_combination(launcher: str, env: dict, matrix: dict, constants: dict) -> Verdict:
    properties = matrix.get("properties", [])
    verdict = classify(run_tlc(launcher, env, matrix, constants, properties),
                       properties[0] if len(properties) == 1 else None)
    if verdict.violated != "temporal":
        return verdict
    # TLC does not say which temporal property failed; re-check each alone to name it.
    for prop in properties:
        single = classify(run_tlc(launcher, env, matrix, constants, [prop]), prop)
        if single.violated is not None or single.error is not None:
            return single
    return verdict


def check_matrix(launcher: str, env: dict, matrix: dict) -> list[Row]:
    combos = combinations(matrix)
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as pool:
        verdicts = pool.map(lambda c: check_combination(launcher, env, matrix, c), combos)
        return [Row(c, v, expected_violation(matrix, c)) for c, v in zip(combos, verdicts)]


def format_row(row: Row) -> str:
    constants = "  ".join(f"{k}={v}" for k, v in row.constants.items())
    if row.verdict.error is not None:
        outcome = "TLC error"
    else:
        outcome = f"violates {row.verdict.violated}" if row.verdict.violated else "ok"
    mismatch = "" if row.matches else f"   <-- expected {row.expected or 'ok'}"
    return f"{constants}  =>  {outcome} ({row.verdict.states} states){mismatch}"


def tlc_environment() -> tuple[str, dict]:
    r = runfiles.Create()
    env = {**os.environ, **r.EnvVars()}
    # The java_binary launcher finds its JDK and jars through JAVA_RUNFILES when it
    # runs inside another binary's runfiles tree.
    env["JAVA_RUNFILES"] = env.get("RUNFILES_DIR") or os.path.dirname(r.Rlocation("_main"))
    return r.Rlocation(TLC_LAUNCHER), env


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("matrix", nargs="+", help="matrix JSON files, relative to the workspace or absolute")
    parser.add_argument("--check", action="store_true", help="fail when a verdict differs from its expectation")
    args = parser.parse_args(argv)

    launcher, env = tlc_environment()
    workspace = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")
    failed = False
    for path in args.matrix:
        matrix = load_matrix(os.path.join(workspace, path))
        print(f"== {path}")
        for row in check_matrix(launcher, env, matrix):
            print(format_row(row))
            if row.verdict.error is not None:
                print(row.verdict.error, file=sys.stderr)
            failed |= not row.matches
    return 1 if args.check and failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
