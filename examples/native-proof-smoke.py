"""Reproduce a bounded signed-evidence trace with existing native CL and PCGM."""
import json
from hashlib import sha256
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory


def run(tool, *args, wire=b""):
    result = subprocess.run([tool, *args], input=wire, capture_output=True, timeout=30)
    result.check_returncode()
    if result.stderr:
        raise RuntimeError(result.stderr.decode(errors="replace"))
    return result.stdout


def main():
    if len(sys.argv) != 3:
        raise SystemExit("Usage: python native-proof-smoke.py /path/to/cl /path/to/pcgm-alpha")
    cl, pcgm = (Path(p).resolve(strict=True) for p in sys.argv[1:])
    with TemporaryDirectory(prefix="al01-proof-smoke-") as directory:
        work = Path(directory)
        model = """: observed(sym).
: confirmed(sym).
: conclusion(sym).
conclusion($x) <- observed($x) & confirmed($x).
!conclusion($x) <- !confirmed($x).
"""
        formal, lock, api, data, package, goals = [
            work / p for p in ("formal.cl", "formal.lock", "formal.clapi", "data.cl", "test.clpkg", "goals")
        ]
        formal.write_text("module formal.\ninput observed.\ninput confirmed.\nexport conclusion.\n" + model)
        lock.write_bytes(run(cl, "lock", formal))
        api.write_bytes(run(cl, "project-api", formal, lock))
        goals.write_text("conclusion(item)\n")
        source, state = work / "actor.cl", work / "state"
        source.write_text(model + "? conclusion(item).\n")
        run(pcgm, "--init-managed", source, state, wire=b":observe base observed(item)\n")

        def cl_proof(extra):
            data.write_text("& base observed(item).\n" + extra)
            package.write_bytes(run(cl, "project-pack", formal, lock, api, data))
            stamp = sha256(package.read_bytes()).hexdigest()
            return json.loads(run(cl, "package-prove", package, stamp, goals))["results"][0]

        def read(frame, path=state):
            before = path.read_bytes()
            response = json.loads(run(pcgm, "--read-channel", path, wire=("x " + frame + "\n").encode()))
            if not response["ok"] or before != path.read_bytes():
                raise AssertionError(response)
            return response["payload"]

        def sign(proof):
            return proof["status"], proof["positive"]["supported"], proof["negative"]["supported"]

        def compare(expected, extra):
            actual = read("proof conclusion(item)")["results"][0]
            oracle = cl_proof(extra)
            if sign(actual) != expected or sign(oracle) != expected:
                raise AssertionError((sign(actual), sign(oracle), expected))
            print(expected[0])

        actor = read("storage")["state"]["individual"]
        unknown = ("UNKNOWN", False, False)
        compare(unknown, "")
        # Alternate CL packages only: treating each assumption locally must not
        # add an observation or conclusion to the original managed actor.
        original = state.read_bytes()
        if sign(cl_proof("& assumed confirmed(item).\n")) != ("POSITIVE", True, False):
            raise AssertionError("positive counterfactual")
        if sign(cl_proof("& assumed !confirmed(item).\n")) != ("NEGATIVE", False, True):
            raise AssertionError("negative counterfactual")
        if state.read_bytes() != original:
            raise AssertionError("hypothesis changed actor state")
        run(pcgm, "--resume", state, wire=b":observe yes confirmed(item)\n")
        compare(("POSITIVE", True, False), "& yes confirmed(item).\n")
        run(pcgm, "--resume", state, wire=b":observe no !confirmed(item)\n")
        compare(("CONFLICT", True, True), "& yes confirmed(item).\n& no !confirmed(item).\n")
        run(pcgm, "--resume", state, wire=b":withdraw yes\n")
        compare(("NEGATIVE", False, True), "& no !confirmed(item).\n")
        run(pcgm, "--resume", state, wire=b":withdraw no\n")
        compare(unknown, "")
        archive, restored = work / "archive", work / "restored"
        run(pcgm, "--export-complete", state, archive)
        run(pcgm, "--restore-copy", archive, restored)
        if read("storage", restored)["state"]["individual"] != actor:
            raise AssertionError("identity did not survive archive recovery")
        if sign(read("proof conclusion(item)", restored)["results"][0]) != unknown:
            raise AssertionError("restored proof differs")
        print("PASS: signed branches, withdrawal, and same-actor archive recovery")


if __name__ == "__main__":
    main()
