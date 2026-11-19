"""Generate the cross-language fixtures of Powermug P1 (PM-ACC-002, PM-ACC-003).

The reference is gridloop: its `rfc8785` canonicalization (through `ibtnm.canonical`) and
its signing service (`ibtnm.signing`). Run it with gridloop's virtualenv:

    GRIDLOOP_SRC=~/Dev/gridloop/src ~/Dev/gridloop/.venv/bin/python \
        tests/fixtures/generate.py

Outputs, committed:
- rfc8785/corpus.jsonl: one {"input", "canonical"} object per line;
- signed-plan/public.pem, signed-plan/envelope.json, signed-plan/revisions.json.

The private key is written to a temporary directory and never committed.
"""

from __future__ import annotations

import json
import os
import random
import sys
import tempfile
from pathlib import Path

GRIDLOOP_SRC = os.environ.get("GRIDLOOP_SRC")
if not GRIDLOOP_SRC:
    sys.exit("set GRIDLOOP_SRC to gridloop's src directory")
sys.path.insert(0, str(Path(GRIDLOOP_SRC).expanduser()))

from ibtnm.canonical import artifact_id, canonical_json  # noqa: E402
from ibtnm.signing import generate_keypair, sign_plan, write_signed_plan  # noqa: E402

HERE = Path(__file__).resolve().parent
CORPUS_SIZE = 10_000
SEED = 20261119


def random_key(rng: random.Random) -> str:
    alphabet = ["a", "b", "z", "A", "Z", "_", "é", "中", "😀", "ﬁ", "~", "/", " ", " "]
    length = rng.randint(1, 6)
    return "".join(rng.choice(alphabet) for _ in range(length))


def random_string(rng: random.Random) -> str:
    pool = ["x", "é", "中", "😀", "\"", "\\", "\n", "\t", "/", "\u0001", " ", "ﬁ", " "]
    return "".join(rng.choice(pool) for _ in range(rng.randint(0, 12)))


def random_number(rng: random.Random) -> int | float:
    choice = rng.randrange(8)
    if choice == 0:
        return rng.randint(-(2**53) + 1, 2**53 - 1)
    if choice == 1:
        return rng.choice([0, -0.0, 1.0, 1.5, 100.0, 1e21, 1e-7, 5e-324, 1.7976931348623157e308])
    if choice == 2:
        return rng.uniform(-1e6, 1e6)
    if choice == 3:
        return rng.uniform(-1, 1) * 10 ** rng.randint(-300, 300)
    if choice == 4:
        return round(rng.uniform(0, 1000), rng.randint(0, 6))
    if choice == 5:
        return 0.1 + 0.2
    if choice == 6:
        return rng.randint(0, 10)
    return float(rng.randint(-1000, 1000))


def random_value(rng: random.Random, depth: int):
    kinds = ["num", "str", "bool", "null"]
    if depth < 4:
        kinds += ["obj", "arr", "obj"]
    kind = rng.choice(kinds)
    if kind == "num":
        return random_number(rng)
    if kind == "str":
        return random_string(rng)
    if kind == "bool":
        return rng.choice([True, False])
    if kind == "null":
        return None
    if kind == "arr":
        return [random_value(rng, depth + 1) for _ in range(rng.randint(0, 4))]
    obj = {}
    for _ in range(rng.randint(0, 5)):
        obj[random_key(rng)] = random_value(rng, depth + 1)
    return obj


def write_corpus() -> None:
    rng = random.Random(SEED)
    out_dir = HERE / "rfc8785"
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "corpus.jsonl").open("w", encoding="utf-8", newline="\n") as fh:
        for _ in range(CORPUS_SIZE):
            document = random_value(rng, 0)
            text = json.dumps(document, ensure_ascii=False, allow_nan=False)
            # The canonical form comes from gridloop's rfc8785 through ibtnm.canonical.
            canonical = canonical_json(json.loads(text)).decode("utf-8")
            fh.write(json.dumps({"input": text, "canonical": canonical}, ensure_ascii=False))
            fh.write("\n")


def revision(site: str, rtu: str, common_address: int) -> dict:
    content = {
        "artifact_type": "ibtnm.target-revision",
        "schema_version": "1.1.0",
        "parent": f"revision:sha256:{'0' * 64}",
        "driver": {"name": "powermug", "version": "0.1.0"},
        "target_profile": "powermug@1",
        "site": site,
        "rtu": {"name": rtu, "common_address": common_address, "protocol": "iec104"},
        "mappings": [
            {
                "point_id": f"{site}/TM_001",
                "iidm_id": f"{rtu}.P",
                "type": "TM",
                "data_class": "A",
                "common_address": common_address,
                "ioa": 1001,
                "asdu": "M_ME_NB_1",
                "quantity": "active_power",
                "unit": "MW",
            }
        ],
        "routes": [],
    }
    content["id"] = artifact_id("revision", content)
    return content


def write_signed_plan_fixture() -> None:
    out_dir = HERE / "signed-plan"
    out_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        private_path = Path(tmp) / "private.pem"
        public_path = out_dir / "public.pem"
        generate_keypair(private_path, public_path)

        revisions = [
            revision("S3", "RTU_S3", 3),
            revision("S4", "RTU_S4", 4),
        ]
        plan = {
            "artifact_type": "ibtnm.plan",
            "schema_version": "1.0.0",
            "artifacts": revisions,
            "diff": {"added_sites": ["S3", "S4"], "removed_sites": []},
        }
        plan["id"] = artifact_id("plan", {k: v for k, v in plan.items() if k != "id"})
        envelope = sign_plan(plan, private_path)
        write_signed_plan(out_dir / "envelope.json", envelope)

    (out_dir / "revisions.json").write_text(
        json.dumps({r["site"]: r["id"] for r in revisions}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    write_corpus()
    write_signed_plan_fixture()
    print("fixtures written under", HERE)
