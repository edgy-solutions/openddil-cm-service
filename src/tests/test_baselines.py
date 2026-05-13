"""Tests for the baseline loader. No Faust, no Kafka."""
from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

# Generated protobuf bindings live outside the service tree
sys.path.insert(0, str(Path(__file__).resolve().parents[3]
                        / "openddil-contracts" / "gen" / "python"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from baselines.loader import BaselineLoadError, BaselineRegistry, make_registry


# Path to real Phase 1 baselines so we test against authoritative shapes
REAL_BASELINES_DIR = (
    Path(__file__).resolve().parents[3]
    / "openddil-contracts" / "baselines"
)


def test_load_real_phase1_baselines():
    """The three baselines shipped in Phase 1 must all load cleanly."""
    reg = make_registry(REAL_BASELINES_DIR)
    variants = reg.all_variants()
    assert "M1A2-SEPv3" in variants
    assert "UH-60M" in variants
    assert "F-35A-Block4" in variants

    m1 = reg.get("M1A2-SEPv3")
    assert m1 is not None
    assert m1.baseline_id == "M1A2-SEPv3-Baseline-2024.2"
    assert len(m1.authorized_cis) == 5
    assert len(m1.required_mods) == 2

    # Check ModType / ComplianceCategory enums parsed from string form
    mwo_2024 = next(m for m in m1.required_mods if m.mod_id == "MWO-2024-117")
    from openddil.configuration.v1 import configuration_baseline_pb2 as cb
    assert mwo_2024.type == cb.MOD_TYPE_MWO
    assert mwo_2024.category == cb.COMPLIANCE_SAFETY_OF_FLIGHT


def test_missing_directory_raises(tmp_path):
    reg = BaselineRegistry(tmp_path / "does-not-exist")
    with pytest.raises(BaselineLoadError, match="does not exist"):
        reg.load()


def test_empty_directory_loads_zero(tmp_path):
    reg = make_registry(tmp_path)
    assert reg.all_variants() == []


def test_malformed_yaml_raises(tmp_path):
    (tmp_path / "broken.yaml").write_text("this is not: : a valid: yaml:")
    with pytest.raises(BaselineLoadError):
        make_registry(tmp_path)


def test_missing_platform_variant_raises(tmp_path):
    (tmp_path / "nameless.yaml").write_text(textwrap.dedent("""
        baseline_id: "test"
        version: "1.0"
        effective_from: "2024-01-01T00:00:00Z"
        authorized_cis: []
        required_mods: []
    """))
    with pytest.raises(BaselineLoadError, match="no platform_variant"):
        make_registry(tmp_path)


def test_unknown_field_rejected(tmp_path):
    """ParseDict with ignore_unknown_fields=False catches typos in YAML keys."""
    (tmp_path / "typo.yaml").write_text(textwrap.dedent("""
        baseline_id: "test"
        platform_variant: "TEST"
        version: "1.0"
        effective_from: "2024-01-01T00:00:00Z"
        bogus_field: 42
        authorized_cis: []
        required_mods: []
    """))
    with pytest.raises(BaselineLoadError):
        make_registry(tmp_path)


def test_duplicate_platform_variant_raises(tmp_path):
    common = textwrap.dedent("""
        platform_variant: "DUPLICATE"
        version: "1.0"
        effective_from: "2024-01-01T00:00:00Z"
        authorized_cis: []
        required_mods: []
    """)
    (tmp_path / "a.yaml").write_text('baseline_id: "A"\n' + common)
    (tmp_path / "b.yaml").write_text('baseline_id: "B"\n' + common)
    with pytest.raises(BaselineLoadError, match="Duplicate"):
        make_registry(tmp_path)


def test_reload_picks_up_new_baseline(tmp_path):
    """SIGHUP-style reload must observe added files."""
    (tmp_path / "v1.yaml").write_text(textwrap.dedent("""
        baseline_id: "V1"
        platform_variant: "PLATFORM-V1"
        version: "1.0"
        effective_from: "2024-01-01T00:00:00Z"
        authorized_cis: []
        required_mods: []
    """))
    reg = make_registry(tmp_path)
    assert reg.all_variants() == ["PLATFORM-V1"]

    (tmp_path / "v2.yaml").write_text(textwrap.dedent("""
        baseline_id: "V2"
        platform_variant: "PLATFORM-V2"
        version: "2.0"
        effective_from: "2025-01-01T00:00:00Z"
        authorized_cis: []
        required_mods: []
    """))
    reg.reload()
    assert sorted(reg.all_variants()) == ["PLATFORM-V1", "PLATFORM-V2"]


def test_reload_picks_up_removed_baseline(tmp_path):
    f1 = tmp_path / "v1.yaml"
    f1.write_text(textwrap.dedent("""
        baseline_id: "V1"
        platform_variant: "PLATFORM-V1"
        version: "1.0"
        effective_from: "2024-01-01T00:00:00Z"
        authorized_cis: []
        required_mods: []
    """))
    reg = make_registry(tmp_path)
    assert reg.all_variants() == ["PLATFORM-V1"]
    f1.unlink()
    reg.reload()
    assert reg.all_variants() == []
