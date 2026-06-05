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

from baselines.loader import (
    BaselineLoadError,
    BaselineRegistry,
    make_registry,
    parse_dirs_env,
)


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


# ---------------------------------------------------------------------------
# Multi-directory layering — added 2026-06 for customer-overlay baselines
# ---------------------------------------------------------------------------

def _write_baseline(dir_path, baseline_id, variant):
    """Helper: write a minimal valid baseline YAML."""
    (dir_path / f"{variant}.yaml").write_text(textwrap.dedent(f"""
        baseline_id: "{baseline_id}"
        platform_variant: "{variant}"
        version: "1.0"
        effective_from: "2024-01-01T00:00:00Z"
        authorized_cis: []
        required_mods: []
    """))


def test_parse_dirs_env_handles_empty_and_unset():
    assert parse_dirs_env(None) == []
    assert parse_dirs_env("") == []
    assert parse_dirs_env("   ") == []


def test_parse_dirs_env_splits_on_colon_drops_whitespace():
    assert parse_dirs_env("/a:/b:/c") == ["/a", "/b", "/c"]
    assert parse_dirs_env("  /a : /b ") == ["/a", "/b"]
    # Empty entries between colons drop out (e.g. trailing colon).
    assert parse_dirs_env("/a::/b:") == ["/a", "/b"]


def test_registry_accepts_single_dir_backcompat(tmp_path):
    """Existing callers pass a single Path/string -- must keep working."""
    _write_baseline(tmp_path, "BL-A", "VARIANT-A")
    reg = make_registry(tmp_path)
    assert reg.all_variants() == ["VARIANT-A"]
    assert len(reg.directories()) == 1
    assert reg.directories()[0] == tmp_path


def test_registry_accepts_list_of_dirs(tmp_path):
    """The new path: pass an iterable; variants from all dirs merge."""
    dir_a = tmp_path / "oss"
    dir_b = tmp_path / "customer"
    dir_a.mkdir()
    dir_b.mkdir()
    _write_baseline(dir_a, "OSS-1", "OSS-VARIANT-1")
    _write_baseline(dir_a, "OSS-2", "OSS-VARIANT-2")
    _write_baseline(dir_b, "CUST-1", "CUSTOMER-VARIANT-1")

    reg = make_registry([dir_a, dir_b])

    assert sorted(reg.all_variants()) == [
        "CUSTOMER-VARIANT-1",
        "OSS-VARIANT-1",
        "OSS-VARIANT-2",
    ]
    # Both OSS variants still resolve (the customer dir didn't shadow them).
    assert reg.get("OSS-VARIANT-1") is not None
    assert reg.get("CUSTOMER-VARIANT-1") is not None
    assert len(reg.directories()) == 2


def test_duplicate_variant_across_dirs_is_hard_error(tmp_path):
    """Same platform_variant in two layered dirs -- the operator must
    resolve, not the loader. Silent precedence would create debug-hell
    discrepancies between asset_cm_state and what the operator expects."""
    dir_a = tmp_path / "oss"
    dir_b = tmp_path / "customer"
    dir_a.mkdir()
    dir_b.mkdir()
    _write_baseline(dir_a, "OSS-VERSION", "SHARED_VARIANT")
    _write_baseline(dir_b, "CUSTOMER-VERSION", "SHARED_VARIANT")

    with pytest.raises(BaselineLoadError, match="Duplicate platform_variant"):
        make_registry([dir_a, dir_b])


def test_missing_directory_in_list_raises_with_path_in_message(tmp_path):
    """When ONE dir of several is missing, fail with a useful message
    naming THAT dir rather than a generic load error."""
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    _write_baseline(real_dir, "X", "X")
    missing = tmp_path / "does-not-exist"

    with pytest.raises(BaselineLoadError, match=str(missing.name)):
        make_registry([real_dir, missing])


def test_dedup_identical_dirs_in_input(tmp_path):
    """Passing the same dir twice (operator typo) doesn't double-load
    and doesn't trigger the duplicate-variant error -- it dedupes silently."""
    _write_baseline(tmp_path, "ONE", "ONLY")
    reg = make_registry([tmp_path, tmp_path])
    assert reg.all_variants() == ["ONLY"]
    assert len(reg.directories()) == 1


def test_reload_picks_up_changes_in_any_configured_dir(tmp_path):
    """SIGHUP-style reload must rescan EVERY configured dir, not just one."""
    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()
    _write_baseline(dir_a, "A1", "A-VARIANT")
    reg = make_registry([dir_a, dir_b])
    assert reg.all_variants() == ["A-VARIANT"]

    # Add a baseline to the OTHER dir (the customer overlay path).
    _write_baseline(dir_b, "B1", "B-VARIANT")
    reg.reload()
    assert sorted(reg.all_variants()) == ["A-VARIANT", "B-VARIANT"]


def test_customer-overlay_baselines_layer_cleanly_on_real_oss_baselines(tmp_path):
    """End-to-end-shape test: simulate the production layering by reading
    the real OSS baselines + a fake-customer overlay together. Catches
    accidental variant collisions before they ship.

    This locks in the contract the customer-overlay bundle relies on: as long
    as customer overlay variant names don't collide with the OSS set
    (M1A2-SEPv3, F-35A-Block4, UH-60M), the layered load works."""
    customer_dir = tmp_path / "customer"
    customer_dir.mkdir()
    # Names matching the real customer-overlay ORBAT (subset).
    for v in ["CUAS_Sensor", "MRAD_Interceptor", "HEADQUARTER_COMPLEX"]:
        _write_baseline(customer_dir, f"{v}-Baseline-2026.1", v)

    reg = make_registry([REAL_BASELINES_DIR, customer_dir])

    variants = set(reg.all_variants())
    assert {"M1A2-SEPv3", "F-35A-Block4", "UH-60M"}.issubset(variants)
    assert {"CUAS_Sensor", "MRAD_Interceptor", "HEADQUARTER_COMPLEX"}.issubset(variants)
