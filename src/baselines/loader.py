"""
Baseline loader — reads ConfigurationBaseline YAMLs from disk and produces
a dict[platform_variant -> ConfigurationBaseline] proto messages.

Hot-reloadable via SIGHUP. Validates each YAML against the protobuf schema;
malformed files raise BaselineLoadError instead of silently skipping (a silent
skip would mean an asset gets initialized against the wrong baseline).

Multi-directory layering (added 2026-06): the registry now accepts one or
more directories. Each one is scanned, results merged. A duplicate
`platform_variant` across directories is an error -- the operator must
either rename the overlay file or remove the colliding OSS baseline,
because cm-service has no policy for picking a winner (overlay or OSS
"wins" depends on the deployment, and silent precedence would create
debug-hell discrepancies between asset_cm_state and what the operator
expects). Same variant in the same directory is also a hard error.
"""
from __future__ import annotations

import logging
import os
import signal
import threading
from pathlib import Path
from typing import Iterable, Sequence, Union

import yaml
from google.protobuf import json_format
from google.protobuf.timestamp_pb2 import Timestamp

# Generated bindings are on PYTHONPATH (set by the Dockerfile and by tests).
from openddil.configuration.v1 import configuration_baseline_pb2 as cb

logger = logging.getLogger("cm_service.baselines")


class BaselineLoadError(RuntimeError):
    """Raised when a baseline YAML fails schema validation or is unreadable."""


# Accept Path, str, or an iterable of either. Centralized so the caller can
# pass whatever's natural in their context (one dir from env, list from
# config, etc.).
DirsArg = Union[Path, str, Sequence[Union[Path, str]]]


def _normalize_dirs(directories: DirsArg) -> list[Path]:
    """Coerce single path | string | iterable into a deduped Path list,
    preserving the input order. Order matters because the precedence rule
    is "first dir wins on equal variants if we ever change to overlay
    precedence" -- today it's "any duplicate is an error", but keeping
    order stable makes the error message deterministic."""
    if isinstance(directories, (str, Path)):
        items: list[Path] = [Path(directories)]
    else:
        items = [Path(d) for d in directories]
    seen: set[Path] = set()
    out: list[Path] = []
    for p in items:
        rp = p.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        out.append(p)
    return out


class BaselineRegistry:
    """In-memory dict of ConfigurationBaseline messages keyed by platform_variant.

    Thread-safe for reads (Python's GIL + a plain dict swap on reload). Reload
    is triggered by SIGHUP or by direct `.reload()` call.

    Accepts one directory (back-compat) OR an iterable of directories. With
    multiple directories, every *.yaml from every directory contributes;
    a duplicate platform_variant across directories raises
    BaselineLoadError naming both files.
    """

    def __init__(self, directories: DirsArg):
        self._dirs: list[Path] = _normalize_dirs(directories)
        # Kept for back-compat with any external caller reading `.directory`.
        # When multiple dirs are configured, this points at the first.
        self._dir: Path = self._dirs[0] if self._dirs else Path()
        self._lock = threading.Lock()
        self._by_variant: dict[str, cb.ConfigurationBaseline] = {}
        self._file_to_variant: dict[Path, str] = {}

    # ---- Public API ----------------------------------------------------

    def load(self) -> None:
        """Initial load. Reads every *.yaml in every configured directory."""
        new_by_variant, new_file_map = self._read_directory()
        with self._lock:
            self._by_variant = new_by_variant
            self._file_to_variant = new_file_map
        logger.info(
            "Loaded %d baseline(s) from %d director(y/ies) %s: %s",
            len(new_by_variant),
            len(self._dirs),
            [str(d) for d in self._dirs],
            sorted(new_by_variant.keys()),
        )

    def directories(self) -> list[Path]:
        """Return the configured baseline directories in input order."""
        return list(self._dirs)

    def reload(self) -> None:
        """Re-read the directory and swap. Logs the diff."""
        new_by_variant, new_file_map = self._read_directory()
        with self._lock:
            old = set(self._by_variant.keys())
            new = set(new_by_variant.keys())
            added = new - old
            removed = old - new
            kept = new & old
            changed = {
                v for v in kept
                if self._by_variant[v].SerializeToString()
                != new_by_variant[v].SerializeToString()
            }
            self._by_variant = new_by_variant
            self._file_to_variant = new_file_map
        logger.info(
            "Baseline reload — added=%s removed=%s changed=%s unchanged=%d",
            sorted(added), sorted(removed), sorted(changed),
            len(kept) - len(changed),
        )

    def get(self, platform_variant: str) -> cb.ConfigurationBaseline | None:
        with self._lock:
            return self._by_variant.get(platform_variant)

    def all_variants(self) -> list[str]:
        with self._lock:
            return sorted(self._by_variant.keys())

    def install_sighup_handler(self) -> None:
        """Hook SIGHUP to trigger reload. Safe to call once at app startup."""
        signal.signal(signal.SIGHUP, lambda *_: self.reload())
        logger.info("SIGHUP installed; send `kill -HUP <pid>` to hot-reload baselines")

    # ---- Internals -----------------------------------------------------

    def _read_directory(
        self,
    ) -> tuple[dict[str, cb.ConfigurationBaseline], dict[Path, str]]:
        """Read every configured directory and merge into one variant map.

        Original name retained -- the method's job is unchanged externally;
        the multi-directory iteration is an implementation detail. Duplicate
        platform_variant anywhere (same dir OR across dirs) is a hard error,
        with a message that names both source files so the operator can
        resolve it.
        """
        if not self._dirs:
            raise BaselineLoadError(
                "No baseline directories configured -- pass at least one path "
                "to BaselineRegistry(...)"
            )
        for d in self._dirs:
            if not d.is_dir():
                raise BaselineLoadError(
                    f"Baseline directory does not exist: {d}"
                )

        by_variant: dict[str, cb.ConfigurationBaseline] = {}
        # variant -> source path, for duplicate-reporting
        variant_source: dict[str, Path] = {}
        file_map: dict[Path, str] = {}

        for directory in self._dirs:
            for yaml_path in sorted(directory.glob("*.yaml")):
                try:
                    baseline = self._read_one(yaml_path)
                except Exception as exc:
                    raise BaselineLoadError(
                        f"Failed to load baseline from {yaml_path}: {exc}"
                    ) from exc
                if not baseline.platform_variant:
                    raise BaselineLoadError(
                        f"{yaml_path} has no platform_variant"
                    )
                pv = baseline.platform_variant
                if pv in by_variant:
                    raise BaselineLoadError(
                        f"Duplicate platform_variant {pv!r}: "
                        f"both {variant_source[pv]} and {yaml_path}"
                    )
                by_variant[pv] = baseline
                variant_source[pv] = yaml_path
                file_map[yaml_path] = pv

        return by_variant, file_map

    def _read_one(self, path: Path) -> cb.ConfigurationBaseline:
        raw = yaml.safe_load(path.read_text())
        if not isinstance(raw, dict):
            raise BaselineLoadError(
                f"{path.name}: top-level YAML must be a mapping, got {type(raw).__name__}"
            )
        # Normalize timestamps from ISO-8601 strings to proto Timestamp form.
        # google.protobuf.json_format.ParseDict accepts ISO-8601 strings for
        # Timestamp fields, so we just need to ensure they're present (drop null).
        raw = _drop_null_timestamps(raw)
        baseline = cb.ConfigurationBaseline()
        try:
            json_format.ParseDict(raw, baseline, ignore_unknown_fields=False)
        except json_format.ParseError as exc:
            raise BaselineLoadError(
                f"{path.name}: schema validation failed: {exc}"
            ) from exc
        return baseline


def _drop_null_timestamps(d: dict) -> dict:
    """ParseDict rejects `null` for Timestamp fields; YAML `null` becomes Python
    None. Drop None-valued top-level keys that correspond to optional Timestamp
    fields. This is a minimal, well-known list — we don't try to be generic."""
    optional_timestamp_keys = {"superseded_at"}
    return {
        k: v for k, v in d.items()
        if not (k in optional_timestamp_keys and v is None)
    }


def make_registry(
    directories: DirsArg,
) -> BaselineRegistry:
    """Convenience constructor -- builds a registry and runs initial load.

    Accepts a single directory (str / Path) for back-compat or an iterable
    of directories for layered overlays. See the BaselineRegistry docstring
    for duplicate-variant semantics.
    """
    reg = BaselineRegistry(directories)
    reg.load()
    return reg


def parse_dirs_env(spec: str | None) -> list[str]:
    """Parse a colon-separated directory list (PATH-style) into individual
    paths. Empty/whitespace entries are dropped. Returns an empty list when
    spec is None / empty; the caller decides what fallback to apply.

    Use case: a CM_BASELINES_DIRS env var like
        '/baselines:/baselines-customer:/baselines-mwo-overrides'
    """
    if not spec:
        return []
    return [s.strip() for s in spec.split(":") if s.strip()]
