"""
Baseline loader — reads ConfigurationBaseline YAMLs from disk and produces
a dict[platform_variant -> ConfigurationBaseline] proto messages.

Hot-reloadable via SIGHUP. Validates each YAML against the protobuf schema;
malformed files raise BaselineLoadError instead of silently skipping (a silent
skip would mean an asset gets initialized against the wrong baseline).
"""
from __future__ import annotations

import logging
import os
import signal
import threading
from pathlib import Path
from typing import Iterable

import yaml
from google.protobuf import json_format
from google.protobuf.timestamp_pb2 import Timestamp

# Generated bindings are on PYTHONPATH (set by the Dockerfile and by tests).
from openddil.configuration.v1 import configuration_baseline_pb2 as cb

logger = logging.getLogger("cm_service.baselines")


class BaselineLoadError(RuntimeError):
    """Raised when a baseline YAML fails schema validation or is unreadable."""


class BaselineRegistry:
    """In-memory dict of ConfigurationBaseline messages keyed by platform_variant.

    Thread-safe for reads (Python's GIL + a plain dict swap on reload). Reload
    is triggered by SIGHUP or by direct `.reload()` call.
    """

    def __init__(self, directory: Path):
        self._dir = Path(directory)
        self._lock = threading.Lock()
        self._by_variant: dict[str, cb.ConfigurationBaseline] = {}
        self._file_to_variant: dict[Path, str] = {}

    # ---- Public API ----------------------------------------------------

    def load(self) -> None:
        """Initial load. Reads every *.yaml in the directory."""
        new_by_variant, new_file_map = self._read_directory()
        with self._lock:
            self._by_variant = new_by_variant
            self._file_to_variant = new_file_map
        logger.info(
            "Loaded %d baseline(s): %s",
            len(new_by_variant),
            sorted(new_by_variant.keys()),
        )

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
        if not self._dir.is_dir():
            raise BaselineLoadError(
                f"Baseline directory does not exist: {self._dir}"
            )
        by_variant: dict[str, cb.ConfigurationBaseline] = {}
        file_map: dict[Path, str] = {}
        for yaml_path in sorted(self._dir.glob("*.yaml")):
            try:
                baseline = self._read_one(yaml_path)
            except Exception as exc:
                raise BaselineLoadError(
                    f"Failed to load baseline from {yaml_path.name}: {exc}"
                ) from exc
            if not baseline.platform_variant:
                raise BaselineLoadError(
                    f"{yaml_path.name} has no platform_variant"
                )
            if baseline.platform_variant in by_variant:
                raise BaselineLoadError(
                    f"Duplicate platform_variant {baseline.platform_variant!r}: "
                    f"both {file_map.get(yaml_path, '?')} and {yaml_path.name}"
                )
            by_variant[baseline.platform_variant] = baseline
            file_map[yaml_path] = baseline.platform_variant
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
    directory: str | os.PathLike,
) -> BaselineRegistry:
    """Convenience constructor — builds a registry and runs initial load."""
    reg = BaselineRegistry(Path(directory))
    reg.load()
    return reg
