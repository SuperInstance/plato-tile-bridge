"""Tile bridge — bidirectional sync between tile stores with transform, conflict resolution."""
import time
from dataclasses import dataclass, field
from typing import Optional, Callable
from collections import defaultdict

@dataclass
class SyncResult:
    created: int = 0
    updated: int = 0
    deleted: int = 0
    conflicts: int = 0
    skipped: int = 0
    duration_s: float = 0.0

@dataclass
class TileMapping:
    source_id: str
    target_id: str
    last_synced: float = 0.0
    version: int = 0
    transform: str = ""  # transform name applied

@dataclass
class ConflictRecord:
    tile_id: str
    source_version: int
    target_version: int
    source_data: dict = field(default_factory=dict)
    target_data: dict = field(default_factory=dict)
    resolution: str = "pending"
    resolved_at: float = 0.0

class TileBridge:
    def __init__(self, source_name: str = "source", target_name: str = "target"):
        self.source_name = source_name
        self.target_name = target_name
        self._mappings: dict[str, TileMapping] = {}
        self._transforms: dict[str, Callable] = {}
        self._conflicts: list[ConflictRecord] = []
        self._sync_log: list[dict] = []
        self._bidirectional: bool = False

    def register_transform(self, name: str, fn: Callable):
        self._transforms[name] = fn

    def map_tile(self, source_id: str, target_id: str, transform: str = "") -> TileMapping:
        mapping = TileMapping(source_id=source_id, target_id=target_id,
                            last_synced=time.time(), transform=transform)
        self._mappings[source_id] = mapping
        return mapping

    def get_mapping(self, source_id: str) -> Optional[TileMapping]:
        return self._mappings.get(source_id)

    def sync(self, source_tiles: dict, target_tiles: dict,
             transform: str = "", conflict_strategy: str = "source_wins") -> SyncResult:
        start = time.time()
        result = SyncResult()
        for tile_id, tile_data in source_tiles.items():
            mapping = self._mappings.get(tile_id)
            target_id = mapping.target_id if mapping else tile_id
            # Apply transform
            data = dict(tile_data)
            if transform and transform in self._transforms:
                try: data = self._transforms[transform](data)
                except: pass
            elif mapping and mapping.transform and mapping.transform in self._transforms:
                try: data = self._transforms[mapping.transform](data)
                except: pass
            # Check target
            existing = target_tiles.get(target_id)
            if existing is None:
                target_tiles[target_id] = data
                result.created += 1
                if not mapping:
                    self.map_tile(tile_id, target_id, transform)
            else:
                src_ver = tile_data.get("version", 0)
                tgt_ver = existing.get("version", 0)
                if src_ver > tgt_ver:
                    target_tiles[target_id] = data
                    result.updated += 1
                elif src_ver == tgt_ver and data != existing:
                    result.conflicts += 1
                    self._conflicts.append(ConflictRecord(
                        tile_id=tile_id, source_version=src_ver,
                        target_version=tgt_ver, source_data=data, target_data=existing))
                    if conflict_strategy == "source_wins":
                        target_tiles[target_id] = data
                    elif conflict_strategy == "merge":
                        merged = {**existing, **data, "version": src_ver}
                        target_tiles[target_id] = merged
                else:
                    result.skipped += 1
            if mapping:
                mapping.last_synced = time.time()
                mapping.version = data.get("version", 0)
        self._log_sync(result)
        result.duration_s = round(time.time() - start, 3)
        return result

    def resolve_conflict(self, tile_id: str, strategy: str = "source") -> bool:
        for conflict in self._conflicts:
            if conflict.tile_id == tile_id and conflict.resolution == "pending":
                conflict.resolution = strategy
                conflict.resolved_at = time.time()
                return True
        return False

    def pending_conflicts(self) -> list[ConflictRecord]:
        return [c for c in self._conflicts if c.resolution == "pending"]

    def sync_status(self) -> dict:
        synced = sum(1 for m in self._mappings.values() if time.time() - m.last_synced < 3600)
        stale = sum(1 for m in self._mappings.values() if time.time() - m.last_synced >= 3600)
        return {"total_mappings": len(self._mappings), "recently_synced": synced,
                "stale": stale, "pending_conflicts": len(self.pending_conflicts())}

    def unmapped(self, source_ids: set, target_ids: set) -> dict:
        in_source_not_target = source_ids - set(m.target_id for m in self._mappings.values())
        in_target_not_source = target_ids - set(m.target_id for m in self._mappings.values())
        return {"source_only": list(in_source_not_target),
                "target_only": list(in_target_not_source)}

    def _log_sync(self, result: SyncResult):
        self._sync_log.append({"created": result.created, "updated": result.updated,
                               "conflicts": result.conflicts, "skipped": result.skipped,
                               "timestamp": time.time()})
        if len(self._sync_log) > 500:
            self._sync_log = self._sync_log[-500:]

    @property
    def stats(self) -> dict:
        return {"source": self.source_name, "target": self.target_name,
                "mappings": len(self._mappings), "transforms": len(self._transforms),
                "conflicts": len(self._conflicts),
                "syncs": len(self._sync_log)}
