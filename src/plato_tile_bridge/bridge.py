"""Tile format bridge for cross-language conversion."""
import json, struct
from dataclasses import dataclass

TILE_BINARY_SIZE = 384

@dataclass
class TileRecord:
    id: str
    content: str
    domain: str
    confidence: float
    priority: str

class TileBridge:
    def to_dict(self, tile: TileRecord) -> dict:
        return {"id": tile.id, "content": tile.content, "domain": tile.domain,
                "confidence": tile.confidence, "priority": tile.priority}

    def from_dict(self, data: dict) -> TileRecord:
        return TileRecord(id=data.get("id", ""), content=data.get("content", ""),
                          domain=data.get("domain", "general"),
                          confidence=data.get("confidence", 0.5),
                          priority=data.get("priority", "P2"))

    def to_json(self, tile: TileRecord) -> str:
        return json.dumps(self.to_dict(tile))

    def from_json(self, raw: str) -> TileRecord:
        return self.from_dict(json.loads(raw))

    def to_binary(self, tile: TileRecord) -> bytes:
        buf = bytearray(TILE_BINARY_SIZE)
        content_bytes = tile.content.encode('utf-8')[:360]
        buf[:4] = struct.pack('<I', len(content_bytes))
        buf[4:4+len(content_bytes)] = content_bytes
        domain_bytes = tile.domain.encode('utf-8')[:8]
        buf[364:364+len(domain_bytes)] = domain_bytes
        buf[372] = int(tile.confidence * 255)
        priority_map = {"P0": 0, "P1": 1, "P2": 2}
        buf[373] = priority_map.get(tile.priority, 2)
        return bytes(buf)

    def from_binary(self, data: bytes) -> TileRecord:
        content_len = struct.unpack('<I', data[:4])[0]
        content = data[4:4+content_len].decode('utf-8', errors='replace')
        domain = data[364:372].split(b'\x00')[0].decode('utf-8', errors='replace')
        confidence = data[372] / 255.0
        priority_map = {0: "P0", 1: "P1", 2: "P2"}
        priority = priority_map.get(data[373], "P2")
        return TileRecord(id="", content=content, domain=domain,
                          confidence=confidence, priority=priority)

    def roundtrip(self, tile: TileRecord) -> TileRecord:
        return self.from_binary(self.to_binary(tile))
