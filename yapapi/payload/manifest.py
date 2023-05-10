import asyncio
import re
import sys
from datetime import datetime
from typing import Dict, List, Optional

from dataclasses import dataclass, field, fields

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

from dateutil.tz import UTC

from yapapi.payload import vm
from yapapi.utils import explode_dict


class Aliased:
    """Aliasing mixin for Manifest dataclasses."""

    @classmethod
    def _to_aliased_dict(cls, values: dict):
        for f in fields(cls):
            alias = f.metadata.get("alias")
            if alias and f.name in values:
                values[alias] = values.pop(f.name)

    @classmethod
    def _from_aliased_dict(cls, values: dict):
        for f in fields(cls):
            alias = f.metadata.get("alias")
            if alias and alias in values:
                values[f.name] = values.pop(alias)


def parse_datetime(date_str: str) -> datetime:
    """Parse input datetime from the manifest."""

    return datetime.fromisoformat(re.sub("Z$", "+00:00", date_str))


@dataclass
class ManifestMetadata:
    name: str
    version: str
    authors: List[str] = field(default_factory=list)
    homepage: Optional[str] = None
    description: Optional[str] = None

    def dict(self, *, by_alias=False):
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "authors": self.authors.copy(),
            "homepage": self.homepage,
        }

    @classmethod
    def parse_obj(cls, obj: Dict, by_alias: bool = False) -> "ManifestMetadata":
        obj_copy = obj.copy()

        if obj_copy.get("authors") is not None:
            obj_copy["authors"] = obj_copy["authors"].copy()

        return cls(**obj_copy)


@dataclass
class ManifestPayloadPlatform(Aliased):
    arch: str = "x86_64"
    os: str = "linux"
    os_version: Optional[str] = field(default=None, metadata={"alias": "osVersion"})

    def dict(self, *, by_alias=False):
        obj = {
            "arch": self.arch,
            "os": self.os,
        }
        if self.os_version is not None:
            obj["os_version"] = self.os_version

        if by_alias:
            self._to_aliased_dict(obj)

        return obj

    @classmethod
    def parse_obj(cls, obj: Dict, by_alias: bool = False) -> "ManifestPayloadPlatform":
        obj_copy = obj.copy()

        if by_alias:
            cls._from_aliased_dict(obj_copy)

        return cls(**obj_copy)


@dataclass
class ManifestPayload:
    hash: str
    urls: List[str] = field(default_factory=list)
    platform: Optional[ManifestPayloadPlatform] = field(default_factory=ManifestPayloadPlatform)

    def dict(self, *, by_alias=False):
        obj: Dict = {
            "hash": self.hash,
        }

        if self.platform is not None:
            obj["platform"] = self.platform.dict(by_alias=by_alias)

        if self.urls is not None:
            obj["urls"] = self.urls.copy()

        return obj

    @classmethod
    def parse_obj(cls, obj: Dict, by_alias: bool = False) -> "ManifestPayload":
        obj_copy = obj.copy()

        if obj_copy.get("platform") is not None:
            obj_copy["platform"] = ManifestPayloadPlatform.parse_obj(
                obj_copy["platform"], by_alias=by_alias
            )

        if obj_copy.get("urls") is not None:
            obj_copy["urls"] = obj_copy["urls"].copy()

        return cls(**obj_copy)

    async def resolve_urls_from_hash(self) -> None:
        package = await vm.repo(image_hash=self.hash)

        if ":" not in self.hash:
            self.hash = f"sha3:{self.hash}"

        full_url = await package.resolve_url()

        image_url = full_url.split(":", 3)[3]

        if image_url not in self.urls:
            self.urls.append(image_url)


@dataclass
class CompManifestScript:
    commands: List[str] = field(default_factory=lambda: ["run .*"])
    match: Literal["strict", "regex"] = "regex"

    def dict(self, *, by_alias=False):
        return {
            "commands": self.commands.copy(),
            "match": self.match,
        }

    @classmethod
    def parse_obj(cls, obj: Dict, by_alias: bool = False) -> "CompManifestScript":
        obj_copy = obj.copy()

        if obj_copy.get("commands") is not None:
            obj_copy["commands"] = obj_copy["commands"].copy()

        return cls(**obj_copy)


@dataclass
class CompManifestNetInetOut:
    protocols: List[Literal["http", "https", "ws", "wss"]] = field(
        default_factory=lambda: ["http", "https", "ws", "wss"]
    )
    urls: Optional[List[str]] = None

    def dict(self, *, by_alias=False):
        obj: Dict = {
            "protocols": self.protocols.copy(),
        }

        if self.urls is not None:
            obj["urls"] = self.urls.copy()

        return obj

    @classmethod
    def parse_obj(cls, obj: Dict, by_alias: bool = False) -> "CompManifestNetInetOut":
        obj_copy = obj.copy()

        if obj_copy.get("protocols") is not None:
            obj_copy["protocols"] = obj_copy["protocols"].copy()

        if obj_copy.get("urls") is not None:
            obj_copy["urls"] = obj_copy["urls"].copy()

        return cls(**obj_copy)


@dataclass
class CompManifestNetInet:
    out: Optional[CompManifestNetInetOut] = None

    def dict(self, *, by_alias=False):
        obj = {}

        if self.out is not None:
            obj["out"] = self.out.dict(by_alias=by_alias)

        return obj

    @classmethod
    def parse_obj(cls, obj: Dict, by_alias: bool = False) -> "CompManifestNetInet":
        obj_copy = obj.copy()

        if obj_copy.get("out") is not None:
            obj_copy["out"] = CompManifestNetInetOut.parse_obj(obj_copy["out"], by_alias=by_alias)

        return cls(**obj_copy)


@dataclass
class CompManifestNet:
    inet: Optional[CompManifestNetInet] = None

    def dict(self, *, by_alias=False):
        obj = {}

        if self.inet is not None:
            obj["inet"] = self.inet.dict(by_alias=by_alias)

        return obj

    @classmethod
    def parse_obj(cls, obj: Dict, by_alias: bool = False) -> "CompManifestNet":
        obj_copy = obj.copy()

        if obj_copy.get("inet") is not None:
            obj_copy["inet"] = CompManifestNetInet.parse_obj(obj_copy["inet"], by_alias=by_alias)

        return cls(**obj_copy)


@dataclass
class CompManifest:
    version: str = "0.0.0"
    script: Optional[CompManifestScript] = None
    net: Optional[CompManifestNet] = None

    def dict(self, *, by_alias=False):
        obj = {
            "version": self.version,
        }

        if self.script is not None:
            obj["script"] = self.script.dict(by_alias=by_alias)

        if self.net is not None:
            obj["net"] = self.net.dict(by_alias=by_alias)

        return obj

    @classmethod
    def parse_obj(cls, obj: Dict, by_alias: bool = False) -> "CompManifest":
        obj_copy = obj.copy()

        if obj_copy.get("script") is not None:
            obj_copy["script"] = CompManifestScript.parse_obj(obj_copy["script"], by_alias=by_alias)

        if obj_copy.get("net") is not None:
            obj_copy["net"] = CompManifestNet.parse_obj(obj_copy["net"], by_alias=by_alias)

        return cls(**obj_copy)


@dataclass
class Manifest(Aliased):
    payload: List[ManifestPayload]
    version: str = "0.0.0"
    comp_manifest: Optional[CompManifest] = field(default=None, metadata={"alias": "compManifest"})
    # Using lambda helps with mocking in tests
    created_at: datetime = field(
        default_factory=lambda: datetime.now(UTC), metadata={"alias": "createdAt"}
    )
    expires_at: datetime = field(
        default_factory=lambda: datetime(2100, 1, 1, tzinfo=UTC), metadata={"alias": "expiresAt"}
    )
    metadata: Optional[ManifestMetadata] = None

    def dict(self, *, by_alias=False):
        obj = {
            "version": self.version,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "payload": [payload.dict(by_alias=by_alias) for payload in self.payload],
        }

        if self.comp_manifest is not None:
            obj["comp_manifest"] = self.comp_manifest.dict(by_alias=by_alias)

        if self.metadata is not None:
            obj["metadata"] = self.metadata.dict(by_alias=by_alias)

        if by_alias:
            self._to_aliased_dict(obj)

        return obj

    @classmethod
    def parse_obj(cls, obj: Dict, by_alias: bool = False) -> "Manifest":
        obj_copy = obj.copy()

        if by_alias:
            cls._from_aliased_dict(obj_copy)

        if isinstance(obj_copy.get("created_at"), str):
            obj_copy["created_at"] = parse_datetime(obj_copy["created_at"])

        if isinstance(obj_copy.get("expires_at"), str):
            obj_copy["expires_at"] = parse_datetime(obj_copy["expires_at"])

        if obj_copy.get("payload") is not None:
            obj_copy["payload"] = [
                ManifestPayload.parse_obj(p, by_alias=by_alias) for p in obj_copy["payload"]
            ]

        if obj_copy.get("comp_manifest") is not None:
            obj_copy["comp_manifest"] = CompManifest.parse_obj(
                obj_copy["comp_manifest"], by_alias=by_alias
            )

        if obj_copy.get("metadata") is not None:
            obj_copy["metadata"] = ManifestMetadata.parse_obj(
                obj_copy["metadata"], by_alias=by_alias
            )

        return cls(**obj_copy)

    @classmethod
    async def generate(
        cls, image_hash: str = None, outbound_urls: List[str] = None, **kwargs
    ) -> "Manifest":
        if image_hash is not None:
            kwargs["payload.0.hash"] = image_hash

        if outbound_urls is not None:
            kwargs["comp_manifest.net.inet.out.urls"] = outbound_urls

        manifest = cls.parse_obj(explode_dict(kwargs))

        await asyncio.gather(*[payload.resolve_urls_from_hash() for payload in manifest.payload])

        return manifest
