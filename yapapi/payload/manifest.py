import asyncio
import sys
from datetime import datetime
from typing import Dict, List, Optional

from dataclasses import dataclass, field

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

from dateutil.tz import UTC

from yapapi.payload import vm
from yapapi.utils import explode_dict


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
    def parse_obj(cls, obj: Dict) -> "ManifestMetadata":
        obj_copy = obj.copy()

        if obj_copy.get("authors") is not None:
            obj_copy["authors"] = obj_copy["authors"].copy()

        return cls(**obj_copy)


@dataclass
class ManifestPayloadPlatform:
    arch: str = "x86_64"
    os: str = "linux"
    os_version: Optional[str] = None

    def dict(self, *, by_alias=False):
        obj = {
            "arch": self.arch,
            "os": self.os,
        }

        if self.os_version is not None:
            obj["osVersion" if by_alias else "os_version"] = self.os_version

        return obj

    @classmethod
    def parse_obj(cls, obj: Dict) -> "ManifestPayloadPlatform":
        obj_copy = obj.copy()

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
    def parse_obj(cls, obj: Dict) -> "ManifestPayload":
        obj_copy = obj.copy()

        if obj_copy.get("platform") is not None:
            obj_copy["platform"] = ManifestPayloadPlatform.parse_obj(obj_copy["platform"])

        if obj_copy.get("urls") is not None:
            obj_copy["urls"] = obj_copy["urls"].copy()

        return cls(**obj_copy)

    async def resolve_urls_from_hash(self) -> None:
        package = await vm.repo(image_hash=self.hash)

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
    def parse_obj(cls, obj: Dict) -> "CompManifestScript":
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
    def parse_obj(cls, obj: Dict) -> "CompManifestNetInetOut":
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
    def parse_obj(cls, obj: Dict) -> "CompManifestNetInet":
        obj_copy = obj.copy()

        if obj_copy.get("out") is not None:
            obj_copy["out"] = CompManifestNetInetOut.parse_obj(obj_copy["out"])

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
    def parse_obj(cls, obj: Dict) -> "CompManifestNet":
        obj_copy = obj.copy()

        if obj_copy.get("inet") is not None:
            obj_copy["inet"] = CompManifestNetInet.parse_obj(obj_copy["inet"])

        return cls(**obj_copy)


@dataclass
class CompManifest:
    version: str = ""
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
    def parse_obj(cls, obj: Dict) -> "CompManifest":
        obj_copy = obj.copy()

        if obj_copy.get("script") is not None:
            obj_copy["script"] = CompManifestScript.parse_obj(obj_copy["script"])

        if obj_copy.get("net") is not None:
            obj_copy["net"] = CompManifestNet.parse_obj(obj_copy["net"])

        return cls(**obj_copy)


@dataclass
class Manifest:
    payload: List[ManifestPayload]
    version: str = ""
    comp_manifest: Optional[CompManifest] = None
    # Using lambda helps with mocking in tests
    created_at: datetime = field(default_factory=lambda: datetime.utcnow())
    expires_at: datetime = datetime(2100, 1, 1, tzinfo=UTC)
    metadata: Optional[ManifestMetadata] = None

    def dict(self, *, by_alias=False):
        obj = {
            "version": self.version,
            "createdAt" if by_alias else "created_at": self.created_at.isoformat(),
            "expiresAt" if by_alias else "expires_at": self.expires_at.isoformat(),
            "payload": [payload.dict(by_alias=by_alias) for payload in self.payload],
        }

        if self.comp_manifest is not None:
            comp_manifest_field_name = "compManifest" if by_alias else "comp_manifest"
            obj[comp_manifest_field_name] = self.comp_manifest.dict(by_alias=by_alias)

        if self.metadata is not None:
            obj["metadata"] = self.metadata.dict(by_alias=by_alias)

        return obj

    @classmethod
    def parse_obj(cls, obj: Dict) -> "Manifest":
        obj_copy = obj.copy()

        if isinstance(obj_copy.get("created_at"), str):
            obj_copy["created_at"] = datetime.strptime(
                obj_copy["created_at"], "%Y-%m-%dT%H:%M:%S%z"
            )

        if isinstance(obj_copy.get("expires_at"), str):
            obj_copy["expires_at"] = datetime.strptime(
                obj_copy["expires_at"], "%Y-%m-%dT%H:%M:%S%z"
            )

        if obj_copy.get("payload") is not None:
            obj_copy["payload"] = [ManifestPayload.parse_obj(p) for p in obj_copy["payload"]]

        if obj_copy.get("comp_manifest") is not None:
            obj_copy["comp_manifest"] = CompManifest.parse_obj(obj_copy["comp_manifest"])

        if obj_copy.get("metadata") is not None:
            obj_copy["metadata"] = ManifestMetadata.parse_obj(obj_copy["metadata"])

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
