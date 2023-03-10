import json
from datetime import datetime
from unittest import mock

import pytest
from dateutil.tz import UTC

from yapapi.payload.manifest import (
    CompManifest,
    CompManifestNet,
    CompManifestNetInet,
    CompManifestNetInetOut,
    CompManifestScript,
    Manifest,
    ManifestMetadata,
    ManifestPayload,
    ManifestPayloadPlatform,
)


@pytest.fixture
def manifest_obj():
    return Manifest(
        version="0.1.0",
        created_at=datetime(2022, 12, 1, 0, 0, 0, tzinfo=UTC),
        expires_at=datetime(2100, 1, 1, 0, 0, 0, tzinfo=UTC),
        metadata=ManifestMetadata(
            name="Service1",
            description="Description of Service1",
            version="0.1.1",
            authors=[
                "mf <mf@golem.network>",
                "ng <ng@golem.network>",
            ],
            homepage="https://github.com/golemfactory/s1",
        ),
        payload=[
            ManifestPayload(
                platform=ManifestPayloadPlatform(
                    arch="x86_64",
                    os="linux",
                    os_version="6.1.7601",
                ),
                urls=[
                    "http://girepo.dev.golem.network:8000/"
                    "docker-gas_scanner_backend_image-latest-91c471517a.gvmi",
                ],
                hash="sha3:05270a8a938ff5f5e30b0e61bc983a8c3e286c5cd414a32e1a077657",
            ),
        ],
        comp_manifest=CompManifest(
            version="0.1.0",
            script=CompManifestScript(
                commands=[
                    "run .*",
                ],
                match="regex",
            ),
            net=CompManifestNet(
                inet=CompManifestNetInet(
                    out=CompManifestNetInetOut(
                        protocols=[
                            "http",
                        ],
                        urls=[
                            "http://bor.golem.network",
                        ],
                    ),
                ),
            ),
        ),
    )


@pytest.fixture
def manifest_obj_with_no_optional_values(manifest_obj):
    manifest_obj.metadata = None
    manifest_obj.payload[0].platform.os_version = None

    return manifest_obj


@pytest.fixture
def manifest_dict():
    return {
        "version": "0.1.0",
        "created_at": "2022-12-01T00:00:00+00:00",
        "expires_at": "2100-01-01T00:00:00+00:00",
        "metadata": {
            "name": "Service1",
            "description": "Description of Service1",
            "version": "0.1.1",
            "authors": [
                "mf <mf@golem.network>",
                "ng <ng@golem.network>",
            ],
            "homepage": "https://github.com/golemfactory/s1",
        },
        "payload": [
            {
                "platform": {
                    "arch": "x86_64",
                    "os": "linux",
                    "os_version": "6.1.7601",
                },
                "urls": [
                    "http://girepo.dev.golem.network:8000/"
                    "docker-gas_scanner_backend_image-latest-91c471517a.gvmi",
                ],
                "hash": "sha3:05270a8a938ff5f5e30b0e61bc983a8c3e286c5cd414a32e1a077657",
            }
        ],
        "comp_manifest": {
            "version": "0.1.0",
            "script": {
                "commands": [
                    "run .*",
                ],
                "match": "regex",
            },
            "net": {
                "inet": {
                    "out": {
                        "protocols": [
                            "http",
                        ],
                        "urls": [
                            "http://bor.golem.network",
                        ],
                    },
                },
            },
        },
    }


@pytest.fixture
def manifest_dict_with_alias(manifest_dict):
    manifest_dict["createdAt"] = manifest_dict.pop("created_at")
    manifest_dict["expiresAt"] = manifest_dict.pop("expires_at")
    manifest_dict["compManifest"] = manifest_dict.pop("comp_manifest")
    manifest_dict["payload"][0]["platform"]["osVersion"] = manifest_dict["payload"][0][
        "platform"
    ].pop("os_version")

    return manifest_dict


@pytest.fixture
def manifest_dict_with_no_optional_values(manifest_dict):
    manifest_dict.pop("metadata")
    manifest_dict["payload"][0]["platform"].pop("os_version")

    return manifest_dict


def test_manifest_to_dict_no_alias(manifest_obj, manifest_dict):
    assert manifest_obj.dict() == manifest_dict


def test_manifest_to_dict_with_alias(manifest_obj, manifest_dict_with_alias):
    assert manifest_obj.dict(by_alias=True) == manifest_dict_with_alias


def test_manifest_to_dict_missing_values(
    manifest_obj_with_no_optional_values, manifest_dict_with_no_optional_values
):
    assert manifest_obj_with_no_optional_values.dict() == manifest_dict_with_no_optional_values


def test_manifest_to_json(manifest_obj, manifest_dict):
    manifest_obj_json = json.dumps(manifest_obj.dict())
    manifest_dict_json = json.dumps(manifest_dict)

    assert json.loads(manifest_obj_json) == json.loads(manifest_dict_json)


def test_manifest_parse_obj(manifest_obj, manifest_dict):
    assert Manifest.parse_obj(manifest_dict) == manifest_obj


@mock.patch(
    "yapapi.payload.manifest.datetime", **{"utcnow.return_value": datetime(2020, 1, 1, tzinfo=UTC)}
)
def test_manifest_with_minimal_data(mocked_datetime):
    payload_hash = "asd"

    minimal_manifest_dict = Manifest(
        payload=[
            ManifestPayload(
                hash=payload_hash,
            ),
        ],
    ).dict()

    assert minimal_manifest_dict == {
        "created_at": "2020-01-01T00:00:00+00:00",
        "expires_at": "2100-01-01T00:00:00+00:00",
        "payload": [
            {
                "hash": payload_hash,
                "urls": [],
                "platform": {
                    "arch": "x86_64",
                    "os": "linux",
                },
            },
        ],
        "version": "",
    }


@mock.patch(
    "yapapi.payload.manifest.datetime", **{"utcnow.return_value": datetime(2020, 1, 1, tzinfo=UTC)}
)
def test_manifest_generate(mocked_datetime):
    payload_urls = [
        "some url",
        "some other url",
        "yet another url",
    ]
    payload_hash = "some hash"
    metadata_name = "example"
    metadata_version = "0.0.1"
    comp_manifest_script_match = "strict"

    assert Manifest.parse_imploded_obj(
        {
            "metadata.name": metadata_name,
            "metadata.version": metadata_version,
            "payload.0.hash": payload_hash,
            "payload.0.urls": [
                payload_urls[0],
                payload_urls[1],
            ],
            "payload.0.urls.2": payload_urls[2],
            "comp_manifest.script.match": comp_manifest_script_match,
        }
    ).dict() == {
        "metadata": {
            "name": metadata_name,
            "version": metadata_version,
            "authors": [],
            "homepage": None,
            "description": None,
        },
        "comp_manifest": {
            "script": {
                "commands": [
                    "run .*",
                ],
                "match": comp_manifest_script_match,
            },
            "version": "",
        },
        "created_at": "2020-01-01T00:00:00+00:00",
        "expires_at": "2100-01-01T00:00:00+00:00",
        "payload": [
            {
                "hash": payload_hash,
                "urls": payload_urls,
                "platform": {
                    "arch": "x86_64",
                    "os": "linux",
                },
            },
        ],
        "version": "",
    }


@pytest.mark.asyncio
async def test_manifest_payload_resolve_urls_from_hash():
    payload = ManifestPayload(
        hash="05270a8a938ff5f5e30b0e61bc983a8c3e286c5cd414a32e1a077657",
    )

    assert payload.urls == []

    await payload.resolve_urls_from_hash()

    assert payload.urls == [
        "http://girepo.dev.golem.network:8000/"
        "docker-gas_scanner_backend_image-latest-91c471517a.gvmi",
    ]


@mock.patch("yapapi.payload.vm.repo", mock.AsyncMock(**{'return_value.resolve_url': mock.AsyncMock(return_value='dupa')}))
@pytest.mark.asyncio
async def test_manifest_payload_resolve_urls_from_hash_is_not_duplicating_or_overriding_values():
    urls = [
        "some url",
        "http://girepo.dev.golem.network:8000/"
        "docker-gas_scanner_backend_image-latest-91c471517a.gvmi",
        "other url",
    ]

    payload = ManifestPayload(
        hash="05270a8a938ff5f5e30b0e61bc983a8c3e286c5cd414a32e1a077657",
        urls=urls,
    )

    assert payload.urls == urls

    await payload.resolve_urls_from_hash()

    assert payload.urls == urls
