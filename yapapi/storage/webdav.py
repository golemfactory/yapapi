from os import PathLike
from typing import Optional, AsyncIterator, List, NamedTuple
from . import StorageProvider, Source, Destination, Content
import aiohttp
import uuid
from dataclasses import dataclass
from datetime import datetime
import logging
from urllib.parse import urlparse, urljoin

_logger: logging.Logger = logging.getLogger(__name__)


class DavResource(NamedTuple):
    path: str
    length: int
    collection: bool
    last_modified: Optional[datetime] = None


def _parse_prop_resp(xml: str) -> List[DavResource]:
    from io import StringIO
    from xml.etree import ElementTree
    from email.utils import parsedate_to_datetime

    tree = ElementTree.parse(StringIO(xml))

    def resource(element: ElementTree.Element) -> DavResource:
        href = element.findtext("{DAV:}href")
        prop = element.find("{DAV:}propstat")
        if prop is not None:
            prop = prop.find("{DAV:}prop")

        if prop is None or href is None:
            raise RuntimeError("missing props in xml response")
        length = prop.findtext("{DAV:}getcontentlength")
        resource_type = prop.find("{DAV:}resourcetype")
        last_modified = prop.findtext("{DAV:}getlastmodified")
        collection = resource_type.find("{DAV:}collection") is not None if resource_type else False
        return DavResource(
            path=href,
            length=0 if length is None else int(length),
            collection=collection,
            last_modified=None if last_modified is None else parsedate_to_datetime(last_modified),
        )

    return list(map(resource, tree.findall("{DAV:}response")))


class _DavSource(Source):
    def __init__(self, url: str, length: Optional[int] = None):
        self._url = url
        self._length = length

    @property
    def download_url(self) -> str:
        return self._url

    async def content_length(self) -> int:
        if self._length:
            return self._length
        async with aiohttp.ClientSession() as session:
            async with session.get(url=self._url) as resp:
                if resp.status != 200:
                    raise RuntimeError("invalid url")
                if resp.content_length is None:
                    raise RuntimeError("invalid response missing length")
                return resp.content_length


class _DavDestination(Destination):
    def __init__(self, client: aiohttp.ClientSession, url: str):
        self._client = client
        self._url = url

    @property
    def upload_url(self) -> str:
        return self._url

    async def download_stream(self) -> Content:
        resp = await self._client.get(self._url)
        length = resp.content_length
        if length is None:
            raise RuntimeError("missing content-length")

        return Content.from_reader(length, resp.content)


@dataclass
class DavStorageProvider(StorageProvider):
    client: aiohttp.ClientSession
    base_url: str
    auth: Optional[aiohttp.BasicAuth] = None

    def __post_init__(self):
        if self.base_url[-1] != "/":
            self.base_url += "/"

    @classmethod
    async def for_directory(
        cls,
        client: aiohttp.ClientSession,
        base_url: str,
        directory_name: str,
        auth: Optional[aiohttp.BasicAuth] = None,
        **kwargs,
    ) -> "DavStorageProvider":
        if base_url[-1] != "/":
            base_url += "/"
        col_url = urljoin(base_url, directory_name)
        response = await client.request(method="MKCOL", url=col_url, auth=auth)
        if response.status != 201 and response.status != 405:
            response.raise_for_status()
        return cls(client, col_url, auth=auth)

    async def upload_stream(self, length: int, stream: AsyncIterator[bytes]) -> Source:
        upload_url = self.__new_url()
        resp = await self.client.request(method="PUT", url=upload_url, data=stream, auth=self.auth)
        _logger.debug("upload done: %i", resp.status)
        if resp.status != 201:
            raise RuntimeError(
                f"invalid response for url: {upload_url}, {resp.status}/{resp.reason}"
            )
        return _DavSource(self.__export_url(upload_url))

    async def new_destination(self, destination_file: Optional[PathLike] = None) -> Destination:
        upload_url = self.__new_url()
        return _DavDestination(self.client, self.__export_url(upload_url))

    async def prop_find(self):
        headers = {"content-type": "text/xml", "depth": "1"}
        data = """<?xml version="1.0"?>
        <a:propfind xmlns:a="DAV:">
        <a:prop>
            <a:resourcetype/>
            <a:getcontentlength/>
            <a:getlastmodified/>
        </a:prop>
        </a:propfind>"""

        async with self.client.request(
            method="PROPFIND", url=self.base_url, auth=self.auth, headers=headers, data=data,
        ) as response:
            if response.status != 200:
                response.raise_for_status()
            return _parse_prop_resp(await response.text())

    def __export_url(self, url) -> str:
        if self.auth:
            parsed_url = urlparse(url)
            login, password, *_ = self.auth
            return parsed_url._replace(netloc=f"{login}:{password}@{parsed_url.netloc}").geturl()
        else:
            return url

    def __new_url(self):
        name = uuid.uuid4()
        return urljoin(self.base_url, str(name))
