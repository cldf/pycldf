"""
Functionality to manipulate URLs.
"""
from typing import Callable, Union
import urllib.parse

__all__ = ['update_url', 'sanitize_url', 'url_without_fragment']


def update_url(
        url: Union[str, urllib.parse.ParseResult],
        updater: Callable[[urllib.parse.ParseResult], tuple[str, str, str, str, str]],
) -> Union[str, None]:
    """Generic update function for URLs."""
    if url is None:
        return None
    if isinstance(url, str):
        url = urllib.parse.urlparse(url)
    return urllib.parse.urlunsplit(updater(url)) or None


def sanitize_url(url: str) -> str:
    """
    Removes auth credentials from a URL.
    """
    def fix(u):
        host = u.hostname
        if u.port:
            host += f':{u.port}'
        return (u.scheme, host, u.path, u.query, u.fragment)

    return update_url(url, fix)


def url_without_fragment(url: Union[str, urllib.parse.ParseResult]) -> str:
    """Removes fragment from URL."""
    return update_url(url, lambda u: (u.scheme, u.hostname, u.path, u.query, ''))
