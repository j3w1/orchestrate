"""Small host-platform predicates shared by admission and controller bootstrap."""

from __future__ import annotations

from collections.abc import Mapping
import os
import platform as platform_module
import sys


SUPPORTED_NATIVE_PLATFORMS = frozenset({"linux", "win32"})


def is_wsl(
    environment: Mapping[str, str] | None = None,
    *,
    platform_name: str | None = None,
    kernel_release: str | None = None,
) -> bool:
    """Return whether a Linux process is running through the WSL boundary."""

    selected_platform = sys.platform if platform_name is None else platform_name
    if selected_platform != "linux":
        return False
    env = os.environ if environment is None else environment
    if any(env.get(name, "").strip() for name in ("WSL_DISTRO_NAME", "WSL_INTEROP")):
        return True
    release = platform_module.release() if kernel_release is None else kernel_release
    return "microsoft" in release.casefold()
