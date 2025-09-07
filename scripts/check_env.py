#!/usr/bin/env python
from __future__ import annotations

import os
import platform
import shutil
import sys


def _mem_total_gb() -> float | None:
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    return round(kb / (1024**2), 2)
    except Exception:
        return None
    return None


def main() -> None:
    print("SPLF Environment Check")
    print("-" * 60)

    # Basics
    print(f"Python: {sys.version.split()[0]}")
    print(f"Platform: {platform.platform()} ({platform.machine()})")
    print(f"CPU cores: {os.cpu_count() or 1}")
    mem_gb = _mem_total_gb()
    if mem_gb:
        print(f"RAM: {mem_gb} GB")

    # Core packages
    def v(mod):
        try:
            m = __import__(mod)
            return getattr(m, "__version__", "unknown")
        except Exception:
            return "missing"

    print("Packages:")
    print(f"  numpy: {v('numpy')}")
    print(f"  pandas: {v('pandas')}")
    print(f"  scikit-learn: {v('sklearn')}")

    # GPU / cuML
    has_gpu = False
    cupy_ver = v("cupy")
    print(f"  cupy: {cupy_ver}")
    if cupy_ver != "missing":
        try:
            import cupy as cp  # type: ignore

            ndev = cp.cuda.runtime.getDeviceCount()
            has_gpu = ndev > 0
            if has_gpu:
                props = cp.cuda.runtime.getDeviceProperties(0)
                name = props.get("name", b"")
                if isinstance(name, (bytes, bytearray)):
                    name = name.decode(errors="ignore")
                print(f"  GPU devices: {ndev} (0: {name})")
        except Exception as e:
            print(f"  GPU check via CuPy failed: {e}")

    cuml_ver = v("cuml")
    print(f"  cuml: {cuml_ver}")

    # NVIDIA tooling (informational)
    nvsmi = shutil.which("nvidia-smi")
    if nvsmi:
        print(f"nvidia-smi: {nvsmi}")
    else:
        print("nvidia-smi: not found (normal on Jetson)")

    # Recommendations
    print("-" * 60)
    sym_count = 1
    try:
        import yaml  # type: ignore

        if os.path.exists("config/config.yaml"):
            with open("config/config.yaml", "r") as f:
                cfg = yaml.safe_load(f)
                uni = cfg.get("universe", {})
                symbols = uni.get("symbols") or (uni.get("tier_a", []) + uni.get("tier_b", []) + uni.get("tier_c", []))
                sym_count = max(1, len(symbols))
    except Exception:
        pass

    cpu_cores = os.cpu_count() or 1
    suggested_workers = max(1, min(sym_count, cpu_cores))
    print(f"Suggested runtime.workers: {suggested_workers} (symbols={sym_count}, cores={cpu_cores})")
    if has_gpu and cuml_ver != "missing":
        print("Model backend: auto (GPU cuML will be used)")
    else:
        print("Model backend: sklearn (GPU not detected or cuML missing)")


if __name__ == "__main__":
    main()
