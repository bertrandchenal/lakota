from itertools import chain

from numcodecs import registry
from numpy import arange, array, random

arrays = {
    "arange": arange(100_000),
    "random": random.random(100_000),
}
mixed = array(list(chain.from_iterable(zip(arrays["arange"], arrays["random"]))))
arrays["mixed"] = mixed

mixed_sorted = mixed.copy()
mixed_sorted.sort()
arrays["mixed_sorted"] = mixed_sorted


for arr_name, arr in arrays.items():
    print("**", arr_name)
    base = len(arr.tostring())
    print("  raw", base)
    for name in ("blosc", "zstd", "lz4"):
        if name == "blosc":
            codec = registry.codec_registry[name]()
        else:
            codec = registry.codec_registry[name]()
        print(" ", name, len(codec.encode(arr)) / base)
