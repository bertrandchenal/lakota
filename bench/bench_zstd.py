from itertools import chain

from numcodecs import registry
from numpy import arange, array, random

from lakota.utils import pretty_nb

arrays = {
    "arange": arange(100_000),
    "random": random.rand(100_000),
}
mixed = array(
    list(
        chain.from_iterable(
            zip(
                arrays["arange"][:50_000],
                arrays["random"][:50_000],
            )
        )
    )
)
arrays["mixed"] = mixed

random_sorted = random.rand(100_000)
random_sorted.sort()
arrays["random_sorted"] = random_sorted

mixed_sorted = mixed.copy()
mixed_sorted.sort()
arrays["mixed_sorted"] = mixed_sorted


for arr_name, arr in arrays.items():
    print("**", arr_name)
    base = len(arr.tobytes())
    print("  raw", pretty_nb(base))
    for name in ("blosc", "zstd", "lz4"):
        if name == "blosc":
            codec = registry.codec_registry[name]()
        else:
            codec = registry.codec_registry[name]()
        codec_len = len(codec.encode(arr))
        print(" ", name, codec_len / base, pretty_nb(codec_len))
