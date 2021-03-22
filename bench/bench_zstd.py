# Script output:

# ** arange
#   raw 800.00k
#   blosc 0.00671 5.37k
#   zstd 0.1277825 102.23k
#   lz4 0.50026125 400.21k
# ** random
#   raw 800.00k
#   blosc 0.87689625 701.52k
#   zstd 0.93866375 750.93k
#   lz4 1.00392875 803.14k
# ** mixed
#   raw 800.00k
#   blosc 0.87322375 698.58k
#   zstd 0.5925975 474.08k
#   lz4 0.79945625 639.57k
# ** random_sorted
#   raw 800.00k
#   blosc 0.69136125 553.09k
#   zstd 0.92005625 736.04k
#   lz4 1.00392875 803.14k
# ** mixed_sorted
#   raw 800.00k
#   blosc 0.36376 291.01k
#   zstd 0.5220125 417.61k
#   lz4 0.7560675 604.85k


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
