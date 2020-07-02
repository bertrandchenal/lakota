from numpy import arange, random
from numcodecs import registry

arrays = {
    'arange': arange(100_000),
    'random': random.random(100_000),
}

for arr_name, arr in arrays.items():
    print('**', arr_name)
    base = len(arr.tostring())
    print('raw', base)
    for name in ('blosc', 'zstd'):
        codec = registry.codec_registry[name]()
        print(name, len(codec.encode(arr))/base)
