from numpy import array

from jensen import Schema


def test_vlen_codecs():
    for codecs in ("", "vlen-utf8", "vlen-utf8 gzip"):
        schema = Schema(f"val str*  |{codecs}")

        arr = array(["ham", "spam"])
        buff = schema["val"].encode(arr)
        arr2 = schema["val"].decode(buff)

        assert all(arr == arr2)
        assert arr.dtype == arr2.dtype
