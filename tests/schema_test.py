from numpy import array

from baltic import Schema


def test_vlen_codecs():
    for codecs in ("", "|vlen-utf8", "|vlen-utf8|gzip"):
        schema = Schema([f"val:<U{codecs}"])
        arr = array(["ham", "spam"])
        buff = schema.encode("val", arr)
        arr2 = schema.decode("val", buff)

        assert all(arr == arr2)
        assert arr.dtype == arr2.dtype
