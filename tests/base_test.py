from uuid import uuid4

import zarr

from baltic import Pod


def test_write():
    from pandas import DataFrame
    FACTOR = 20_000
    use_idx = False
    names = [str(uuid4()) for _ in range(5)]
    df = DataFrame({
        'value': [1.1, 2.2, 3.3, 4.4, 5.5]*FACTOR * 10,
        'category': names * FACTOR * 10,
    })
    gr = zarr.TempStore()
    import pdb;pdb.set_trace()
    pod = Pod(gr,)
    pod.save(df, use_idx=use_idx)

    res = pod.read('category', 'value')

    for col in res:
        assert (res[col][:] == df[col]).all()
