from tempfile import TemporaryDirectory

import pytest

from baltic import POD

@pytest.yield_fixture(scope='function', params=['memory', 'tmp']) #, 's3', 
def pod(request):
    if request.param == 'memory':
        yield POD.from_uri('memory://')

    elif request.param == 'tmp':
        # Local filesytem
        with TemporaryDirectory() as tdir:
            yield POD.from_uri(f'file://{tdir}')

    # elif request.param == 's3':
    #     # Minio
    #     uri = 's3://minioadmin:minioadmin@127.0.0.1:9000/baltic-test/'
    #     registry = Registry(uri)
    #     registry.clear()
    #     yield uri
    else:
        raise

