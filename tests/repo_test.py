from tempfile import TemporaryDirectory

from pandas import DataFrame
import pytest

from baltic import Repo, Schema, Segment


@pytest.yield_fixture(params=['directory', 'memory'])
def repo(request):
    schema = Schema({
        'timestamp': 'dim',
        'value': 'msr',
    })

    if request.param == 'directory':
        with TemporaryDirectory() as td:
            # directory based repo
            repo = Repo(td)
            repo.init(schema)
            yield repo
    else:
        # in-memory repo
        repo = Repo()
        repo.init(schema)
        yield repo


def test_schema(repo):
    repo._schema = None
    assert repo.schema is not None

def test_write(repo):
    df = DataFrame({
        'timestamp': [f'2020-01-0{i}' for i in range(10)],
        'value': range(10),
    })
    sgm = Segment.from_df(repo.schema, df)
    repo.write(sgm, '2020-01-01', '2020-01-02')
    sgm = repo.read()
    for col in repo.schema.columns:
        assert all(sgm[col][:] == df[col])
