from lakota import Repo, Schema

# TODO implement squash on top of collection


def test_create():
    repo = Repo()
    schema = Schema(
        [
            "timestamp int*",
            "value float",
        ]
    )
    frame = {
        "timestamp": [0, 1, 2],
        "value": [0, 10, 20],
    }
    series_a, series_b = repo.create(schema, "label_a", "label_b", collection="test")
    c1 = series_a.write(frame)
    c2 = series_b.write(frame)

    assert c2 != c1
    assert len(series_a.revisions()) == 1
    assert len(series_b.revisions()) == 1

    collection = repo.collection("test")
    assert len(collection.revisions()) == 2
