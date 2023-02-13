import unicodedata
from bisect import bisect_left, bisect_right
from itertools import chain, islice


from numcodecs import registry
from numpy import (
    concatenate,
    frombuffer,
    fromiter,
    rec,
    repeat,
)
from sortednp import intersect

from lakota import Repo, Schema, Frame


def generate_trigrams(text):
    length = len(text)
    if length < 3:
        return
    a_s, b_s, c_s = (
        islice(text, 0, length-2),
        islice(text, 1, length-1),
        islice(text, 2, length),
    )
    for a, b, c in zip(a_s, b_s, c_s):
        yield a + b + c


def unidecode(text):
    return ''.join(c for c in unicodedata.normalize('NFD', text)
                   if unicodedata.category(c) != 'Mn')


def ingest(record):
    trigrams = set()
    for value in record.values():
        value = unidecode(str(value).lower()) 
        trigrams.update(generate_trigrams(value))
    return trigrams


def test():
    schema = Schema(timestamp="timestamp*", app="str*", msg="O")
    length = 20
    keys = ["ham", "spam", "foo"]
    frm = Frame(schema, {
        'timestamp': range(length),
        'app': ["app%s" % i for i in range(length)],
        "msg": [{
            key: key +str(i) for key in keys
        } for i in range(length)],
    })
    trigram_idx = from_frame(frm, ["msg"])
    assert len(trigram_idx) > 1 # TODO :)

    # Create repo and save trg-idx
    repo = Repo()
    schema = Schema(trigram="U3*", offset="i8*")  #pos="i4"
    collection = repo.create_collection(schema, "trig")
    save(trigram_idx, collection)

    # Search
    idx = collection.series("DEADBEEF").frame()
    search(idx, "spam19")

def save(trigram_idx, collection):
    checksum = "DEADBEEF"
    series = collection / checksum
    series.write({
        "trigram": trigram_idx.trigram,
        "offset": trigram_idx.offset,
    })


def from_frame(frame, trigram_columns):
    """
    Extract string content from trigram_columns in fram. Return a
    rec-array containing two arrays: "trigram" and "offset"
    """
    trigrams_list = []
    ids_list = []
    for column in trigram_columns:
        if frame.schema[column].codec.dt != "O":
            raise ValueError("TODO")
        for offset, value in enumerate(frame[column]):
            trigrams = ingest(value)
            trigrams_list.append(fromiter(trigrams, "U3", len(trigrams)))
            ids_list.append(repeat(offset, len(trigrams)).astype("u4"))

    # Create index & sort it
    idx = rec.fromarrays(
        [
            concatenate(trigrams_list),
            concatenate(ids_list),
        ],
        names=["trigram", "offset"],
    )
    idx.sort()
    return idx


def search(idx, *pattern):
    res = None
    trigrams = chain.from_iterable(generate_trigrams(p) for p in pattern)
    for trg in trigrams:
        start_pos = bisect_left(idx["trigram"], trg)
        end_pos = bisect_right(idx["trigram"], trg)
        sub_array = idx["offset"][start_pos:end_pos]
        if res is None:
            res = sub_array
        else:
            res = intersect(res, sub_array)
    print(pattern, len(res), res)


test()


