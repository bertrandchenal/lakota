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
    from lakota import Schema, Frame
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
        if offset > 2**(4*8):
            # TODO Does this test make sense?
            raise ValueError("Frame is too long!")

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


def load():
    arrs = []
    for col in ("trigram", "doc_id"):
        with open(f"{col}.idx", "rb") as fh:
            content = fh.read()
        codec = registry.codec_registry["blosc"]
        arr = codec().decode(content)
        dt = "U3" if col == "trigram" else "u4"
        arrs.append(frombuffer(arr, dtype=dt))

    idx = rec.fromarrays(arrs, names=["trigram", "doc_id"])
    return idx


def search(*pattern):
    idx = load()
    res = None
    trigrams = chain.from_iterable(generate_trigrams(p) for p in pattern)
    for trg in trigrams:
        start_pos = bisect_left(idx.trigram, trg)
        end_pos = bisect_right(idx.trigram, trg)
        sub_array = idx.doc_id[start_pos:end_pos]
        if res is None:
            res = sub_array
        else:
            res = intersect(res, sub_array)
    print(pattern, len(res), res)


test()


