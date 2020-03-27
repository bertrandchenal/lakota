from numpy import dtype, unique
import zarr


class Pod:

    def __init__(self, zarr_group, schema=None):
        if not isinstance(zarr_group, zarr.Group):
            zarr_group = zarr.group(zarr_group)
        self.root = zarr_group

    def save_col(self, name, arr):
        if name in self.root:
            self.root[name].append(arr)
        else:
            self.root.array(name, arr)

    def read_col(self, name):
        return self.root[name]

    def save_categ_col(self, name, arr):
        if name in self.root:
            # TODO check categories are the same
            self.root[name].append(arr)
        else:
            categ = zarr.Categorize(unique(arr), dtype=object)
            self.root.array(name, arr, dtype=object, object_codec=categ)

    def save(self, df, use_idx=False):
        # TODO ensure columns are part of schema
        categ_like = [dtype('O'), dtype('U')]
        cols = dict(df)

        for name, arr in cols.items():
            arr = arr.values # FIXME
            if arr.dtype in categ_like:
                self.save_categ_col(name, arr)
            else:
                self.save_col(name, arr)

    def read(self, *names):
        cols = {}
        for name in names:
            arr = self.read_col(name)
            cols[name] = arr
        return cols
