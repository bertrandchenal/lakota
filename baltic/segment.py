from numpy import dtype, unique
import zarr


class Segment:
    '''
    In-memory storage for one or more dataframe
    '''

    def __init__(self, schema, zarr_group=None):
        if zarr_group is None:
            # Create in-memory group
            zarr_group = zarr.group()
        elif not isinstance(zarr_group, zarr.Group):
            zarr_group = zarr.group(zarr_group)
        self.root = zarr_group
        self.schema = schema

    @classmethod
    def from_df(cls, schema, df):
        sgm = cls(schema)
        sgm.write(df)
        return sgm

    def write_col(self, name, arr):
        if name in self.root:
            self.root[name].append(arr)
        else:
            self.root.array(name, arr)

    def write_categ_col(self, name, arr):
        if name in self.root:
            # TODO check categories are the same
            self.root[name].append(arr)
        else:
            categ = zarr.Categorize(unique(arr), dtype=object)
            self.root.array(name, arr, dtype=object, object_codec=categ)

    def write(self, df, reverse_idx=False):
        # TODO check no column is missing (at least in the index)
        categ_like = [dtype('O'), dtype('U')]
        for name in self.schema.columns:
            arr = df[name]
            if hasattr(arr, 'values'):
                arr = arr.values
            if arr.dtype in categ_like:
                self.write_categ_col(name, arr)
            else:
                self.write_col(name, arr)

    def read_col(self, name):
        return self.root[name]

    def read(self, *names):
        cols = {}
        for name in names:
            arr = self.read_col(name)
            cols[name] = arr
        return cols

    def hexdigests(self):
        for name in self.schema.columns:
            yield name, self.root[name].hexdigest()

    def copy(self, column, dest_group, dest_name):
        zarr.copy(self.root[column], dest_group, dest_name)
