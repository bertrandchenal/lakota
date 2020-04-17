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

    def __setitem__(self, name, arr):
        categ_like = (dtype('O'), dtype('U')) # should be managed in schema
        if arr.dtype in categ_like:
            categ = zarr.Categorize(unique(arr), dtype=object)
            self.root.array(name, arr, dtype=object, object_codec=categ)
        else:
            self.root.array(name, arr)

    def write(self, df, reverse_idx=False):
        # TODO check no column is missing (at least in the index)
        for name in self.schema.columns:
            arr = df[name]
            if hasattr(arr, 'values'):
                arr = arr.values
            self[name] = arr

    def read(self, *names):
        cols = {}
        for name in names:
            arr = self[name]
            cols[name] = arr
        return cols

    def __getitem__(self, name):
        return self.root[name]

    def hexdigests(self):
        for name in self.schema.columns:
            yield name, self.root[name].hexdigest()

    def copy(self, column, dest_group, dest_name=None):
        dest_name = dest_name or column
        zarr.copy(self.root[column], dest_group, dest_name)
