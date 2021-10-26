import ray
import pandas as pd
import dask.dataframe as dd

# Create a Dataset from a list of Pandas DataFrame objects.
pdf = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
ds = ray.data.from_pandas([ray.put(pdf)])

# Create a Dataset from a Dask-on-Ray DataFrame.
dask_df = dd.from_pandas(pdf, npartitions=10)
ds = ray.data.from_dask(dask_df)