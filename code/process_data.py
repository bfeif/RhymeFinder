import dask.dataframe as dd
import dask.bag as db
import csv
from dask.distributed import Client, progress
client = Client(processes=False, 
	threads_per_worker=4,
	n_workers=4,
	memory_limit='2GB')

# cmu list split helper function
def cmu_list_split_helper(x, rhyme_length=3):
	splitted = x.split()
	return (splitted[0], splitted[-rhyme_length:])

# glove stuff
glove_data_file = "../data/glove/glove.6B.100d_1000lines.txt"
gloves = dd.read_table(glove_data_file, 
	sep=" ", 
	header=None, 
	quoting=csv.QUOTE_NONE)
gloves = gloves.set_index(0)
print(gloves.compute().head())

# cmu stuff
cmu_data_file = "../data/cmu/cmudict_1000lines.dict"
phone_seqs = db.read_text(cmu_data_file)
phone_seqs = phone_seqs.map(cmu_list_split_helper)
phone_seqs = phone_seqs.to_dataframe()
phone_seqs = phone_seqs.set_index(0)

# preprocess gloves and cmus together
def preprocess_df(df):
	return df

# merge them
joined = dd.merge_ordered(gloves, phone_seqs, left_index=True, right_index=True)
print(joined.compute().head())