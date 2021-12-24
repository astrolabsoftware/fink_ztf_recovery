import pandas as pd
import numpy as np

pdf = pd.read_clipboard(sep='\t', skiprows=range(1, 10000, 2))
pdf = pdf[pdf['Size'].apply(lambda x: 'G' in x)]
n_tera = pdf['Size'].apply(lambda x: float(x[:-1])).sum()/1024
pdf['Name'].apply(lambda x: 'https://ztf.uw.edu/alerts/public/' + x).to_csv('uri-ztf.csv', header=None, index=False)

n_file_per_split = 5
pdf = pd.read_csv('uri-ztf.csv', header=None)
indices = np.arange(0, len(pdf), n_file_per_split).tolist() + [-1]
for l, r in zip(indices[0:-1], indices[1:]):
    subpdf = pd.DataFrame(pdf[0].values[l:r])
    subpdf.to_csv('uris/{:03d}.txt'.format(l), header=None, index=False)
