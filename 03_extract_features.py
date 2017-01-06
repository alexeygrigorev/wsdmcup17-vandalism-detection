# coding: utf-8

from glob import glob
from time import time


import pandas as pd
import numpy as np

import feather
from tqdm import tqdm

import feature_extraction_utils as fu


# In[3]:

feather_files = sorted(glob('./data_frames/wdvc16_*.feather'))


dfs = []
for f in feather_files:
    year_match = re.match('.+wdvc16_(\d{4})_.+', f)
    year = int(year_match.group(1))
    
    if year < 2015:
        continue
    df = feather.read_dataframe(f)
    dfs.append(df)

df_all = pd.concat(dfs)
df_all.reset_index(drop=1, inplace=1)

del dfs, df

# train-validation-test split

TRAIN = 0
VAL = 1
TEST = 2

ts = df_all.revision_timestamp

jan16 = pd.to_datetime('2016-01-01')
march16 = pd.to_datetime('2016-03-01')

df_all['fold'] = TRAIN
df_all['fold'] = df_all['fold'].astype('uint8')

df_all.loc[(ts > jan16) & (ts <= march16), 'fold'] = VAL
df_all.loc[ts > march16, 'fold'] = TEST


# user features


df_all.anonimous_ip = df_all.anonimous_ip.apply(fu.ip_features)


df_all['user_info'] = 'user_id=' + df_all.user_id.astype('str') + ' ' + \
                       df_all.anonimous_ip + ' ' + df_all.anonimous_meta


# comment features


comments = df_all.revision_comment
struc = comments.apply(fu.extract_structured_comment)
links = comments.apply(fu.extract_links)
unstruc = comments.apply(fu.extract_unstructured_text)


# saving everything

df_features = pd.DataFrame()
df_features['revision_id'] = df_all.revision_id
df_features['user_info'] = df_all.user_info
df_features['page_title'] = df_all.page_title
df_features['reverted'] = df_all.reverted

df_features['comment_structured_text'] = struc
df_features['comment_links'] = links
df_features['comment_unstructured_text'] = unstruc

df_features['fold'] = df_all.fold


feather.write_dataframe(df_features, 'df_features.feather')
