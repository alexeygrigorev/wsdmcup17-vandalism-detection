# coding: utf-8

# In[1]:

import pandas as pd
import feather

from glob import glob
from tqdm import tqdm


df_label = pd.read_csv('../data/wdvc16_truth.csv')
del df_label['UNDO_RESTORE_REVERTED']
df_label.ROLLBACK_REVERTED = (df_label.ROLLBACK_REVERTED == 'T').astype('uint8')
df_label.REVISION_ID = df_label.REVISION_ID.astype('uint32')

feather.write_dataframe(df_label, 'data_frames/df_label_train.feather')


# In[3]:

df_label = pd.read_csv('../data/wdvc16_2016_03_truth.csv')
del df_label['UNDO_RESTORE_REVERTED']
df_label.ROLLBACK_REVERTED = (df_label.ROLLBACK_REVERTED == 'T').astype('uint8')
df_label.REVISION_ID = df_label.REVISION_ID.astype('uint32')

feather.write_dataframe(df_label, 'data_frames/df_label_val.feather')


# In[4]:

def stringify_row(row):
    d = row.to_dict()
    dict_str = ' '.join('%s=%s' % (k, v.replace(' ', '_')) for (k, v) in d.items() if v)
    return dict_str


# In[5]:

df_meta = pd.read_csv('../data/wdvc16_meta.csv')
meta_nas = df_meta.isnull().sum(axis=1)

df_meta = df_meta[meta_nas != 7].reset_index(drop=1)
df_meta.REVISION_ID = df_meta.REVISION_ID.astype('uint32')
df_meta.fillna('', inplace=1)

del df_meta['REVISION_SESSION_ID']
df_meta.set_index('REVISION_ID', inplace=1)

meta_strings = df_meta.apply(stringify_row, axis=1)

df_meta_strings = meta_strings.reset_index()
df_meta_strings.columns = ['REVISION_ID', 'meta_string']
feather.write_dataframe(df_meta_strings, 'data_frames/df_meta_strings_train.feather')


# In[6]:

df_sessions = pd.read_csv('../data/wdvc16_meta.csv', usecols=['REVISION_ID', 'REVISION_SESSION_ID'], 
                          dtype={'REVISION_ID': 'uint32', 'REVISION_SESSION_ID': 'uint32'})
feather.write_dataframe(df_sessions, 'data_frames/df_sessions_train.feather')


# In[12]:

df_meta = pd.read_csv('../data/wdvc16_2016_03_meta.csv')
meta_nas = df_meta.isnull().sum(axis=1)

df_meta = df_meta[meta_nas != 7].reset_index(drop=1)
df_meta.REVISION_ID = df_meta.REVISION_ID.astype('uint32')
df_meta.fillna('', inplace=1)

del df_meta['REVISION_SESSION_ID']
df_meta.set_index('REVISION_ID', inplace=1)

meta_strings = df_meta.apply(stringify_row, axis=1)

df_meta_strings = meta_strings.reset_index()
df_meta_strings.columns = ['REVISION_ID', 'meta_string']
feather.write_dataframe(df_meta_strings, 'data_frames/df_meta_strings_val.feather')


# In[14]:

df_sessions = pd.read_csv('../data/wdvc16_2016_03_meta.csv', usecols=['REVISION_ID', 'REVISION_SESSION_ID'], 
                          dtype={'REVISION_ID': 'uint32', 'REVISION_SESSION_ID': 'uint32'})
feather.write_dataframe(df_sessions, 'data_frames/df_sessions_val.feather')


# In[17]:

df_meta_train = feather.read_dataframe('./data_frames/df_meta_strings_train.feather')
df_meta_val = feather.read_dataframe('./data_frames/df_meta_strings_val.feather')
df_meta = pd.concat([df_meta_train, df_meta_val])
df_meta.reset_index(inplace=1, drop=1)
df_meta.set_index('REVISION_ID', inplace=1)


# In[23]:

df_session_train = feather.read_dataframe('./data_frames/df_sessions_train.feather')
df_session_val = feather.read_dataframe('./data_frames/df_sessions_val.feather')
df_session = pd.concat([df_session_train, df_session_val])
df_session.reset_index(inplace=1, drop=1)
df_session.set_index('REVISION_ID', inplace=1)


# In[28]:

df_label_train = feather.read_dataframe('./data_frames/df_label_train.feather')
df_label_val = feather.read_dataframe('./data_frames/df_label_val.feather')
df_label = pd.concat([df_label_train, df_label_val])
df_label.reset_index(inplace=1, drop=1)
df_label.set_index('REVISION_ID', inplace=1)


# In[31]:

df_label.ROLLBACK_REVERTED.value_counts(normalize=1)


# csv to feather

# In[33]:

# In[ ]:

csvs = sorted(glob('../data/processed_wdvc16_*.csv'))

for csv_file in tqdm(csvs):
    new_name = csv_file[len('../data/processed_'):-4]

    df = pd.read_csv(csv_file)
    df.anonimous_ip.fillna('', inplace=1)
    df.revision_comment.fillna('', inplace=1)
    df.user_id.fillna(-1, inplace=1)
    df.user_id = df.user_id.astype('int32')
    del df['page_ns']

    df.revision_timestamp = pd.to_datetime(df.revision_timestamp)

    df['anonimous_meta'] = df_meta.meta_string.loc[df.revision_id].fillna('').reset_index(drop=1)
    df['revision_session_id'] = df_session.REVISION_SESSION_ID.loc[df.revision_id].reset_index(drop=1)
    df['reverted'] = df_label.ROLLBACK_REVERTED.loc[df.revision_id].reset_index(drop=1)

    feather.write_dataframe(df, 'data_frames/' + new_name + '.feather')

