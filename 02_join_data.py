# coding: utf-8

# In[1]:

import pandas as pd
import feather

from glob import glob
from tqdm import tqdm


# read meta information

def stringify_row(row):
    d = row.to_dict()
    dict_str = ' '.join('%s=%s' % (k, v.replace(' ', '_')) for (k, v) in d.items() if v)
    return dict_str


def read_meta_data(filename):
    df = pd.read_csv(filename)
    meta_nas = df.isnull().sum(axis=1)

    df = df[meta_nas != 7].reset_index(drop=1)
    df.REVISION_ID = df.REVISION_ID.astype('uint32')
    df.fillna('', inplace=1)

    del df['REVISION_SESSION_ID']
    df.set_index('REVISION_ID', inplace=1)

    meta_strings = df.apply(stringify_row, axis=1)

    df_res = meta_strings.reset_index()
    df_res.columns = ['REVISION_ID', 'meta_string']
    
    return df_res


df_meta_train = read_meta_data('../data/wdvc16_meta.csv')
df_meta_val = read_meta_data('../data/wdvc16_2016_03_meta.csv')

df_meta = pd.concat([df_meta_train, df_meta_val])
df_meta.reset_index(inplace=1, drop=1)
df_meta.set_index('REVISION_ID', inplace=1)



# reading label information


def read_label_data(filename):
    df = pd.read_csv('../data/wdvc16_truth.csv')
    del df['UNDO_RESTORE_REVERTED']
    df.ROLLBACK_REVERTED = (df.ROLLBACK_REVERTED == 'T').astype('uint8')
    df.REVISION_ID = df.REVISION_ID.astype('uint32')
    return df


df_label_train = read_label_data('../data/wdvc16_truth.csv')
df_label_val = read_label_data('../data/wdvc16_2016_03_truth.csv')

df_label = pd.concat([df_label_train, df_label_val])
df_label.reset_index(inplace=1, drop=1)
df_label.set_index('REVISION_ID', inplace=1)




# csv to feather

csvs = sorted(glob('../data/processed_wdvc16_*.csv'))

for csv_file in tqdm(csvs):
    new_name = csv_file[len('../data/processed_'):-4]

    df = pd.read_csv(csv_file)
    df.anonimous_ip.fillna('', inplace=1)
    df.revision_comment.fillna('', inplace=1)
    df.user_id.fillna(-1, inplace=1)
    df.username.fillna('', inplace=1)
    df.user_id = df.user_id.astype('int32')
    del df['page_ns']

    df.revision_timestamp = pd.to_datetime(df.revision_timestamp)

    metas = df_meta.meta_string.loc[df.revision_id]
    df['anonimous_meta'] = metas.fillna('').reset_index(drop=1)
    labels = df_label.ROLLBACK_REVERTED.loc[df.revision_id]
    df['reverted'] = labels.reset_index(drop=1)

    feather.write_dataframe(df, 'data_frames/' + new_name + '.feather')
