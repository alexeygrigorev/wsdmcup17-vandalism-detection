# coding: utf-8

# In[76]:

from glob import glob
from time import time
import re

import cPickle

from unicodecsv import DictWriter
from collections import Counter

import pandas as pd
import numpy as np

import feather
from tqdm import tqdm


# In[57]:

## TODO: add IP features!

def compute_counter_features(row):
    is_reverted = row['reverted']
    ts = row['revision_timestamp']
    title = row['page_title']
    user_id = row['user_id']
    ip = row['anonimous_ip']

    row_result = {'revision_id': row['revision_id']}
    is_anon = user_id == -1

    # user/ip counters 

    if is_anon:
        user_id = ip

    is_user_first_edit = user_id not in user_first_edit
    row_result['user_first_edit'] = int(is_user_first_edit)

    user_total_edits[user_id] = user_total_edits[user_id] + 1
    row_result['user_total_edits'] = user_total_edits[user_id]

    user_reverted_edits[user_id] = user_reverted_edits[user_id] + is_reverted
    row_result['user_reverted_edits'] = user_reverted_edits[user_id]

    if is_user_first_edit:
        user_first_edit[user_id] = ts
        user_last_edit[user_id] = ts

        row_result['user_days_since_first_edit'] = 0
        row_result['user_minutes_since_last_edit'] = 0

    else:
        first_edit = ts - user_first_edit[user_id]
        row_result['user_days_since_first_edit'] = first_edit.days

        last_edit = ts - user_last_edit[user_id]
        row_result['user_minutes_since_last_edit'] = last_edit.seconds / 60

        user_last_edit[user_id] = ts


    # page counters    
    
    new_page = title not in page_first_edit
    row_result['page_first_edit'] = int(new_page)
    
    page_total_edits[title] = page_total_edits[title] + 1
    row_result['page_total_edits'] = page_total_edits[title]

    page_reverted_edits[title] = page_reverted_edits[title] + is_reverted
    row_result['page_reverted_edits'] = page_reverted_edits[title]

    if new_page:
        page_first_edit[title] = ts
        page_last_edit[title] = ts

        row_result['page_days_since_first_edit'] = 0
        row_result['page_minutes_since_last_edit'] = 0

    else:
        first_edit = ts - page_first_edit[title]
        row_result['page_days_since_first_edit'] = first_edit.days
        
        last_edit = ts - page_last_edit[title]
        row_result['page_minutes_since_last_edit'] = last_edit.seconds / 60

        page_last_edit[title] = ts


    return row_result


# In[58]:

fieldnames = [
        u'revision_id',
        u'page_first_edit', u'page_total_edits',u'page_reverted_edits', 
        u'page_days_since_first_edit', u'page_minutes_since_last_edit', 
        u'user_first_edit', u'user_total_edits', u'user_reverted_edits', 
        u'user_days_since_first_edit', u'user_minutes_since_last_edit']


# In[ ]:

yearmonth_re = re.compile(r'\d{4}_\d{2}')
feather_files = sorted(glob('./data_frames/*.feather'))


# In[ ]:

user_first_edit = {}
user_last_edit = {}
user_total_edits = Counter()
user_reverted_edits = Counter()

page_first_edit = {}
page_last_edit = {}
page_total_edits = Counter()
page_reverted_edits = Counter()

# todo: some aggs on IP details

for f in feather_files:
    print 'processing %s' % f

    yearmonth = yearmonth_re.findall(feather_files[0])[0]

    df = feather.read_dataframe(f)

    df.revision_comment.fillna('', inplace=1)
    df.user_id = df.user_id.astype('int32')
    del df['page_ns']
    df.reset_index(drop=1, inplace=1)

    csv_file_name = 'counter_features/counter_features_%s.csv' % yearmonth

    with open(csv_file_name, 'w') as csv_file:
        writer = DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for _, row in tqdm(df.iterrows()):
            row_result = compute_counter_features(row)
            writer.writerow(row_result)

    print 'csv saved to', csv_file_name
            
    cnts = {
        'user_first_edit': user_first_edit,
        'user_last_edit': user_last_edit,
        'user_total_edits': user_total_edits,
        'user_reverted_edits': user_reverted_edits,

        'page_first_edit': page_first_edit,
        'page_last_edit': page_last_edit,
        'page_total_edits': page_total_edits,
        'page_reverted_edits': page_reverted_edits,
    }

    counter_dict_file_name = 'counters/cnt_%s.bin' % yearmonth
    with open(counter_dict_file_name, 'wb') as f:
        cPickle.dump(cnts, f)

    print 'counter dictionaries saved to', counter_dict_file_name