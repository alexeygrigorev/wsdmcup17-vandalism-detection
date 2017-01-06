# coding: utf-8

import codecs
from tqdm import tqdm
from lxml import etree
from glob import glob
from time import time

from unicodecsv import DictWriter

# In[82]:
page_group = 0

def text_or_none(el):
    if el is None:
        return None
    else:
        return el.text

def parse_page(xml):
    root_el = etree.fromstring(xml)
    page_res = {}
    page_res['page_title'] = text_or_none(root_el.find('title'))
    page_res['page_ns'] = text_or_none(root_el.find('ns'))
    page_res['page_id'] = text_or_none(root_el.find('id'))
    
    global page_group
    page_group = page_group + 1
    page_res['page_group'] = page_group

    revisions = root_el.findall('revision')    
    page_res['revisions_in_group'] = len(revisions)

    results = []

    for rev_el in revisions:
        revision_res = dict(page_res)
        revision_res['revision_id'] = text_or_none(rev_el.find('id'))

        contributor_el = rev_el.find('contributor')
        username = contributor_el.find('username')

        if username is not None:
            revision_res['username'] = username.text
            revision_res['user_id'] = text_or_none(contributor_el.find('id'))
            revision_res['anonimous_ip'] = None
        else:
            revision_res['username'] = None
            revision_res['user_id'] = None
            revision_res['anonimous_ip'] = text_or_none(contributor_el.find('ip'))

        revision_res['revision_comment'] = text_or_none(rev_el.find('comment'))
        revision_res['revision_timestamp'] = text_or_none(rev_el.find('timestamp'))
        # a lot of json - let's put it into a db
        # revision_res['revision_text'] = text_or_none(rev_el.find('text'))
        results.append(revision_res)
    return results


# In[83]:

def stream_pages(file_path):
    with codecs.open(file_path, 'r', 'utf8') as xml:
        acc = []

        for line in xml:
            line = line.strip()

            if line == '<page>':
                acc = [line]
            elif line == '</page>':
                acc.append(line)
                yield ' '.join(acc)
            else:
                acc.append(line)


# In[85]:

def convert_file(file_path):
    result_path = file_path.replace('/wdvc16', '/processed_wdvc16').replace('.xml', '.csv')
    print 'writing to %s...' % result_path

    xml_pages = stream_pages(file_path)

    fieldnames = [u'revision_id', u'revisions_in_group', u'revision_comment', u'revision_timestamp',
                  u'page_id', u'page_group', u'page_ns', u'page_title', 
                  u'anonimous_ip', u'user_id', u'username']

    with open(result_path, 'w') as csv_file:
        writer = DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for xml_page in tqdm(xml_pages):
            pages = parse_page(xml_page)
            for page in pages:
                writer.writerow(page)


xml_files = sorted(glob('../data/wdvc16_*_*.xml'))

for xml_file in xml_files:
    t0 = time()
    print 'processing file %s...' % xml_file
    convert_file(xml_file)
    print 'took %0.3fs' % (time() - t0)