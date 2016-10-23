# coding: utf-8

import codecs
from tqdm import tqdm
from lxml import etree
from glob import glob
from time import time

from unicodecsv import DictWriter

# In[82]:
page_group = 0

def parse_page(xml):
    root_el = etree.fromstring(xml)
    page_res = {}
    page_res['page_title'] = root_el.find('title').text
    page_res['page_ns'] = root_el.find('ns').text
    page_res['page_id'] = root_el.find('id').text
    
    global page_group
    page_group = page_group + 1
    page_res['page_group'] = page_group

    revisions = root_el.findall('revision')    
    page_res['revisions_in_group'] = len(revisions)

    results = []

    for rev_el in revisions:
        revision_res = dict(page_res)
        revision_res['revision_id'] = rev_el.find('id').text

        contributor_el = rev_el.find('contributor')
        username = contributor_el.find('username')

        if username is not None:
            revision_res['username'] = username.text
            revision_res['user_id'] = contributor_el.find('id').text
            revision_res['anonimous_ip'] = None
        else:
            revision_res['username'] = None
            revision_res['user_id'] = None
            revision_res['anonimous_ip'] = contributor_el.find('ip').text

        revision_res['revision_comment'] = rev_el.find('comment').text
        revision_res['revision_text'] = rev_el.find('text').text
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
    
    xml_pages = stream_pages(file_path)
    
    fieldnames = [u'revision_id', u'revisions_in_group', u'revision_text', u'revision_comment',
                  u'page_id', u'page_group', u'page_ns', u'page_title', u'page_group', 
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