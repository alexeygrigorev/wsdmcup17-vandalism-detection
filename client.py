import sys
import re

import cPickle
from StringIO import StringIO
from lxml import etree

from unicodecsv import reader

from twisted.protocols.basic import IntNStringReceiver
from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet import reactor

import numpy as np
import scipy.sparse as sp


### settings

args = sys.argv

dataserver = args[1] #'localhost:9901'
address, port = dataserver.split(':')
port = int(port)

access_token = args[2] #'wtf-twisted-py'


### the classifier 

print 'reading vectorizers...'
with open('models/title_vec.bin', 'rb') as f:
    title_vec = cPickle.load(f)

with open('models/user_vec.bin', 'rb') as f:
    user_vec = cPickle.load(f)

print 'reading the model...'
with open('models/title_user_svm_946.bin', 'rb') as f:
    svm = cPickle.load(f)


c = 0

def title_ohe_features(rev):
    return title_vec.transform([rev['page_title']])


def paths(tokens):
    all_paths = ['_'.join(tokens[0:(i+1)]) for i in range(len(tokens))]
    return ' '.join(all_paths)

def ip_path_features(ip):
    if not ip:
        return ''
    
    if '.' in ip:
        return paths(ip.split('.'))
    elif ':' in ip:
        return paths(ip.split(':'))
    return ip

def stringify_dict(d):
    dict_str = ' '.join('%s=%s' % (k, v.replace(' ', '_')) for (k, v) in d.items() if v)
    return dict_str

def user_ohe_features(rev, meta):
    ip_paths = ip_path_features(rev['anonimous_ip'])
    meta_string = stringify_dict(meta)
    
    user_id = 'user_id=' + rev['user_id']
    user_string = user_id + ' ' + ip_paths + ' ' + meta_string
    return user_vec.transform([user_string])


def classifier(meta, rev):
    X_title = title_ohe_features(rev)
    X_user = user_ohe_features(rev, meta)
    X = sp.hstack([X_title, X_user], format='csr')

    scores = svm.decision_function(X)
    return scores[0]


### data parsing

re_page_title = re.compile('<title>(.+?)</title>')
re_page_id = re.compile('<id>(.+?)</id>')

def page_info(page_xml):
    page_res = {}
    page_res['page_title'] = re_page_title.findall(page_xml)[0]
    page_res['page_id'] = re_page_id.findall(page_xml)[0]
    return page_res

def text_or_none(el):
    if el is None:
        return None

    return el.text

def revision_info(rev_xml):
    rev_el = etree.fromstring(rev_xml)

    revision_res = {}
    revision_res['revision_id'] = text_or_none(rev_el.find('id'))

    contributor_el = rev_el.find('contributor')
    username = contributor_el.find('username')

    if username is not None:
        revision_res['username'] = username.text
        revision_res['user_id'] = text_or_none(contributor_el.find('id'))
        revision_res['anonimous_ip'] = None
    else:
        revision_res['username'] = None
        revision_res['user_id'] = '-1'
        revision_res['anonimous_ip'] = text_or_none(contributor_el.find('ip'))

    revision_res['revision_comment'] = text_or_none(rev_el.find('comment'))
    revision_res['revision_text'] = text_or_none(rev_el.find('text'))
    revision_res['revision_timestamp'] = text_or_none(rev_el.find('timestamp'))

    return revision_res


### socket reading & wring

class EchoClient(IntNStringReceiver):
    MAX_LENGTH = 9999999
    structFormat = "!i"
    prefixLength = 4

    meta = None
    rev = None
    first = True

    def connectionMade(self):
        print 'writing access_token'
        self.write(access_token)

    def write(self, data):
        self.transport.write(data + '\r\n')        

    def connectionLost(self, reason):
        print "connection lost:", reason
        print 'meta:', self.meta
        print 'rev:', self.rev

    
    def lengthLimitExceeded(self, length):
        raise Exception("the input is too large: %d" % length)

    def stringReceived(self, data):
        if self.meta is None:
            self.meta = unicode(data, 'utf-8')
        elif self.rev is None:
            try:
                self.rev = unicode(data, 'utf-8')
                self.process_data(self.meta, self.rev)
                self.meta = None
                self.rev = None
            except Exception, e:
                print 'got exception in stringReceived:', e
                raise e
        else:
            raise Exception('Unexpected state: both meta and rev are not None')

    def process_data(self, meta, rev):
        try:
            if self.first:
                rev_id, score = self.process_data_first(meta, rev)
                self.write('REVISION_ID,VANDALISM_SCORE')
                self.first = False
            else:
                rev_id, score = self.process_data_others(meta, rev)

            if rev_id is not None:
                print 'processed rev_id=%s' % rev_id
                self.write('%s,%f' % (rev_id, score))
            else:
                print 'rev_id is None! something went wrong!'
                print 'meta:', meta
                print 'rev:', rev
            
        except Exception, e:
            print 'got exception in process_data:', e
            print 'meta:', meta
            print 'rev:', rev
            raise e


    def process_data_first(self, meta, rev):
        r = reader(StringIO(meta))
        self.meta_header = next(r)
        meta_line = next(r)
        meta_rec = dict(zip(self.meta_header, meta_line))

        rev = rev[rev.find('<page>'):]
        page = rev[:rev.find('<revision>')]
        self.last_page_rec = page_info(page)

        rev_xml = rev[rev.find('<revision>'):]
        revision_rec = revision_info(rev_xml)
        revision_rec.update(self.last_page_rec)
        rev_id = revision_rec['revision_id']

        score = classifier(meta_rec, revision_rec)
        return rev_id, score


    def process_data_others(self, meta, rev):
        meta_line = next(reader(StringIO(meta)))
        meta_rec = dict(zip(self.meta_header, meta_line))

        if '</page>' in rev:
            rev = rev[rev.find('<page>'):]
            page = rev[:rev.find('<revision>')]
            self.last_page_rec = page_info(page)

        pos_rev_begin = rev.find('<revision>')
        pos_rev_end = rev.find('</revision>') + len('</revision>')
        rev_xml = rev[pos_rev_begin:pos_rev_end]

        revision_rec = revision_info(rev_xml)
        revision_rec.update(self.last_page_rec)
        rev_id = revision_rec['revision_id']

        score = classifier(meta_rec, revision_rec)
        return rev_id, score


class EchoFactory(ClientFactory):
    protocol = EchoClient

    def clientConnectionFailed(self, connector, reason):
        print "Connection failed - goodbye!", reason
        reactor.stop()

    def clientConnectionLost(self, connector, reason):
        print "Connection lost - goodbye!", reason
        reactor.stop()


factory = EchoFactory()
reactor.connectTCP(address, port, factory)
reactor.run()
