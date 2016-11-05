import sys
import re

from StringIO import StringIO
from lxml import etree

from unicodecsv import reader

from twisted.protocols.basic import IntNStringReceiver
from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet import reactor

### settings

args = sys.argv

dataserver = args[1] #'localhost:9901'
address, port = dataserver.split(':')
port = int(port)

access_token = args[2] #'wtf-twisted-py'


### the classifier 

c = 0

def classifier(meta, rev):
    global c
    c = c + 1
    return c / 1000.0


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
        revision_res['user_id'] = None
        revision_res['anonimous_ip'] = text_or_none(contributor_el.find('ip'))

    revision_res['revision_comment'] = text_or_none(rev_el.find('comment'))
    revision_res['revision_text'] = text_or_none(rev_el.find('text'))
    revision_res['revision_timestamp'] = text_or_none(rev_el.find('timestamp'))

    return revision_res


### socket reading & wring

class EchoClient(IntNStringReceiver):
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

    def stringReceived(self, data):
        if self.meta is None:
            self.meta = data
        elif self.rev is None:
            self.rev = data
            self.process_data(self.meta, self.rev)
            self.meta = None
            self.rev = None
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

            print rev_id
            self.write('%s,%f' % (rev_id, score))
        except Exception, e:
            print e
            print meta
            print rev
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
        print "Connection failed - goodbye!"
        reactor.stop()

    def clientConnectionLost(self, connector, reason):
        print "Connection lost - goodbye!"
        reactor.stop()


factory = EchoFactory()
reactor.connectTCP(address, port, factory)
reactor.run()