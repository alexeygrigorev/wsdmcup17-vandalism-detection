import re


def stringify_row(dit):
    d = row.to_dict()
    dict_str = ' '.join('%s=%s' % (k, v.replace(' ', '_')) for (k, v) in d.items() if v)
    return dict_str

def paths(tokens):
    all_paths = ['_'.join(tokens[0:(i+1)]) for i in range(len(tokens))]
    return ' '.join(all_paths)

def ip_features(ip):
    if not ip:
        return ''

    if '.' in ip:
        return paths(ip.split('.'))
    elif ':' in ip:
        return paths(ip.split(':'))
    return ip


re_prop = re.compile(re.escape('/*') + '(.+?)' + re.escape('*/'))
re_links = re.compile(re.escape('[[') + '(.+?)' + re.escape(']]'))

re_punct = re.compile('[' + ''.join(re.escape(p) for p in string.punctuation) + ']')
re_space = re.compile(' +')

def to_unicode(s):
    if isinstance(s, unicode):
        return s
    return s.decode('utf8')


def extract_structured_comment(comment):
    all_found = re_prop.findall(comment)
    command_tokens = set()

    for res in all_found:
        res = res.strip()
        split = res.split('|')

        for tok in split:
            tok = tok.strip()
            if not tok:
                continue

            tok = tok.replace(' ', '_')
            command_tokens.add(tok)
            command_tokens.update(tok.split(':'))

    result = ' '.join(sorted(command_tokens))
    return to_unicode(result).lower()

def extract_links(comment):
    all_found = re_links.findall(comment)
    props = [t.replace(' ', '_') for t in all_found]
    result = ' '.join(props).lower()
    return to_unicode(result).lower()

def extract_unstructured_text(comment):
    comment = re_prop.sub('', comment)
    comment = comment.strip().lower()

    comment = ' '.join(re_punct.split(comment))
    comment = comment.replace('property', '')
    
    comment = re_space.sub(' ', comment).strip()
    return to_unicode(comment).lower()