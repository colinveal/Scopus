import pandas as pd
import itertools
import pybliometrics.scopus as sc
pd.set_option('display.max_colwidth', None) #need to set otherwise author list is cut in pandas dataframe
from neo4j import GraphDatabase

def getKW(pubkws):
    kw = []
    if pubkws != None:
        for k in pubkws.split('|'):
            kw.append(k.strip())
    return kw

def getauthsafs(a_afids, a_names, a_ids, eid):
    if a_afids == None:  # may need to check for creator to keep those ones #taken article from scopus fixes most of these
        print('warn no affiliation ids', eid)
        afids = []
        auth = []
        authids = []
        ab = sc.AbstractRetrieval(eid, view='FULL')
        for a in ab.authors:
            authids.append(a.auid)
            auth.append(a.indexed_name)
            afids.append(a.affiliation)
    else:
        afids = a_afids.split(';')
        auth = a_names.split(';')
        authids = a_ids.split(';')

    if len(authids) == 99:
        afids = []
        auth = []
        authids = []
        print('alert over 99 authors')
        ab = sc.AbstractRetrieval(eid, view='FULL')
        for a in ab.authors:
            authids.append(a.auid)
            auth.append(a.indexed_name)
            afids.append(a.affiliation)
    return afids, auth, authids



def procArticles(articles, years):
    lauthors = {}
    oauthors = {}
    authafs = {}
    lpubtog = {}
    opubtog = {}
    lauthkeys = {}

    leiclist = set()
    otherlist = set()
    kwlist = set()
    aflist = {}

    for year in years:
        print(year)
        for index, row in articles[year].iterrows():
            if row.eid == '2-s2.0-85168343644':
                print(row.author_names)

            #get keywords for that article, put into array and remove leading and trailing spaces
            kw = getKW(row.authkeywords)

            # create list of authors, authorids, affiliation ids
            afids, auth, authids = getauthsafs(row.author_afids, row.author_names, row.author_ids, row.eid)

            # loop through each affiliation id and place into dictionaries for leicester authors, other authors, pub/leicester auth.
            leicauthp = set()
            oauthp = set()
            afsp = set()

            for af, author, authorid in zip(afids, auth, authids):
                if af == None: af = 'NA'
                afarray = af.replace(';','-').split('-')
                afsp.update(afarray)
                if any(item in ['60007974', '60033125', '60171766'] for item in afarray):   #leicester authors
                    leicauthp.add(authorid)
                    if authorid in lauthors.keys():
                        lauthors[authorid].append({'author':author,'kw':kw, 'eid':row.eid , 'title':row.title})
                    else:
                        lauthors[authorid] = []
                        lauthors[authorid].append({'author':author,'kw':kw, 'eid':row.eid , 'title':row.title})
                else:
                    oauthp.add(authorid)
                    if authorid in oauthors.keys():
                        oauthors[authorid].append({'author':author,'kw':kw, 'eid':row.eid , 'title':row.title})
                    else:
                        oauthors[authorid] = []
                        oauthors[authorid].append({'author':author,'kw':kw, 'eid':row.eid , 'title':row.title})

                if authorid in authafs:
                    authafs[authorid].update(afarray)
                else:
                    authafs[authorid] = set()
                    authafs[authorid].update(afarray)

            for pair in list(itertools.combinations(leicauthp,2)):
                if pair in lpubtog.keys():
                    lpubtog[pair] += 1
                elif (pair[1], pair[0]) in lpubtog.keys():
                    lpubtog[(pair[1], pair[0])] += 1
                else:
                    lpubtog[pair] = 1

            for la in leicauthp:
                for oa in oauthp:
                    if (la,oa) in opubtog.keys():
                        opubtog[(la,oa)] += 1
                    else:
                        opubtog[(la,oa)] = 1

                for k in kw:
                    if (la,k) in lauthkeys.keys():
                        lauthkeys[(la,k)] += 1
                    else:
                        lauthkeys[(la,k)] = 1

            leiclist.update(leicauthp)

            otherlist.update(oauthp)
            kwlist.update(kw)
            for aff in afsp:
                if aff in aflist.keys():
                    aflist[aff] += 1
                else:
                    aflist[aff] = 1

    return lauthors, oauthors, authafs, lpubtog, opubtog, lauthkeys, leiclist, otherlist, kwlist, aflist
