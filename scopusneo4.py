from neo4j import GraphDatabase
import pybliometrics as sc

def insertlResearcher(tx, rname, authid,  rtype):
    result = tx.run(""" CREATE (l:LeicRes {name: $name, type:$type, id:$id}) """, name=rname, id=authid, type=rtype)
    return list(result)

def insertLresearchers(leiclist, lauthors):
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "leicesterresearchers")

    with GraphDatabase.driver(URI, auth=AUTH) as driver, driver.session() as session:
        for auth in leiclist:
            authn = lauthors[auth][0]['author']
            with driver.session(database="neo4j") as session:
                people = session.execute_write(
                    insertlResearcher,
                    authn, auth, "Leicester"
                )
def insertoResearcher(tx, rname, authid,  rtype):
    result = tx.run(""" CREATE (e:ExtRes {name: $name, type:$type, id:$id}) """, name=rname, id=authid, type=rtype)
    return list(result)

def insertOresearchers(otherlist, oauthors):
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "leicesterresearchers")

    with GraphDatabase.driver(URI, auth=AUTH) as driver, driver.session() as session:
        for auth in otherlist:
            authn = oauthors[auth][0]['author']
            with driver.session(database="neo4j") as session:
                people = session.execute_write(
                    insertoResearcher,
                    authn, auth, "External"
                )

def insertKeyword(tx, rname, rtype):
    result = tx.run(""" CREATE (k:Keyword {name: $name, type:$type}) """, name=rname, type=rtype)
    return list(result)

def insertKWS(kwlist):
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "leicesterresearchers")

    with GraphDatabase.driver(URI, auth=AUTH) as driver, driver.session() as session:
        for tkw in kwlist:
            with driver.session(database="neo4j") as session:
                people = session.execute_write(
                    insertKeyword,
                    tkw, "keyword"
                )

def createlpubwith(tx, ida, idb, relw):
    result = tx.run(""" Match (res1:LeicRes {id:$ra})
    Match (res2:LeicRes {id:$rb})
    MERGE (res1)-[:PUBWITH {pc:$cn}]->(res2) """,ra=ida, rb=idb, cn=relw)
    return list(result)
def createopubwith(tx, ida, idb, relw):
    result = tx.run(""" Match (res1:LeicRes {id:$ra})
    Match (res2:ExtRes {id:$rb})
    MERGE (res1)-[:PUBWITH {pc:$cn}]->(res2) """,ra=ida, rb=idb, cn=relw)
    return list(result)

def createLpubwiths(lpubtog):
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "leicesterresearchers")
    with GraphDatabase.driver(URI, auth=AUTH) as driver, driver.session() as session:
        for pair in lpubtog:
            people = session.execute_write(
                createlpubwith, pair[0], pair[1], lpubtog[pair]
            )

def createOpubwiths(opubtog):
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "leicesterresearchers")
    with GraphDatabase.driver(URI, auth=AUTH) as driver, driver.session() as session:
        for pair in opubtog:
            people = session.execute_write(
                createopubwith, pair[0], pair[1], opubtog[pair]
            )

def createkwrel(tx, ida, idb, relw):
    result = tx.run(""" Match (res1:LeicRes {id:$ra})
    Match (kw:Keyword {name:$rb})
    MERGE (res1)-[:HAS {pc:$cn}]->(kw) """,ra=ida, rb=idb, cn=relw)
    return list(result)

def createKWrels(lauthkeys):
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "leicesterresearchers")
    with GraphDatabase.driver(URI, auth=AUTH) as driver, driver.session() as session:
        for auth in lauthkeys:
            people = session.execute_write(
                createkwrel, auth[0], auth[1], lauthkeys[auth]
        )

def insertAff(tx, af, rname, rcity, raltn, rtype):
    result = tx.run(""" CREATE (k:Affiliation {id: $id, name: $name, city: $city, altn: $altnames, type:$type}) """, id=af, name=rname, type=rtype, city=rcity, altnames = raltn)
    return list(result)

def insertAffs(aflist, affinfo ):
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "leicesterresearchers")

    with GraphDatabase.driver(URI, auth=AUTH) as driver, driver.session() as session:
        for af in aflist.keys():
            if af == '' or af == 'NA' or af not in affinfo.keys():
                afname = 'NA'
                afcity = 'NA'
                altnames = 'NA'
            else:
                afname = affinfo[af].affiliation_name
                afcity = affinfo[af].city
                altnames = ''
                for nv in affinfo[af].name_variants:
                    altnames = altnames + '| ' + nv.name
            # print(af, afname, afcity, altnames)
            with driver.session(database="neo4j") as session:
                people = session.execute_write(
                    insertAff,
                    af, afname, afcity, altnames, "keyword"
                )

def createLafrel(tx, ida, idb):
    result = tx.run(""" Match (res1:LeicRes {id:$ra})
    Match (kw:Affiliation {id:$rb})
    MERGE (res1)-[:AFTO]->(kw) """,ra=ida, rb=idb)
    return list(result)

def createLAFrels(authafs, leiclist):
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "leicesterresearchers")
    with GraphDatabase.driver(URI, auth=AUTH) as driver, driver.session() as session:
        for auth in authafs:
            if auth in leiclist:
                for af in authafs[auth]:
                    people = session.execute_write(
                    createLafrel, auth, af
        )

def createOafrel(tx, ida, idb):
    result = tx.run(""" Match (res1:ExtRes {id:$ra})
    Match (kw:Affiliation {id:$rb})
    MERGE (res1)-[:AFTO]->(kw) """,ra=ida, rb=idb)
    return list(result)

def createOAFrels(authafs, otherlist):
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "leicesterresearchers")
    with GraphDatabase.driver(URI, auth=AUTH) as driver, driver.session() as session:
        for auth in authafs:
            if auth in otherlist:
                for af in authafs[auth]:
                    people = session.execute_write(
                    createOafrel, auth, af
        )