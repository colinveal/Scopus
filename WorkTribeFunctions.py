import csv
from neo4j import GraphDatabase

def importWT(file):
    WT = []
    researchers = set()
    funders = set()
    partners = set()
    with open('Projects.csv',encoding='utf-8') as csvfile:
        worktribe = csv.reader(csvfile)
        funders = set()
        researchers = set()
        partners = set()
        for row in worktribe:
            for f in row[17].strip().split(','):
                if f !='':
                    funders.add(f)
            for f in row[34].strip().split(','):
                if f !='':
                    researchers.add(f)
            for f in row[35].strip().split(','):
                if f !='':
                    researchers.add(f)
            for f in row[41].strip().split(','):
                if f !='':
                    partners.add(f)
            for f in row[42].strip().split(','):
                if f !='':
                    partners.add(f)
            for f in row[43].strip().split(','):
                if f !='':
                    partners.add(f)
            for f in row[44].strip().split(','):
                if f !='':
                    partners.add(f)
            projdata = {'ptitle':row[0], 'pid':row[3], 'funder':row[17].split(','), 'pi':row[34].split(','), 'cois':row[35].split(','), 'colabs':row[41].split(','), 'suba':row[42].split(','), 'partners':row[43].split(','), 'otherOs':row[44].split(','), 'status':row[6], 'amount':row[103]}
            WT.append(projdata)
    return WT, funders, researchers, partners

def insertProject(tx, ptitle, pid, st, am):
    result = tx.run(""" CREATE (p:Project {title: $title, id:$id, status:$status, amount:$amount}) """, title=ptitle, id=pid, status=st, amount=am)
    return list(result)

def insertProjects(WT):
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "leicesterresearchers")

    with GraphDatabase.driver(URI, auth=AUTH) as driver, driver.session() as session:
        for proj in WT:
            ptitle = proj['ptitle']
            pid = proj['pid']
            st = proj['status']
            am = proj['amount']
            print(ptitle)
            with driver.session(database="neo4j") as session:
                people = session.execute_write(
                    insertProject,
                    ptitle, pid, st, am
                )

def insertFunder(tx, fname):
    result = tx.run(""" CREATE (p:Funder {name: $name}) """, name=fname)
    return list(result)

def insertFunders(funders):
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "leicesterresearchers")

    with GraphDatabase.driver(URI, auth=AUTH) as driver, driver.session() as session:
        for f in funders:
            with driver.session(database="neo4j") as session:
                people = session.execute_write(
                    insertFunder,
                    f
                )

def insertPartner(tx, fname):
    result = tx.run(""" CREATE (p:Partner {name: $name}) """, name=fname)
    return list(result)

def insertPartners(partners):
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "leicesterresearchers")

    with GraphDatabase.driver(URI, auth=AUTH) as driver, driver.session() as session:
        for f in partners:
            with driver.session(database="neo4j") as session:
                people = session.execute_write(
                    insertPartner,
                    f
                )
def insertResearcher(tx, fname):
    result = tx.run(""" CREATE (p:Researcher {name: $name}) """, name=fname)
    return list(result)

def insertResearchers(researchers):
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "leicesterresearchers")

    with GraphDatabase.driver(URI, auth=AUTH) as driver, driver.session() as session:
        for f in researchers:
            with driver.session(database="neo4j") as session:
                people = session.execute_write(
                    insertResearcher,
                    f
                )

def createprojtofunder(tx, ida, idb):
    result = tx.run(""" Match (res1:Project {id:$ra})
    Match (res2:Funder {name:$rb})
    MERGE (res1)-[:FUNDEROF]->(res2) """,ra=ida, rb=idb)
    return list(result)
def createProjtoResearcher(tx, ida, idb):
    result = tx.run(""" Match (res1:Project {id:$ra})
    Match (res2:Researcher {name:$rb})
    MERGE (res1)-[:PARTOF]->(res2) """,ra=ida, rb=idb)
    return list(result)
def createProjtoPartner(tx, ida, idb):
    result = tx.run(""" Match (res1:Project {id:$ra})
    Match (res2:Partner {name:$rb})
    MERGE (res1)-[:PARTOF]->(res2) """,ra=ida, rb=idb)
    return list(result)

def createWTRels(WT):
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "leicesterresearchers")
    with GraphDatabase.driver(URI, auth=AUTH) as driver, driver.session() as session:
        for proj in WT:
            for f in proj['funder']:
                if f != '':
                    people = session.execute_write(
                        createprojtofunder, proj['pid'], f
                    )
            for r in proj['pi']:
                if r != '':
                    people = session.execute_write(
                        createProjtoResearcher, proj['pid'], r
                    )
            for r in proj['cois']:
                if r != '':
                    people = session.execute_write(
                        createProjtoResearcher, proj['pid'], r
                    )
            for r in proj['colabs']:
                if r != '':
                    people = session.execute_write(
                        createProjtoPartner, proj['pid'], r
                    )
            for r in proj['suba']:
                if r != '':
                    people = session.execute_write(
                        createProjtoPartner, proj['pid'], r
                    )
            for r in proj['partners']:
                if r != '':
                    people = session.execute_write(
                        createProjtoPartner, proj['pid'], r
                    )
            for r in proj['otherOs']:
                if r != '':
                    people = session.execute_write(
                        createProjtoPartner, proj['pid'], r
                    )

def createResearcherAlias(tx, ida, idb):
    result = tx.run(""" Match (res1:Researcher {name:$ra})
    Match (res2:LeicRes) where toLower(res2.name) contains toLower($rb)
    MERGE (res1)-[:CRUDEALIAS]->(res2) """,ra=ida, rb=idb)
    return list(result)

def createAlias(researchers):
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "leicesterresearchers")
    with GraphDatabase.driver(URI, auth=AUTH) as driver, driver.session() as session:
        for r in researchers:
            if len(r.split(' ')) > 1:
                print(r, r.split(' ')[-2])
                people = session.execute_write(
                    createResearcherAlias, r, r.split(' ')[-2]
                )

