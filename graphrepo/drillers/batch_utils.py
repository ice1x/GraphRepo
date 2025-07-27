"""This module is the wild wild west of batch indexing :-)
In contains all Neo4j queries for indexing the data in batches.
More documentation will follow soon.
"""
from datetime import datetime


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def index_commits(graph, commits, batch_size=100):
    query = """
        UNWIND $commits AS c
        MERGE (nc :Commit { hash: c.hash})
          ON CREATE SET
                    nc = c
          ON MATCH SET
                    nc = c
    """
    for b in batch(commits, batch_size):
        graph.run(query, commits=b)


def index_parent_commits(graph, parents, batch_size=100):
    query = """
        UNWIND $ac AS a
        MATCH (x:Commit),(y:Commit)
        WHERE x.hash = a.parent_hash AND y.hash = a.child_hash
        MERGE (x)-[r:Parent{}]->(y)
    """
    for b in batch(parents, batch_size):
        graph.run(query, ac=b)


def index_authors(graph, authors, batch_size=100):
    query = """
        UNWIND $authors AS a
        MERGE (nd:Developer { hash: a.hash})
          ON CREATE SET nd = a
          ON MATCH SET nd = a
    """
    for b in batch(authors, batch_size):
        graph.run(query, authors=b)


def index_branches(graph, branches, batch_size=100):
    query = """
        UNWIND $branches AS a
        MERGE (nb:Branch { hash: a.hash})
          ON CREATE SET nb = a
          ON MATCH SET nb = a
    """
    for b in batch(branches, batch_size):
        graph.run(query, branches=b)


def index_branch_commits(graph, bc, batch_size=100):
    query = """
        UNWIND $ac AS a
        MATCH (x:Branch),(y:Commit)
        WHERE x.hash = a.branch_hash AND y.hash = a.commit_hash
        MERGE (x)-[r:BranchCommit{}]->(y)
    """
    for b in batch(bc, batch_size):
        graph.run(query, ac=b)


def index_files(graph, files, batch_size=100):
    query = """
        UNWIND $files AS f
        MERGE (nf:File { hash: f.hash})
          ON CREATE SET nf = f
          ON MATCH SET nf = f
    """
    for b in batch(files, batch_size):
        graph.run(query, files=b)


def index_methods(graph, methods, batch_size=100):
    query = """
        UNWIND $methods AS f
        MERGE (nm:Method { hash: f.hash})
          ON CREATE SET nm = f
          ON MATCH SET nm = f
    """

    for b in batch(methods, batch_size):
        graph.run(query, methods=b)


def index_author_commits(graph, ac, batch_size=100):
    query = """
        UNWIND $ac AS a
        MATCH (x:Developer),(y:Commit)
        WHERE x.hash = a.author_hash AND y.hash = a.commit_hash
        MERGE (x)-[r:Author{timestamp: a.timestamp}]->(y)
    """
    for b in batch(ac, batch_size):
        graph.run(query, ac=b)


def index_commit_files(graph, cf, batch_size=100):
    query = """
        UNWIND $cf AS a
        MATCH (x:Commit),(y:File)
        WHERE x.hash = a.commit_hash AND y.hash = a.file_hash
        MERGE (x)-[r:UpdateFile{}]->(y)
        ON CREATE SET r=a['attributes']
    """
    for i, b in enumerate(batch(cf, batch_size)):
        graph.run(query, cf=b)


def index_file_methods(graph, cf, batch_size=100):
    query = """
        UNWIND $cf AS a
        MATCH (x:File),(y:Method)
        WHERE x.hash = a.file_hash AND y.hash = a.method_hash
        MERGE (x)-[r:Method{}]->(y)
    """
    for b in batch(cf, batch_size):
        graph.run(query, cf=b)


def index_commit_method(graph, cm, batch_size=100):
    query = """
        UNWIND $cf AS a
        MATCH (x:Commit),(y:Method)
        WHERE x.hash = a.commit_hash AND y.hash = a.method_hash
        MERGE (x)-[r:UpdateMethod]->(y)
        ON CREATE SET r=a['attributes']
    """
    for i, b in enumerate(batch(cm, batch_size)):
        graph.run(query, cf=b)


def create_index_authors(graph):
    query = (
        "CREATE INDEX index_developer_hash IF NOT EXISTS "
        "FOR (n:Developer) ON (n.hash)"
    )
    graph.run(query)


def create_index_commits(graph, hash=True):
    if hash:
        hash_q = (
            "CREATE INDEX index_commit_hash IF NOT EXISTS "
            "FOR (n:Commit) ON (n.hash)"
        )
        graph.run(hash_q)

    pid_q = (
        "CREATE INDEX index_commit_project_id IF NOT EXISTS "
        "FOR (n:Commit) ON (n.project_id)"
    )
    graph.run(pid_q)


def create_index_branches(graph, hash=True):
    if hash:
        hash_q = (
            "CREATE INDEX index_branch_hash IF NOT EXISTS "
            "FOR (n:Branch) ON (n.hash)"
        )
        graph.run(hash_q)

    pid_q = (
        "CREATE INDEX index_branch_project_id IF NOT EXISTS "
        "FOR (n:Branch) ON (n.project_id)"
    )
    graph.run(pid_q)


def create_index_files(graph, hash=True):
    if hash:
        hash_q = (
            "CREATE INDEX index_file_hash IF NOT EXISTS "
            "FOR (n:File) ON (n.hash)"
        )
        graph.run(hash_q)

    mhash_q = (
        "CREATE INDEX index_file_merge_hash IF NOT EXISTS "
        "FOR (n:File) ON (n.merge_hash)"
    )
    graph.run(mhash_q)

    pid_q = (
        "CREATE INDEX index_file_project_id IF NOT EXISTS "
        "FOR (n:File) ON (n.project_id)"
    )
    graph.run(pid_q)


def create_index_methods(graph, hash=True):
    if hash:
        hash_q = (
            "CREATE INDEX index_method_hash IF NOT EXISTS "
            "FOR (n:Method) ON (n.hash)"
        )
        graph.run(hash_q)

    mhash_q = (
        "CREATE INDEX index_method_merge_hash IF NOT EXISTS "
        "FOR (n:Method) ON (n.merge_hash)"
    )
    graph.run(mhash_q)

    pid_q = (
        "CREATE INDEX index_method_project_id IF NOT EXISTS "
        "FOR (n:Method) ON (n.project_id)"
    )
    graph.run(pid_q)


def merge_renamed_files(graph, project_id):
    query = """
        MATCH (f:File {project_id: $pid})
        WITH f.merge_hash AS mh, collect(f) AS nodes
        WHERE size(nodes) > 1
        CALL apoc.refactor.mergeNodes(nodes,
           {properties:'overwrite', mergeRels:true}) YIELD node
        WITH node
        MATCH (node)-[]->(m:Method)
        SET m.merge_hash = node.hash
    """
    graph.run(query, {"pid": project_id})


def merge_new_files(graph, project_id):
    query = """
        MATCH (f:File {project_id: $pid})
        WITH f.hash AS h, collect(f) AS nodes
        WHERE size(nodes) > 1
        CALL apoc.refactor.mergeNodes(nodes,
           {properties:'overwrite', mergeRels:true}) YIELD node
        WITH node
        MATCH (node)-[]->(m:Method)
        SET m.merge_hash = node.hash
    """
    graph.run(query, {"pid": project_id})


def merge_methods(graph, project_id):
    query = """
        MATCH (m:Method {project_id: $pid})
        WITH m.merge_hash AS mh, m.file_name AS fn, m.name AS n, collect(m) AS methods
        WHERE size(methods) > 1
        CALL apoc.refactor.mergeNodes(methods,
           {properties:'overwrite', mergeRels:true}) YIELD node
        RETURN node
    """
    graph.run(query, {"pid": project_id})


def merge_files(graph, config):
    print('Merging moved files and methods')
    start = datetime.now()
    merge_renamed_files(graph, config.project_id)
    merge_methods(graph, config.project_id)
    merge_new_files(graph, config.project_id)
    merge_methods(graph, config.project_id)
    print('Merged files and methods \t', datetime.now() - start)


def index_all(graph, developers, commits, parents, dev_commits, branches,
              branches_commits, files, commit_files, methods, file_methods,
              commit_methods, config):
    total = datetime.now()

    batch_size = config.batch_size

    developers = list({v['hash']: v for v in developers}.values())
    print('Indexing ', len(developers), ' authors')
    start = datetime.now()
    index_authors(graph, developers, batch_size)
    print('Indexed authors in: \t', datetime.now() - start)

    print('Indexing ', len(commits), ' commits')
    start = datetime.now()
    index_commits(graph, commits, batch_size)
    print('Indexed commits in: \t', datetime.now() - start)

    branches = list({v['hash']: v for v in branches}.values())
    branches_commits = list({str(i): i for i in branches_commits}.values())
    print('Indexing ', len(branches), ' branches')
    start = datetime.now()
    index_branches(graph, branches, batch_size)
    index_branch_commits(graph, branches_commits, batch_size)
    print('Indexed branches in: \t', datetime.now() - start)

    files = list({v['hash']: v for v in files}.values())
    print('Indexing ', len(files), ' files')
    start = datetime.now()
    index_files(graph, files, batch_size)
    print('Indexed files in: \t', datetime.now() - start)

    methods = list({v['hash']: v for v in methods}.values())
    print('Indexing ', len(methods), ' methods')
    start = datetime.now()
    index_methods(graph, methods, batch_size)
    print('Indexed methods in: \t', datetime.now() - start)

    parents = list({str(i): i for i in parents}.values())
    print('Indexing ', len(parents), ' parent commits')
    start = datetime.now()
    index_parent_commits(graph, parents, batch_size)
    print('Indexed commits in: \t', datetime.now() - start)

    print('Indexing ', len(dev_commits), ' author_commits')
    start = datetime.now()
    index_author_commits(graph, dev_commits, batch_size)
    print('Indexed author_commits in: \t', datetime.now() - start)

    file_methods = list({str(i): i for i in file_methods}.values())
    print('Indexings ', len(file_methods), ' file_methods')
    start = datetime.now()
    index_file_methods(graph, file_methods, batch_size)
    print('Indexed file_methods in: \t', datetime.now() - start)

    print('Indexing ', len(commit_methods), ' commit_methods')
    start = datetime.now()
    index_commit_method(graph, commit_methods, batch_size)
    print('Indexed commit_methods in: \t', datetime.now() - start)

    print('Indexing ', len(commit_files), ' commit_files')
    start = datetime.now()
    index_commit_files(graph, commit_files, batch_size)
    print('Indexed commit_files in: \t', datetime.now() - start)
    print('Indexing took: \t', datetime.now() - total)


def index_cache(graph, cache, config):
    batch_size = config.batch_size
    total = datetime.now()
    index_authors(graph, list(
        {v['hash']: v for v in cache.data['developers']}.values()), batch_size)
    index_commits(graph, cache.data['commits'], batch_size)
    index_branches(graph, list(
        {v['hash']: v for v in cache.data['branches']}.values()), batch_size)
    index_branch_commits(graph, list(
        {str(i): i for i in cache.data['branches_commits']}.values()), batch_size)
    index_files(graph, list(
        {v['hash']: v for v in cache.data['files']}.values()), batch_size)
    index_methods(graph, list(
        {v['hash']: v for v in cache.data['methods']}.values()), batch_size)
    index_parent_commits(graph, list(
        {str(i): i for i in cache.data['parents']}.values()), batch_size)
    index_author_commits(graph, cache.data['dev_commits'], batch_size)
    index_file_methods(graph, list(
        {str(i): i for i in cache.data['file_methods']}.values()), batch_size)
    index_commit_method(graph, cache.data['commit_methods'], batch_size)
    index_commit_files(graph, cache.data['commit_files'], batch_size)
    print('Indexing took: \t', datetime.now() - total)
