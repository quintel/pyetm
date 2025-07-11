
from pyetm.models import Gqueries


def test_queries_from_list(valid_queries):
    queries = Gqueries.from_list(valid_queries)

    assert not queries.is_ready()
    assert valid_queries[0] == queries.query_keys()[0]

def test_update(valid_queries):
    queries = Gqueries.from_list(valid_queries)

    assert not queries.is_ready()

    queries.update({
        valid_queries[0]: 20.5,
        valid_queries[1]: 1.0
    })

    assert queries.is_ready()
    assert queries.get(valid_queries[0]) == 20.5

    assert queries.get('invalid_query') is None


def test_add_one_query(valid_queries):
    queries = Gqueries.from_list(valid_queries)

    queries.add('extra_query')

    assert not queries.is_ready()
    assert 'extra_query' in queries.query_keys()
    assert queries.get('extra_query') is None
    assert valid_queries[0] in queries.query_keys()
    assert queries.get(valid_queries[0]) is None


def test_add_one_query_when_queries_were_already_run(valid_queries):
    queries = Gqueries.from_list(valid_queries)

    queries.update({
        valid_queries[0]: 20.5,
        valid_queries[1]: 1.0
    })

    queries.add('extra_query')

    assert not queries.is_ready()
    assert 'extra_query' in queries.query_keys()
    assert queries.get('extra_query') is None
    assert queries.get(valid_queries[0]) == 20.5


def test_add_mulitple_queries(valid_queries):
    queries = Gqueries.from_list(valid_queries)

    queries.add('extra_query', 'extra_query_2')

    assert not queries.is_ready()
    assert 'extra_query' in queries.query_keys()
    assert 'extra_query_2' in queries.query_keys()

    assert queries.get('extra_query') is None
    assert queries.get('extra_query_2') is None



def test_add_multiple_queries_but_one_is_already_present(valid_queries):
    queries = Gqueries.from_list(valid_queries)

    queries.update({
        valid_queries[0]: 20.5,
        valid_queries[1]: 1.0
    })


    queries.add('extra_query', valid_queries[0])

    assert not queries.is_ready()
    assert 'extra_query' in queries.query_keys()
    # Was not overwritten
    assert queries.get(valid_queries[0]) == 20.5


def test_to_dataframe(valid_queries):
    queries = Gqueries.from_list(valid_queries)

    queries.update({
        valid_queries[0]: {'present': 0.0, 'future': 20.5, 'unit': 'euros'},
        valid_queries[1]: {'present': 1.0, 'future': 1.0, 'unit': 'PJ'}
    })

    dataframe = queries.to_dataframe()

    assert dataframe['unit'][valid_queries[0]] == 'euros'
    assert dataframe['future'][valid_queries[1]] == 1.0
