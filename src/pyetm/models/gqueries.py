'''
Wraps a dict of queries and answers
'''
import pandas as pd

from pyetm.models.base import Base
from pyetm.services.scenario_runners import GetQueryResultsRunner

class Gqueries(Base):
    '''
    We cannot validat yet - as we'd need a servcie connected to the main
    gquery endpoint
    '''
    query_dict: dict

    def query_keys(self) -> list[str]:
        return list(self.query_dict.keys())

    def is_ready(self) -> bool:
        return all((not v is None for v in self.query_dict.values()))

    def update(self, json):
        '''
        Updates the values with a JSON response from the API
        '''
        self.query_dict.update(json)

    def get(self, key):
        '''
        Returns the query value if set, otherwise returns None
        '''
        return self.query_dict.get(key, None)

    def add(self, *query_keys):
        '''
        Add more queries to be requested
        '''
        self.query_dict.update(
            {q : None for q in query_keys if q not in self.query_dict.keys()}
        )

    def execute(self, client, scenario):
        result = GetQueryResultsRunner.run(client, scenario, self.query_keys())

        if result.success:
            self.update(result.data)
        else:
            self.add_warning(f"Error retrieving queries: {result.errors}")

    def to_dataframe(self, values='future'):
        if not self.is_ready():
            return pd.DataFrame()

        df = pd.DataFrame.from_dict(self.query_dict).reindex(['unit']+[values]).T
        df.index.name = 'gquery'
        return df.set_index('unit', append=True)

    @classmethod
    def from_list(cls, query_list: list[str]):
        return cls(query_dict={q : None for q in query_list})

