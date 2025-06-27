import pandas as pd

from pydantic import BaseModel
from pathlib import Path
from typing import Optional

from pyetm.clients import BaseClient
from pyetm.services.custom_curves import download_curve
from pyetm.config.settings import get_settings

class CustomCurve(BaseModel):
    '''
    Wrapper around a single custom curve.
    Curves are getting saved to the filesystem, as bulk processing of scenarios
    could end up with several 100 MBs of curves, which we don't want to keep in
    memory.
    '''
    key: str
    type: str
    file_path: Optional[Path] = None

    def available(self):
        return bool(self.file_path)

    def retrieve(self, client, scenario):
        ''' Process curve from client, save to file, set file_path'''
        result = download_curve(client, scenario, self.key)

        if result.success:
            curve = pd.read_csv(result.data, index_col=False, dtype=float)
            self.file_path = get_settings().path_to_tmp(str(scenario.id)) / f'{self.key}.csv'
            curve.to_csv(self.file_path, index=False)
            return curve
        else:
            # TODO: log the error on the object, so we can collect a bunch of
            # them to give back to the user!
            return result.errors

    def contents(self) -> pd.Series:
        ''' Open file from path and return contents'''
        if not self.available():
            return

        return pd.read_csv(
            self.file_path, index_col=False, dtype=float
        ).squeeze('columns').dropna(how='all')

    def remove(self):
        '''TODO: destroy file and remove path'''


class CustomCurves(BaseModel):
    curves: list['CustomCurve']

    def __len__(self):
        return len(self.curves)

    def __iter__(self):
        yield from iter(self.curves)

    def attached_keys(self):
        ''' Returns the keys of attached curves '''
        yield from (curve.key for curve in self.curves)

    def get_contents(self, scenario, curve_name: str):
        curve = self._find(curve_name)

        if not curve:
            return

        if not curve.available():
            return curve.retrieve(BaseClient(), scenario)
        else:
            return curve.contents()

    def _find(self, curve_name: str) -> CustomCurve | None:
        return next((c for c in self.curves if c.key == curve_name), None)

    @classmethod
    def from_json(cls, data):
        return cls(curves=[CustomCurve(**curve_data) for curve_data in data])
