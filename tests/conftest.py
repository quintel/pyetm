'''
Runs during test collection. You can also supply fixtures here that should be loaded
before each test
'''

import os, sys, importlib, pytest

os.environ['ETM_API_TOKEN'] = 'real-token'
os.environ['BASE_URL']       = 'https://example.com/api'

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC  = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

