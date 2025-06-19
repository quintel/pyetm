# pyetm

This package provides a set of tools for interaction with the Energy Transition Model's API. Learn more
about the Energy Transition Model [here](https://energytransitionmodel.com/). The
package is designed to be a modular tool that advanced users can incorporate into their workflows. The
complete documentation is available [via the ETM documentation page](https://docs.energytransitionmodel.com/main/pyetm/introduction).

## Installation

You can clone the pyetm from [our Github](https://github.com/quintel/pyetm). The package is also
available via pip like any other python package - install it and use it in your project!
```
pip install pyetm
```

## Getting started
Make sure you have [Python 3](https://www.python.org/downloads/) installed. Then, install all required libraries by opening a terminal/command-prompt window in the `pyetm` folder (or navigate to this folder in the terminal using `cd "path/to/scenario-tools-folder"`). All following examples of running the tool expect you to be in this folder.

#### Using pipenv
It is recommended (but not required) that you use [`pipenv`](https://pipenv.pypa.io/en/latest/) for running these tools. When using `pipenv`
it will create a virtual environment for you. A virtual environment helps with keeping the libraries you install here separate of your global libraries (in
other words your `scenario-tools` will be in a stable and isolated environment and are thus less likely to break when updating things elsewhere on your computer)
and this one comes with some nice shortcuts for running the tools.

You can install `pipenv` with `pip` or `pip3` if you don't have it installed yet.
```
pip3 install pipenv
```

Then you can create a new environment and install all the libraries in one go by running:
```
pipenv install
```

If you plan to develop with the tool, install the dev dependencies too:
```
pipenv install --dev
```







#TODO - check links
