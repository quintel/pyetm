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
Make sure you have [Python 3](https://www.python.org/downloads/) installed. Then, install all required
libraries by opening a terminal/command-prompt window in the `pyetm` folder (or navigate to this folder
in the terminal using `cd "path/to/scenario-tools-folder"`). All following examples of running the tool
expect you to be in this folder.

#### Using pipenv
It is recommended (but not required) that you use [`pipenv`](https://pipenv.pypa.io/en/latest/) for
running these tools. When using `pipenv` it will create a virtual environment for you. A virtual
environment helps with keeping the libraries you install here separate of your global libraries (in
other words your `pyetm` will be in a stable and isolated environment and are thus less
likely to break when updating things elsewhere on your computer).

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

#### Configuring your settings

You can set your API token and the base url for your requests (depending which
[environment](https://docs.energytransitionmodel.com/api/intro#environments) you want to interact with)
either directly in the ENV or via a config.yml file.

##### config.yml
pyetm uses a `config.yml` file in the project root to store your personal settings:

1. Duplicate the example file provided (`examples/config.example.yml`) and rename it to `config.yml`.
2. Open `config.yml` and fill in your values:
   - **etm_api_token**: Your personal ETM API token (overridden by the `$ETM_API_TOKEN` environment variable if set).
   - **base_url**: The API base URL for the target environment (overridden by the `$BASE_URL` environment
    variable if set) e.g., default pro, a stable engine at `https://2025-01.engine.energytransitionmodel.com/api/v3`,
    or beta at `https://beta.engine.energytransitionmodel.com/api/v3`).
   - **local_engine_url** and **local_model_url**: URLs for a local ETM instance, if running locally.
   - **proxy_servers**: (Optional) HTTP/HTTPS proxy URLs, if required by your network.
   - **csv_separator** and **decimal_separator**: Defaults are `,` and `.`; adjust if your CSV exports
    use different separators.

Your `config.yml` should reside in the root `pyetm/` folder.

##### ENV variables
If you use pyetm as a package, you may want to set your ENV variables using a custom flow. In that
case, the variables you need to set are:

    $ETM_API_TOKEN - Your api token (specific to the environment you are interacting with)
    $BASE_URL - The base url of the environment you are interacting with.
    $LOCAL_ENGINE_URL - The local url of the engine if running locally.
    $LOCAL_MODEL_URL - The local url of the model if running locally.


#TODO - check links
