# Setting up everything for you!


def setup_notebook():
    import sys
    import pprint

    from pyetm.config.settings import get_settings

    # Hide the trackback for now

    ipython = get_ipython()

    def hide_traceback(
        exc_tuple=None,
        filename=None,
        tb_offset=None,
        exception_only=False,
        running_compiled_code=False,
    ):
        etype, value, tb = sys.exc_info()
        return ipython._showtraceback(
            etype, value, ipython.InteractiveTB.get_exception_only(etype, value)
        )

    ipython.showtraceback = hide_traceback

    print("Environment setup complete")

    # Check if our API is ready!

    print("  Using ETM API at    ", get_settings().base_url)
    print("  Token loaded?       ", bool(get_settings().etm_api_token))

    if not get_settings().etm_api_token:
        print(
            " Warning: No ETM_API_TOKEN found. Please set your token in the environment."
        )
    else:
        print("API connection ready")
