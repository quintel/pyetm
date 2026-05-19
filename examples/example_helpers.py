"""Helper functions for example notebooks."""


def setup_logging(level="WARNING"):
    """
    Configure logging for pyetm to show errors and warnings from batch operations.

    Args:
        level (str): Logging level. Options: 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
                    - 'WARNING' (default): Shows warnings and errors from batch operations
                    - 'INFO': Shows informational messages plus warnings/errors
                    - 'ERROR': Only shows critical errors
                    - 'DEBUG': Shows all logging output (verbose)

    Example:
        # Show all warnings from batch operations (recommended)
        setup_logging('WARNING')

        # Show detailed debug information
        setup_logging('DEBUG')

        # Only show critical errors
        setup_logging('ERROR')
    """
    import logging

    # Get the pyetm logger
    logger = logging.getLogger("pyetm")
    logger.setLevel(getattr(logging, level.upper()))

    # Create console handler if not already present
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(getattr(logging, level.upper()))

        # Create formatter
        formatter = logging.Formatter("%(levelname)s [%(name)s.%(module)s]: %(message)s")
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    # Prevent propagation to root logger to avoid duplicate messages
    logger.propagate = False

    return logger


def setup_notebook(debug=False, logging_level="WARNING"):
    """
    Set up the notebook environment for ETM API usage.

    Configures pandas display options, IPython traceback handling, and pyetm logging.
    By default, configures logging at WARNING level to show errors from batch operations.

    Args:
        debug (bool): If True, shows full tracebacks. If False, hides them for cleaner output.
        logging_level (str): Logging level for pyetm. Options: 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
                            Default is 'WARNING' to show batch operation errors.

    Example:
        # Standard setup (shows warnings and errors)
        setup_notebook()

        # Verbose setup (shows all debug output)
        setup_notebook(debug=True, logging_level='DEBUG')

        # Quiet setup (only critical errors)
        setup_notebook(logging_level='ERROR')
    """
    import sys
    import builtins
    from pyetm.config.settings import get_settings
    import warnings
    from IPython import get_ipython
    from IPython.display import display, HTML

    warnings.filterwarnings("ignore", category=FutureWarning)

    # Configure logging for pyetm
    setup_logging(logging_level)

    # Handle traceback display based on debug mode
    ipython = get_ipython()

    if not debug:
        # Hide the traceback for a cleaner demo experience
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
    else:
        if hasattr(ipython, "_original_showtraceback"):
            ipython.showtraceback = ipython._original_showtraceback
        else:
            ipython._original_showtraceback = ipython.showtraceback

    try:
        import pandas as pd

        pd.set_option("display.max_rows", 60)
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", None)
        pd.set_option("display.max_colwidth", None)
        pd.options.display.float_format = "{:,.3f}".format

        try:
            pd.options.styler.render.max_elements = 200000
        except Exception:
            pass

        def show(obj):
            """Pretty-display DataFrames/Series (HTML) or fall back to normal display."""

            if isinstance(obj, (pd.DataFrame, pd.Series)):
                try:
                    if getattr(obj, "empty", False):
                        from html import escape

                        if isinstance(obj, pd.DataFrame):
                            cols = [str(c) for c in obj.columns]
                            preview = ", ".join(cols[:8]) + ("…" if len(cols) > 8 else "")
                            meta = f" — 0 rows, {obj.shape[1]} columns"
                            extra = f" (columns: {escape(preview)})" if cols else ""
                            msg = f"Empty DataFrame{meta}{extra}"
                        else:
                            msg = "Empty Series — 0 rows"
                        display(
                            HTML(
                                "<div style='font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; color: #374151; margin: 4px 0;'>"
                                + escape(msg)
                                + "</div>"
                            )
                        )
                        return

                    styler = obj.style.format(precision=3)
                    display(styler)
                except Exception:
                    display(obj)
            else:
                display(obj)

        # Make 'show' available in the notebook namespace
        ipython.user_ns.setdefault("show", show)
        _orig_print = builtins.print

        def _smart_print(*args, **kwargs):
            if len(args) == 1 and not kwargs and isinstance(args[0], (pd.DataFrame, pd.Series)):
                show(args[0])
            else:
                _orig_print(*args, **kwargs)

        builtins.print = _smart_print

    except Exception as e:
        print(f"Error setting up pandas features: {e}")
        if debug:
            import traceback

            traceback.print_exc()

    print("Environment setup complete")

    # Check if our API is ready!
    try:
        print("  Using ETM API at    ", get_settings().base_url)
        print("  Token loaded?       ", bool(get_settings().etm_api_token))

        if not get_settings().etm_api_token:
            print(
                " Warning: No ETM_API_TOKEN found. You will only be able to access public scenarios without authentication."
            )
        else:
            print("API connection ready")
    except Exception as e:
        print(f"Error checking API settings: {e}")
        if debug:
            import traceback

            traceback.print_exc()
