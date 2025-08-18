# Setting up everything for you!


def setup_notebook(debug=False):
    """
    Set up the notebook environment for ETM API usage.

    Args:
        debug (bool): If True, shows full tracebacks. If False, hides them for cleaner output.
    """
    import sys
    import builtins
    from pyetm.config.settings import get_settings
    from IPython import get_ipython
    from IPython.display import display, HTML

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

        def show(obj, *, index=False):
            """Pretty-display DataFrames/Series (HTML) or fall back to normal display."""

            if isinstance(obj, (pd.DataFrame, pd.Series)):
                try:
                    if getattr(obj, "empty", False):
                        from html import escape

                        if isinstance(obj, pd.DataFrame):
                            cols = [str(c) for c in obj.columns]
                            preview = ", ".join(cols[:8]) + (
                                "…" if len(cols) > 8 else ""
                            )
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

                    styler = obj.style
                    styler = styler.format(precision=3)
                    if not index and isinstance(obj, pd.DataFrame):
                        try:
                            styler = styler.hide(axis="index")
                        except Exception:
                            pass
                    display(styler)
                except Exception:
                    display(obj)
            else:
                display(obj)

        # Make 'show' available in the notebook namespace
        ipython.user_ns.setdefault("show", show)
        _orig_print = builtins.print

        def _smart_print(*args, **kwargs):
            if (
                len(args) == 1
                and not kwargs
                and isinstance(args[0], (pd.DataFrame, pd.Series))
            ):
                show(args[0])
            else:
                _orig_print(*args, **kwargs)

        builtins.print = _smart_print

    except Exception as e:
        if debug:
            print(f"Error setting up pandas features: {e}")
            import traceback

            traceback.print_exc()

    print("Environment setup complete")

    # Check if our API is ready!
    try:
        print("  Using ETM API at    ", get_settings().base_url)
        print("  Token loaded?       ", bool(get_settings().etm_api_token))

        if not get_settings().etm_api_token:
            print(
                " Warning: No ETM_API_TOKEN found. Please set your token in the environment."
            )
        else:
            print("API connection ready")
    except Exception as e:
        if debug:
            print(f"Error checking API settings: {e}")
            import traceback

            traceback.print_exc()
        else:
            print("Error checking API settings. Run with debug=True for details.")
