# Compatibility

Each pyetm line is tested on one ETM Version. Since 2.1, pyetm reaches every Version through one API, so a line can also work on other Versions whose inputs, curves and exports still have the names it expects.

| pyetm | Talks to | Versions |
|---|---|---|
| 2.1.x | The ETM's single API at `my.energytransitionmodel.com` | Tested on `latest`. Other Versions load with a warning. Not `2025-01`, whose curve and export names differ |
| 2.0.x | Each Version's engine directly | Every Version, including stable ones such as `2025-01`, until the engine's direct API access for tokens is retired |

## Working across Versions

One pyetm can serve work that spans several Versions, so you do not need a second virtual environment. Loading a scenario on a Version other than `ETM_VERSION` warns once, because that pairing is untested.

A token works with both lines. pyetm 2.0 reaches stable Versions through the engine's legacy token support, which is temporary.

## Release lines

When ETM forks a stable Version, pyetm forks with it:

1. The current pyetm minor, for example 2.1, gets a maintenance branch. Its default `ETM_VERSION` becomes the stable tag, for example `2027-01`, and it receives fixes only.
2. `main` moves on to the next minor, for example 2.2, which targets `latest`.
3. This table gains a row for the new line.

So the newest pyetm always targets `latest`, and each stable Version keeps the pyetm line that was current when it forked. This is first exercised at the 2027 fork.

To pin a line, install it explicitly:

```bash
pip install "pyetm>=2.1,<2.2"
```
