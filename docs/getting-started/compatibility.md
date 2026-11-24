# Compatibility

Each pyetm line targets one ETM Version. The table says which line to install for the Version you work on.

| pyetm | Talks to | Versions |
|---|---|---|
| 2.1.x | The ETM's single API at `my.energytransitionmodel.com` | `latest`. Scenarios on other Versions load with a warning, untested. |
| 2.0.x | Each Version's engine directly | Every Version, including stable ones such as `2025-01`, until the engine's direct API access for tokens is retired |

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
