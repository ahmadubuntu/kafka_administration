# compare_storage.sh executed as Python (mm2_parse import)

Date: 2026-09-10 01:56

Previous plan: `plans/2026-09-10_014412_mm2-logdirs-no-json.md`

## Symptom

On `afr-dt-kmm-eb1`:

```text
File ".../kafka_mirrormaker/./compare_storage.sh", line 10, in <module>
    from mm2_parse import (  # noqa: E402
ModuleNotFoundError: No module named 'mm2_parse'
```

Line 10 of `lib/join_storage.py` is that import. The bash driver line 10 is `SCRIPT_VERSION=...`.
So the file named `compare_storage.sh` on the MM host is the Python joiner (or is being run as Python).
`__file__` is then the toolkit root, not `lib/`, so `mm2_parse` is not on `sys.path`.

`lib/mm2_parse.py` exists; the wrapper was overwritten or copied wrong.

## Fix (v0.1.3)

- `join_storage.py` searches `MM_LIB`, its own dir, and `./lib` for `mm2_parse.py`.
- If invoked as `*.sh` or with `-c` / `--via`, exit 2 with restore instructions.
- Bash scripts export `PYTHONPATH=$MM_LIB` immediately.

## Check on MM host

```bash
head -1 compare_storage.sh
# must be: #!/usr/bin/env bash
# if it is #!/usr/bin/env python3 or "from mm2_parse", restore the bash file
```
