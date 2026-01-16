## Databricks yml

bundle:

- name: Required to run the bundle, this is what makes the bundle "unique", unless the uuid is defined.
- uuid: It is a good practice to add a uuid, it may solve conflicts if a user (or a service principal) has two bundles with the same name

mode: development

- Deployed resources get prefixed with '[dev my_user_name]'
- Any job schedules and triggers are paused by default.
- Activates Source-based deployment
  See also https://docs.databricks.com/dev-tools/bundles/deployment-modes.html.

mode: production
It is recommended to add an explicit and unique root_path defined to avoid duplication of production resources.

## Code suggestions

When running workflows in Databricks, spark sessions and dbutils are inherited from the runtime. While the code may be okay, code linters may detect the lack of definitions as errors.

This code block allows to bypass the errors while maintaining consistency

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.runtime import spark
```

## uv

uv is a fast Python package and project manager that can replace pip, virtualenv, and pip-tools in a single workflow. It installs dependencies, manages virtual environments, and keeps your project metadata in `pyproject.toml` so the dependency list lives next to your code.

When creating a bundle project from the default, the pyproject.toml is already created.

Minimal cheatsheet:

```sh
uv sync                 # install/update deps from pyproject.toml + lockfile
uv venv                 # create a .venv in the project
source .venv/bin/activate  # activate the venv (bash/zsh)
uv add --dev pytest     # add pytest to dev dependencies in pyproject.toml
uv add package          # add package to general dependencies in pyproject.toml
```

## ruff
ruff is a fast Python linter and formatter that can replace multiple tools (flake8, isort, black) with a single, configurable CLI. It reads configuration from `pyproject.toml`, so lint and format settings live with the project.

Minimal cheatsheet:
```sh
uv add --dev ruff       # add ruff as a dev dependency
uv run ruff check .     # lint the project
uv run ruff format .    # format code in place
```
