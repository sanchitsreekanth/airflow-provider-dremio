# How to contribute

We are welcome to accepting patches and contributions to this provider package. Please refer to the guidelines mentioned in this section for contributing to this project


## Development Environment
This project uses [hatch](https://hatch.pypa.io/latest/) as the build and development tool of choice. It is one of popular build tools and environment managers for Python, maintained by the Python Packaging Authority.

### Installing hatch
You can install hatch using any way mentioned [here](https://hatch.pypa.io/latest/install/). The easiest way would be installation via pip or [pipx](https://github.com/pypa/pipx)
```commandline
pipx install hatch
```

### Using hatch to manage python versions
You can also use hatch to install and manage airflow virtualenvs and development environments. For example, you can install Python 3.8 with this command:
```commandline
hatch python install 3.8
```

### Manage your development environment with hatch
There are already some predefined environments available for development and testing purposes in this project. To see the available environments run th command:
```commandline
hatch env show
```
Currently, this will show two environments
* `default` - This envirinment will have python 3.8 and other requirements installed for this project. You can use this for development purposes
* `test` - This environment can be used for running unit tests as it has pytest installed with it.

Create the environment by using the command:
```commandline
hatch env create
```
If no name is mentioned it will use the default environment. Once inside run the command:
```commandline
pip install --editable .
```
You can activate the shell with this environment using the command:
```commandline
hatch -e default shell
```
You can also see where hatch created the virtualenvs and use it in your IDE or activate it manually:
```commandline
hatch env find default
```
You can specify the python binary inside the bin folder of this path to you IDE so that you can use it for local development


## Configure pre commit
Before commiting your changes to github, certain checks for code quality standards have been implemented such as linting, code syntax, formatting etc. These are done via [pre-commit](https://pre-commit.com/) hooks.
<br></br>
Once you have activated the hatch environment shell as mentioned in the section above install pre-commit using the command:
```commandline
pip install pre-commit
```
Once it is installed, you need to install and activate the pre commit checks:
```commandline
pre-commit install
```
Once this is done on every commit pre commit checks will be run automatically. To do a manual pre commit check, run :
```commandline
pre-commit run --all-files
```

## Testing
While contributing, make sure there are apt unit test cases written for your code. All tests are within the `tests` folder. You can use the test environment to run and check your unit tests. To create the test environment run the command:
```commandline
hatch env create test
```
Then you need to activate the shell with this environment:
```commandline
hatch -e test shell
```
Once this is done you can run the unit tests using [pytest](https://docs.pytest.org/en/8.0.x/).
```commandline
pytest -s
```

## Code reviews

All submissions to this project requires code review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests. You can submit a pull request to the ```main``` branch. Make sure that you have rebased
