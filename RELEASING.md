
Releasing pycldf
================

- Do platform test via tox:
  ```shell
  tox -r
  ```

- Make sure statement coverage >= 99%
- Make sure flake8 passes:
  ```shell
  flake8 src
  ```

- Make sure the docs render:
  ```shell
  cd docs
  make clean html
  ```

- Update the version number, by removing the trailing `.dev0` in:
  - `setup.py`
  - `src/pycldf/__init__.py`
  - `docs/conf.py`
  - and edit `CHANGELOG.md`

- Create the release commit:
  ```shell
  git commit -a -m "release <VERSION>"
  ```

- Create a release tag:
  ```shell
  git tag -a v<VERSION> -m"<VERSION> release"
  ```

- Release to PyPI (see https://github.com/di/markdown-description-example/issues/1#issuecomment-374474296):
  ```shell
  rm dist/*
  python setup.py sdist
  twine upload dist/*
  rm dist/*
  python setup.py bdist_wheel
  twine upload dist/*
  ```

- Push to github:
  ```shell
  git push origin
  git push --tags
  ```

- Change version for the next release cycle, i.e. incrementing and adding .dev0
  - `setup.py`
  - `src/pycldf/__init__.py`
  - `docs/conf.py`

- Commit/push the version change:
  ```shell
  git commit -a -m "bump version for development"
  git push origin
  ```
