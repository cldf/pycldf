
Releasing pycldf
================

- Do platform test via tox:
```
tox -r
```

- Make sure statement coverage is at 100%
- Make sure flake8 passes:
```
flake8
```

- Change version to the new version number in

  - `setup.py`
  - `src/pycldf/__init__.py`

- Bump version number:
```
git commit -a -m"bumped version number"
```

- Create a release tag:
```
git tag -a v<version> -m"first version to be released on pypi"
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
```
git push origin
git push --tags
```

- Change version for the next release cycle, i.e. incrementing and adding .dev0

  - `setup.py`
  - `src/pycldf/__init__.py`

- Commit/push the version change:
```shell
git commit -a -m "bump version for development"
git push origin
```
