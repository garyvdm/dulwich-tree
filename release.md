Release Steps
=============

* Change version in pyproject.toml
* `git commit pyproject.toml -m "Version 0.2"` 
* `git tag -a 0.x -m "Version 0.x"`
* `git push --tags`
* `flit publish`
