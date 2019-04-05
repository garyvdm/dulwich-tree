Release Steps
=============

    git tag -a 0.x -m "Version 0.x"
    git push --tags
    python setup.py sdist upload -r pypi
