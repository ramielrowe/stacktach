#!/bin/bash

virtualenv --no-site-packages ".venv"
. .venv/bin/activate
./.venv/bin/easy_install pip
./.venv/bin/pip install -r etc/test-requires.txt
apt-get install libmysql-dev
./.venv/bin/pip install -r etc/pip-requires.txt
./.venv/bin/nosetests tests --exclude-dir=stacktach --with-coverage --cover-package=stacktach,worker,verifier --cover-erase
