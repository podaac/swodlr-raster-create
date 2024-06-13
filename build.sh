#!/bin/bash
set -eo pipefail

BUNDLE_PLUGIN_INSTALLED=$(poetry self show plugins | (grep 'poetry-plugin-bundle' || true) | wc -l)
if [ "$BUNDLE_PLUGIN_INSTALLED" -ne 1 ]; then
  echo "Poetry bundle plugin missing! - https://github.com/python-poetry/poetry-plugin-bundle"
  echo "Install the poetry bundle plugin with: poetry self add poetry-plugin-bundle"
  exit 1
fi

PACKAGE_NAME=$(awk -F' = ' '{gsub(/"/,"");if($1=="name")print $2}' pyproject.toml)
VERSION=$(poetry version -s)

ROOT_PATH="$PWD"
ZIP_PATH="$ROOT_PATH/dist/$PACKAGE_NAME-$VERSION.zip"

poetry bundle venv --clear --without=dev --python=$(which python3.9) build

cd build/lib/python3.*/site-packages
touch podaac/__init__.py
rm -rf *.dist-info _virtualenv.*
find . -type d -name __pycache__ -exec rm -rf {} \+

mkdir -p "$ROOT_PATH/dist/"
rm -f "$ZIP_PATH"
zip -vr9 "$ZIP_PATH" .
