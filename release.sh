# Create distribution and publish it to Pypi
rm dist/*
python setup.py sdist --formats=zip
twine upload dist/*
rm dist/*

# Build doc
pdoc --html --html-dir ../bertrandchenal.github.io/ lakota --force
echo "Don't forget to commit and push doc"
