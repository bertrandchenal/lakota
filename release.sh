# Create distribution and publish it to Pypi
rm dist/*
python setup.py sdist --formats=zip
twine upload dist/*
