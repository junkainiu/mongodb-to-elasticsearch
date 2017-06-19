from setuptools import setup

import pipetrans

setup(
    name='mongodb-to-elasticsearch',
    version=pipetrans.__version__,
    author=pipetrans.__auther__,
    author_email='junkainiu@gmail.com',
    description='Transform mongo aggregate pipeline to elasticsearch query',
    url='https://github.com/junkainiu/mongo_to_elasticsearch',
    author='junkainiu',
    author_email='junkainiu@gmail.com',
    license='MIT',
    packages=['pipetrans'],
    zip_safe=False
)
