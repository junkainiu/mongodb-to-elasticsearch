from setuptools import setup
import pipetrans

setup(
    name='mongodb-to-elasticsearch',
    version='0.1',
    description='Transform mongo aggregate pipeline to elasticsearch query',
    url='https://github.com/junkainiu/mongo_to_elasticsearch',
    author='junkainiu',
    author_email='junkainiu@gmail.com',
    license='MIT',
    packages=['pipetrans'],
    zip_safe=False
)
