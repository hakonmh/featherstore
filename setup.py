from setuptools import setup, find_packages
import os
from featherstore import __version__

current_folder = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(current_folder, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='FeatherStore',
    version=__version__,
    description='High performance datastore built upon Apache Arrow & Feather',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url=r'https://github.com/Hakonmh/featherstore',
    author='Håkon Magne Holmen',
    author_email='haakonholmen@hotmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Database',
        'Topic :: Database :: Database Engines/Servers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    python_requires='>=3.8',
    keywords='feather arrow pandas polars datastore',
    packages=find_packages(exclude=['tests', 'docs', 'benchmarks' 'dev']),
    install_requires=[
        'numpy>=1.21.0',
        'pandas>=1.1.0',
        'polars>=0.10.16',
        'pyarrow>=5.0.0',
    ],
    project_urls={
        'Documentation': r'https://featherstore.readthedocs.io/en/latest/'
    },
)
