from setuptools import setup, find_packages
import os
import re

current_folder = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(current_folder, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

with open(os.path.join(current_folder, 'featherstore', '__init__.py'), encoding='utf-8') as f:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', f.read(), re.M).group(1)

setup(
    name='FeatherStore',
    version=version,
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
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Programming Language :: Python :: 3.14',
    ],
    python_requires='>=3.11',
    keywords='feather arrow pandas polars datastore',
    packages=find_packages(exclude=['tests', 'docs', 'benchmarks' 'dev']),
    install_requires=[
        'pandas>=2.2.0',
        'polars[timezone]>=1.0.0',
        'pyarrow>=14.0.0',
    ],
    project_urls={
        'Documentation': r'https://featherstore.readthedocs.io/en/stable/'
    },
)
