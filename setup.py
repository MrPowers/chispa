from setuptools import setup, find_packages

install_requires = ['pytest']

tests_require = ['pytest']

setup(
    name='chispa',
    version='0.0.1',
    author='Matthew Powers',
    author_email='matthewkevinpowers@gmail.com',
    url='https://github.com/MrPowers/chispa',
    description='Pyspark test helper library',
    long_description='PySpark test helper functions with pretty error messages',
    license='APACHE',
    packages=find_packages(),
    install_requires=install_requires,
    tests_require=tests_require,
    setup_requires=[],
    python_requires='>=2.7',
    extras_require={
        'test': tests_require,
        'all': install_requires + tests_require,
        'docs': ['sphinx'] + tests_require,
        'lint': []
    },
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6'
    ],
    dependency_links=[],
    include_package_data=False,
    keywords=['apachespark', 'spark', 'pyspark'],
)
