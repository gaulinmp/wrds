import os

from setuptools import setup, find_packages

def read(*paths):
    """Build a file path from *paths* and return the contents."""
    with open(os.path.join(*paths), 'r') as f:
        return f.read()

setup(
    name='wrds',
    version='0.1.0',
    description='Python interface for WRDS data.',
    long_description=(read('README.md')),
    url='http://github.com/edwinhu/wrds/',
    license='MIT',
    author='Edwin Hu',
    author_email='eddyhu@gmail.com',
    packages=find_packages(exclude=['tests*']),
    install_requires=['sqlalchemy', 'pandas', 'numpy'],
    include_package_data=True,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
