from os import path

import setuptools

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setuptools.setup(
    name="timbermill",
    version="0.0.8",
    author="Shimon Klebanov",
    author_email="sklebanov@salesforce.com",
    description="A Task-Based, Context-Aware Logging service",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/datorama/Timbermill",
    packages=setuptools.find_packages(),
    install_requires=[
        'requests',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
