import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="timbermill-shim52",
    version="0.1",
    author="Shimon Klebanov",
    author_email="shimonkle@gmail.com",
    description="A Task-Based, Context-Aware Logging service",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/datorama/Timbermill",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
