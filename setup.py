import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pybernate",
    version="0.0.1",
    author="Logan Noel",
    description="A Hibernate-like ORM for Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lmnoel/pybernate",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.5',
)