from setuptools import find_packages, setup

with open("README.md") as f:
    long_description = f.read()

with open("requirements.in") as f:
    install_requires = f.read().splitlines()

with open("requirements.dev.in") as f:
    dev_requires = f.read().splitlines()[1:]  # Elide `-c requirements.txt` constraint

setup(
    name="openalex-terminusdb",
    url="https://github.com/polyneme/openalex-terminusdb",
    packages=find_packages(
        include=[
            "openalex_terminusdb*",
        ],
        exclude=["tests", "tests2"],
    ),
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    author="Donny Winston",
    author_email="donny@polyneme.xyz",
    description="sync the OpenAlex snapshot to a TerminusDB database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
    ],
    install_requires=install_requires,
    extras_require={
        "dev": dev_requires,
    },
    python_requires=">=3.10",
    entry_points={"console_scripts": []},
)
