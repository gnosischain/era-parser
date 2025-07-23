from setuptools import setup, find_packages

setup(
    name="era-parser",
    version="1.0.0",
    description="Ethereum Era File Parser for Beacon Chain Data",
    author="Gnosis Data Team",
    packages=find_packages(),
    install_requires=[
        "python-snappy>=0.7.3",
        "pandas>=2.0.3",
        "pyarrow>=12.0.1",
        "numpy>=1.24.3",
        "requests>=2.31.0"
    ],
    entry_points={
        "console_scripts": [
            "era-parser=era_parser.cli:main",
        ],
    },
    python_requires=">=3.8",
)