from setuptools import setup, find_packages

setup(
    name="era-parser",
    version="1.0.0",
    description="Ethereum Era File Parser for Beacon Chain Data with ClickHouse Support",
    author="Gnosis Data Team",
    packages=find_packages(),
    install_requires=[
        "python-snappy>=0.7.3",
        "pandas>=2.0.3",
        "pyarrow>=12.0.1",
        "numpy>=1.24.3",
        "requests>=2.31.0",
        "clickhouse-connect>=0.8.15"
    ],
    entry_points={
        "console_scripts": [
            "era-parser=era_parser.cli:main",
        ],
    },
    python_requires=">=3.8",
    extras_require={
        "clickhouse": ["clickhouse-connect>=0.6.23"],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Archiving",
        "Topic :: Database",
    ],
)