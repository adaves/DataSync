from setuptools import setup, find_packages

setup(
    name="datasync",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyodbc>=4.0.39",
        "pandas>=2.0.0",
        "python-dateutil>=2.8.2",
        "click>=8.0.0",
        "openpyxl>=3.0.0",
        "xlrd>=2.0.1",
        "pyyaml>=6.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "datasync=datasync.cli:main",
        ],
    },
    author="adam daves",
    author_email="adam.daves@thaiunion.com",
    description="A Python-based data synchronization tool for Microsoft Access databases with automatic file discovery and optimal import methods",
    long_description="DataSync provides automated Excel-to-Access data import with file discovery, one-by-one processing optimization, and comprehensive data management features.",
    long_description_content_type="text/plain",
    url="https://github.com/yourusername/DataSync",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
) 