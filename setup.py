import setuptools

REQUIREMENTS = [
    "apache-airflow[crypto]>=1.9.0,<2",
    "azure-storage-blob>=1.3.1,<2",
    "azure>=1.0.2,<2",
    "google-api-python-client>=1.7.4,<2",
    "numpy~=1.14.0",
    "oauth2client>=4.1.2,<5",
    "pandas-gbq>=0.5.0,<1",
    "paramiko>=2.4.1,<3",
]

EXTRAS_REQUIRE = {
    "dev": [
        "pycodestyle>=2.4.0,<3",
        "pylint>=1.9.2,<2",
        "pytest-cov>=2.5.1,<3",
        "pytest-mock",
        "pytest>=3.7.1,<4",
    ]
}

setuptools.setup(
    name="ohub",
    version="0.1",
    python_requires="~=3.6.6",
    description="Unilever OHUB2.0 Airflow",
    packages=setuptools.find_packages(),
    install_requires=REQUIREMENTS,
    extras_require=EXTRAS_REQUIRE,
)
