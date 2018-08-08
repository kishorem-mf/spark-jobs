import setuptools

REQUIREMENTS = ["apache-airflow[crypto]~=1.9.0"]

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
