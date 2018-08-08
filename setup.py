import setuptools

EXTRAS_REQUIRE = {
    "dev": ["pytest", "pytest-mock", "pycodestyle>=2.4.0,<3", "pylint>=1.9.2,<2"]
}

setuptools.setup(
    name="ohub",
    version="0.1",
    python_requires=">=3.6",
    description="Unilever OHUB2.0 Airflow",
    packages=setuptools.find_packages(),
    extras_require=EXTRAS_REQUIRE,
)
