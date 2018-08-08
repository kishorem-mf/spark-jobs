import setuptools

EXTRAS_REQUIRE = {"dev": ["pytest", "pytest-mock"]}

setuptools.setup(
    name="ohub",
    version="0.1",
    python_requires=">=3.6",
    description="Unilever OHUB2.0 Airflow",
    packages=setuptools.find_packages(),
    extras_require=EXTRAS_REQUIRE,
)
