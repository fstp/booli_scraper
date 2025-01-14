from setuptools import find_packages, setup

setup(
    name="booli_dagster",
    packages=find_packages(exclude=["booli_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
