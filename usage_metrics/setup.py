"""Setup script to make usage_metrics directly installable with pip."""
import setuptools

setuptools.setup(
    name="usage_metrics",
    packages=setuptools.find_packages(exclude=["usage_metrics_tests"]),
    install_requires=[
        "dagster==0.14.3",
        "dagit==0.14.3",
        "pytest",
    ],
)
