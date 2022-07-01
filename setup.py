from setuptools import setup

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="model",
    version="0.1",
    packages=["model"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=required,
)
