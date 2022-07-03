from setuptools import setup

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="model",
    version="0.1",
    packages=["model"],
    install_requires=required,
)
