from setuptools import setup, find_packages


def requirements():
    with open('requirements.txt') as f:
        return f.read().splitlines()


def version():
    with open('./camels_de1h/__version__.py') as f:
        code = f.read()
    loc = dict()
    exec(code, loc, loc)
    return loc['__version__']

setup(
    name='camels_de1h',
    description='Camels data processing helper for hourly data',
    author='Alexander Dolich',
    author_email='alexander.dolich@kit.edu',
    install_requires=requirements(),
    license='CC0',
    version=version(),
    packages=find_packages()
)