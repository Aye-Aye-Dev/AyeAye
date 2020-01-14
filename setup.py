import setuptools


long_description = """An ETL (Extract, Transform, Load) framework."""

setuptools.setup(
    name="ayeaye",
    version="0.0.1",
    author="Si Parker",
    author_email="si@plogic.co.uk",
    description="ETL framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Aye-Aye-Dev/AyeAye",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'ndjson',
        ],
)
