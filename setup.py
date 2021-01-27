import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="secret_statistic_bercy139",  # Replace with your own username
    version="0.0.1",
    author="kaa_serpent",
    description="Apply secret on datas",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/139bercy/secret_stat",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)