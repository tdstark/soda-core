#!/usr/bin/env python
import sys

from setuptools import find_namespace_packages, setup

if sys.version_info < (3, 7):
    print("Error: Soda Core requires at least Python 3.7")
    print("Error: Please upgrade your Python version to 3.7 or later")
    sys.exit(1)

package_name = "soda-core"
# Managed by tbump - do not change manually
package_version = "3.0.41"
description = "Soda Core"

requires = [
    "markupsafe>=2.0.1,<=2.1.1",
    "Jinja2>=2.11,<4.0",
    "click~=8.0",
    "ruamel.yaml>=0.17.0,<0.18.0",
    "requests~=2.27",
    "antlr4-python3-runtime~=4.11.1",
    "openlineage-python~=0.29.2",
    "opentelemetry-api~=1.16.0",
    "opentelemetry-exporter-otlp-proto-http~=1.16.0",
    "sqlparse~=0.4",
    "inflect~=6.0",
]

setup(
    name=package_name,
    version=package_version,
    author="Soda Data N.V.",
    author_email="info@soda.io",
    description="Soda Core library & CLI",
    long_description_content_type="text/markdown",
    packages=find_namespace_packages(include=["soda*"]),
    install_requires=requires,
    entry_points={"console_scripts": ["soda=soda.cli.cli:main"]},
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.7",
)
