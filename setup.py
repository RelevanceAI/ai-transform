from setuptools import find_packages, setup

from core import __version__

requirements = [
    "tqdm>=4.49.0",
    "numpy==1.23.3",
    "pandas>=1.0.0",
    "requests>=2.0.0",
    "pytest==7.1.3",
    "requests==2.28.1",
    "setuptools==65.3.0",
]

ray_requirements = [
    "pyarrow==9.0.0",
    "ray==2.0.0",
]

test_requirements = [
    "pytest",
    "pytest-xdist",
    "pytest-cov",
]

setup(
    name="RelevanceAI Workflows Core",
    version=__version__,
    url="https://relevance.ai/",
    author="Relevance AI",
    author_email="dev@relevance.ai",
    packages=find_packages(),
    setup_requires=["wheel"],
    install_requires=requirements,
    package_data={
        "": [
            "*.ini",
        ]
    },
    extras_require=dict(
        tests=test_requirements,
        ray=ray_requirements,
    ),
)
