from setuptools import find_packages, setup
from slim import __version__


requirements = [
    "tqdm>=4.49.0",
    "pandas>=1.5.0" "requests>=2.0.0",
]

test_requirements = [
    "pytest",
    "pytest-xdist",
    "pytest-cov",
]

setup(
    name="RelevanceAI Slim",
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
    ),
)
