from setuptools import find_packages, setup

from workflows_core import __version__

requirements = ["tqdm>=4.49.0", "requests>=2.0.0", "pandas>=1.0.0", "pydantic==1.10.2"]

ray_requirements = [
    "numpy>=1.19.0",
    "pyarrow==9.0.0",
    "ray==2.0.0",
]

core_test_requirements = [
    "pytest",
    "pytest-xdist",
    "pytest-cov",
]

example_test_requirements = core_test_requirements + [
    "torch",
    "scikit-learn>=0.20.0",
    "transformers[torch]==4.18.0",
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
        core_tests=core_test_requirements,
        example_tests=example_test_requirements,
        ray=ray_requirements,
    ),
)
