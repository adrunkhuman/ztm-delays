"""Build the required extension; never substitute a Python backend."""

from Cython.Build import cythonize
from setuptools import Extension, setup

setup(
    ext_modules=cythonize(
        [
            Extension(
                "_ztm_routing",
                ["native/kernel.pyx"],
                depends=["native/engine.hpp"],
                include_dirs=["native"],
                language="c++",
                extra_compile_args=["-O3", "-std=c++17", "-ffp-contract=off"],
            ),
        ],
        build_dir="build/cython",
        compiler_directives={"language_level": 3, "boundscheck": True, "wraparound": True},
    ),
)
