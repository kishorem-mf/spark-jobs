import numpy
from Cython.Build import cythonize
from Cython.Distutils import build_ext
from setuptools import setup, Extension

from string_matching import __version__

ext_utils = Extension(
    'string_matching.sparse_dot_topn',
    sources=['./string_matching/sparse_dot_topn.pyx',
             './string_matching/sparse_dot_topn_source.cpp'],
    include_dirs=[numpy.get_include()],
    extra_compile_args=['-std=c++0x', '-Os'],
    language='c++',
)

setup(
    name='string_matching',
    version=__version__,
    setup_requires=[
        # Setuptools 18.0 properly handles Cython extensions.
        'setuptools>=18.0',
        'cython',
    ],
    packages=['string_matching'],
    cmdclass={'build_ext': build_ext},
    ext_modules=cythonize([ext_utils]),
)
