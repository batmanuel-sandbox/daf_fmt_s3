"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documenation builds.
"""

from documenteer.sphinxconfig.stackconf import build_package_configs
import lsst.daf_fmt_s3


_g = globals()
_g.update(build_package_configs(
    project_name='daf_fmt_s3',
    version=lsst.daf.fmt.s3.version.__version__))
