#
# LSST Data Management System
# Copyright 2017 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import botocore
import tempfile
import yaml

from .import S3Storage
import lsst.daf.persistence as dafPersist


__all__ = []

remoteCfgName = 'repositoryCfg.yaml'


def writeRepositoryCfg(bucket, butlerLocation, obj):
    """Write a RepositoryCfg to an AWS bucket using boto3.

    Parameters
    ----------
    bucket : dict
        A boto3 S3 Bucket
    butlerLocation : ButlerLocation
        Location info for writing into the database.
        Only getLocations is used.
    obj : object instance
        The object to write into the database.
    """
    # TODO support for not-in-place cfgs (may be referring to a different repo elsewhere via different root)
    # TODO support for concurrency (HOW?)
    with tempfile.NamedTemporaryFile('r+', prefix="FOO", encoding='utf-8') as f:
        yaml.dump(obj, f)
        f.flush()
        with open(f.name, 'rb') as j:
            bucket.put_object(Key=remoteCfgName, Body=j)


def readRepositoryCfg(bucket, butlerLocation):
    """Read an RepositoryCfg from an AWS bucket using boto3.

    bucket : dict
        A boto3 S3 Bucket
    butlerLocation : ButlerLocation
        Location info for reading from the database.
        Only getLocations is used.
    """
    # TOOD this will probably raise something useful if the cfg does not exist in the bucket. Write a test for
    # that and raise NoRepositoryAtCfg when it happens.
    with tempfile.NamedTemporaryFile('w', prefix="BAR", encoding='utf-8') as f:
        try:
            bucket.download_file('repositoryCfg.yaml', f.name)
        except botocore.exceptions.ClientError:
            return None
        with open(f.name, 'r') as j:
            cfg = yaml.load(j)
        return cfg


S3Storage.registerFormatters(dafPersist.RepositoryCfg, readRepositoryCfg, writeRepositoryCfg)
