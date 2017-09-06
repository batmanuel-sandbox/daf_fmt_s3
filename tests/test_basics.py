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


import boto3
import botocore
from moto import mock_s3
import os
import pickle
import tempfile
import unittest
import yaml

import lsst.utils.tests
from lsst.daf.fmt.s3 import S3Storage
import lsst.daf.fmt.s3.fmtRepositoryCfg
import lsst.daf.persistence as dafPersist
from lsst.obs.base import CameraMapper
from lsst.utils import getPackageDir


ROOT = os.path.abspath(os.path.dirname(__file__))
OBS_BASE_DIR = getPackageDir('obs_base')

# Set this environment variable to any True value to access the real S3 server instead of using the moto mock.
LSST_USE_REAL_S3 = os.environ.get('LSST_USE_REAL_S3', False)
# Optional, set this environment variable to any string that will be prepended (followed by a dash) to bucket
# names in the test. It may be that buckets you create in LSST servers are required to start with your user
# name. If this var is not set you may get the following error: `botocore.exceptions.ClientError: An error
# occurred (AccessDenied) when calling the CreateBucket operation: Access Denied`
LSST_S3_TEST_BUCKET_PREFIX = os.environ.get('LSST_S3_TEST_BUCKET_PREFIX', None)


def setup_module(module):
    lsst.utils.tests.init()


class MyTestObject:

    def __init__(self, val):
        self.val = val

    def __eq__(self, other):
        return self.val == other.val


def writeMyTestObject(bucket, butlerLocation, obj):
    with tempfile.NamedTemporaryFile('wb') as f:
        pickle.dump(obj, f)
        f.flush()
        with open(f.name, 'rb') as j:
            bucket.put_object(Key=butlerLocation.getLocations()[0], Body=j)


def readMyTestObject(bucket, butlerLocation):
    with tempfile.NamedTemporaryFile('wb') as f:
        try:
            bucket.download_file(butlerLocation.getLocations()[0], f.name)
        except botocore.exceptions.ClientError:
            return None
        with open(f.name, 'rb') as f:
            obj = pickle.load(f)
        return [obj]


S3Storage.registerFormatters(MyTestObject, readFormatter=readMyTestObject, writeFormatter=writeMyTestObject)


class MyMapper(dafPersist.Mapper):

    def __init__(self, root, *args, **kwargs):
        self.storage = dafPersist.Storage.makeFromURI(root)
        dafPersist.Mapper.__init__(root, *args, **kwargs)

    def map_obj(self, dataId, write):
        loc = dafPersist.ButlerLocation(pythonType=MyTestObject,
                                        cppType=None,
                                        storageName=None,
                                        locationList=['testname'],
                                        dataId={},
                                        mapper=self,
                                        storage=self.storage)
        return loc


class MyCameraMapper(CameraMapper):
    packageName = "daf_fmt_s3"

    def __init__(self, *args, **kwargs):
        # policyFile = dafPersist.Policy.defaultPolicyFile(self.packageName, "MyMapper.yaml", "policy")
        policy = dafPersist.Policy(yaml.load("""
            camera: "camera"
            defaultLevel: "sensor"
            datasets: {}
            exposures: {}
            calibrations: {}
            images: {}"""))
        super().__init__(policy, repositoryDir=os.path.join(OBS_BASE_DIR, 'tests'), **kwargs)

    def map_obj(self, dataId, write):
        loc = dafPersist.ButlerLocation(pythonType=MyTestObject,
                                        cppType=None,
                                        storageName=None,
                                        locationList=['testname'],
                                        dataId={},
                                        mapper=self,
                                        storage=self.rootStorage)
        return loc


class BasicTestCase(unittest.TestCase):

    def setUp(self):
        if not LSST_USE_REAL_S3:
            self.mock = mock_s3()
            self.mock.start()
        self.cleanupBucketNames = []

    def _prefixBucketName(self, bucketName):
        """Prepend the LSST_S3_TEST_BUCKET_PREFIX to bucketName if LSST_S3_TEST_BUCKET_PREFIX is defined.

        Parameters
        ----------
        bucketName : string
            The bucket name to apply the prefix to.

        Returns
        -------
        string
            If the
        """
        if LSST_S3_TEST_BUCKET_PREFIX:
            return LSST_S3_TEST_BUCKET_PREFIX + '-' + bucketName
        else:
            return bucketName

    def _getS3URI(self, bucketName):
        """Take bucketName, apply _prefixBucketName, register bucket to be cleaned up and return complete
        storage URI starting with 'S3:///'"""
        bucketName = self._prefixBucketName('test_copy')
        self.cleanupBucketNames.append(bucketName)
        return os.path.join('s3:///', bucketName)

    def tearDown(self):
        s3client = boto3.client('s3')
        for bucketName in self.cleanupBucketNames:
            try:
                bucket = boto3.resource('s3').Bucket(bucketName)
                for key in bucket.objects.all():
                    key.delete()
                bucket.delete()
            except s3client.exceptions.NoSuchBucket:
                pass
        if not LSST_USE_REAL_S3:
            self.mock.stop()

    def test_no_repo(self):
        """Test that NoRepositoryAt Root is raised when S3Storage init create=False and connecting to a
        repository that does not exist"""
        repoLocation = self._getS3URI('test_doesNotExist')
        with self.assertRaises(dafPersist.storage.NoRepositroyAtRoot):
            S3Storage(uri=repoLocation, create=False)

    def test_two_and_three_slashes(self):
        """Test URIs with 2 and 3 slashes (they parse differently in urllib.parse.urlparse)"""
        bucketName = self._prefixBucketName('test_slashcount')
        self.cleanupBucketNames.append(bucketName)
        for location in (os.path.join('s3://', bucketName),
                         os.path.join('s3:///', bucketName)):
            storage = S3Storage(uri=location, create=True)
            del storage

    def test_S3Storage(self):
        """A simple test to create a bucket using an S3Storage object, and write & read a repositoryCfg object
        """
        repoLocation = self._getS3URI('test_S3Storage')
        storage = S3Storage(uri=repoLocation, create=True)
        cfg = dafPersist.RepositoryCfg.makeFromArgs(dafPersist.RepositoryArgs(root=repoLocation))
        storage.putRepositoryCfg(cfg)

        reloadedCfg = storage.getRepositoryCfg(repoLocation)
        self.assertEqual(cfg, reloadedCfg)

    def test_Butler(self):
        """A test that uses a Butler to create an S3 storage, put an object in it, reload the repo in a new
        butler, and get the object.
        """
        repoLocation = self._getS3URI('test_Butler')
        butler = dafPersist.Butler(outputs={'root': repoLocation, 'mapper': MyMapper})
        testObj = MyTestObject('foo')
        butler.put(testObj, 'obj')

        butler = dafPersist.Butler(inputs=repoLocation)
        reloadedObj = butler.get('obj')
        self.assertEqual(testObj, reloadedObj)

    def test_CameraMapper(self):
        repoLocation = self._getS3URI('test_CameraMapper')
        butler = dafPersist.Butler(outputs={'root': repoLocation, 'mapper': MyCameraMapper})
        testObj = MyTestObject('foo')
        butler.put(testObj, 'obj')

        butler = dafPersist.Butler(inputs=repoLocation)
        reloadedObj = butler.get('obj')
        self.assertEqual(testObj, reloadedObj)

    def test_copy(self):
        repoLocation = self._getS3URI('test_copy')
        storage = S3Storage(uri=repoLocation, create=True)
        loc = dafPersist.ButlerLocation(pythonType=MyTestObject,
                                        cppType=None,
                                        storageName=None,
                                        locationList=['testname'],
                                        dataId={},
                                        mapper=self,
                                        storage=storage)
        testObj = MyTestObject('foo')
        storage.write(loc, testObj)
        storage.copyFile('testname', 'testname_copy')
        storage.read(loc)
        reloadedObj = storage.read(loc)
        self.assertEqual(testObj, reloadedObj[0])
        loc.locationList = ['testname_copy']
        copiedObj = storage.read(loc)
        self.assertEqual(testObj, copiedObj[0])


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
