#!/usr/bin/env python

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
import urllib.parse

import lsst.daf.persistence as dafPersist


# this class emits warnings. some say they are intended:
# https://github.com/boto/boto3/issues/454


class S3Storage(dafPersist.StorageInterface):
    """Defines the interface for a connection to an S3 Storage.

    NOTE this uses boto3 which has a bug causing unit tests to emit lots of resource warnings about unclosed
    sockets. Some try to claim that it's a warning not a bug. See conversation at
    https://github.com/boto/boto3/issues/454.

    Parameters
    ----------
    uri : string
        URI or path that is used as the storage location. Must begin with
        scheme 's3' or 'S3'. Scheme may be followed by 2 or 3 slashes, and the
        bucket name. e.g. `s3://my-bucket`.
    create : bool
        If True The StorageInterface subclass should create a new
        repository at the root location. If False then a new repository
        will not be created.

    Raises
    ------
    NoRepositroyAtRoot
        If create is False and a repository does not exist at the root
        specified by uri then NoRepositroyAtRoot is raised.
    """

    def __init__(self, uri, create):
        """initialzer"""
        parseRes = urllib.parse.urlparse(uri)
        if parseRes.scheme.upper() != "S3":
            raise RuntimeError("S3Storage does not support scheme:{}".format(parseRes.scheme))
        self.s3 = boto3.resource('s3')

        # if the URI is specified with 2 slashes the bucket name will be in the netLoc. If it has more than 2
        # it will be in the path, and may have leading slashes.
        self.bucketName = (parseRes.netloc or parseRes.path).lstrip('/')
        if self._bucketExists(uri) is False:
            if create is True:
                self.s3.create_bucket(Bucket=self.bucketName)
            else:
                raise dafPersist.NoRepositroyAtRoot(uri)
        self.bucket = self.s3.Bucket(self.bucketName)

    def _bucketExists(self, uri):
        """Query if the bucket exists

        Parameters
        ----------
        uri : string
            The name of the bucket.

        Returns
        -------
        bool
            True if the bucket exists else false.
        """
        try:
            self.s3.meta.client.head_bucket(Bucket=uri)
            return True
        except botocore.exceptions.ParamValidationError:
            # The bucket does not exist or you have no access.
            return False

    def write(self, butlerLocation, obj):
        """Writes an object to a location and persistence format specified by ButlerLocation

        Parameters
        ----------
        butlerLocation : ButlerLocation
            The location & formatting for the object to be written.
        obj : object instance
            The object to be written.
        """
        writeFormatter = self.getWriteFormatter(type(obj))
        if writeFormatter is None:
            raise RuntimeError(
                "No write formatter registered with {} for {}".format(__class__.__name__, type(obj)))
        writeFormatter(self.bucket, butlerLocation, obj)

    def read(self, butlerLocation):
        """Read from a butlerLocation.

        Parameters
        ----------
        butlerLocation : ButlerLocation
            The location & formatting for the object(s) to be read.

        Returns
        -------
        A list of objects as described by the butler location. One item for
        each location in butlerLocation.getLocations()
        """
        readFormatter = self.getReadFormatter(butlerLocation.getPythonType())
        if readFormatter is None:
            raise RuntimeError(
                "No read formatter registered with {} for {}".format(__class__.__name__,
                                                                     butlerLocation.getPythonType()))
        return readFormatter(self.bucket, butlerLocation)

    def getLocalFile(self, path):
        """Get a handle to a local copy of the file, downloading it to a
        temporary if needed.

        Parameters
        ----------
        path : string
            A path to the the file in storage, relative to root.

        Returns
        -------
        A handle to a local copy of the file. If storage is remote it will be
        a temporary file. If storage is local it may be the original file or
        a temporary file. The file name can be gotten via the 'name' property
        of the returned object.
        """
        raise NotImplementedError

    def exists(self, location):
        """Check if location exists.

        Performs a "listing request" (by some accounts 12.5x more expensive than
        a get), but since the objects can be large files this seems like a
        better approach than a try-get-catch approach.

        Parameters
        ----------
        location : ButlerLocation or string
            A a string or a ButlerLocation that describes the location of an
            object in this storage.

        Returns
        -------
        bool
            True if exists, else False.
        """
        objectName = location.getLocations()[0]
        bucketObjects = list(self.bucket.objects.filter(Prefix=objectName))
        for bucketObject in bucketObjects:
            if objectName == bucketObject.key:
                return True
        return False

    def instanceSearch(self, path):
        """Search for the given path in this storage instance.

        If the path contains an HDU indicator (a number in brackets before the
        dot, e.g. 'foo.fits[1]', this will be stripped when searching and so
        will match filenames without the HDU indicator, e.g. 'foo.fits'. The
        path returned WILL contain the indicator though, e.g. ['foo.fits[1]'].

        Parameters
        ----------
        path : string
            A filename (and optionally prefix path) to search for within root.

        Returns
        -------
        string or None
            The location that was found, or None if no location was found.
        """
        strippedPath = path
        if strippedPath.endswith(']'):
            strippedPath = strippedPath[:strippedPath.rfind('[')]
        location = dafPersist.ButlerLocation(pythonType=None, cppType=None, storageName=None,
                                             locationList=[strippedPath], dataId={}, mapper=None,
                                             storage=None)
        return bool(self.exists(location))

    @classmethod
    def search(cls, root, path):
        """Look for the given path in the current root.

        Also supports searching for the path in Butler v1 repositories by
        following the Butler v1 _parent symlink

        If the path contains an HDU indicator (a number in brackets, e.g.
        'foo.fits[1]', this will be stripped when searching and so
        will match filenames without the HDU indicator, e.g. 'foo.fits'. The
        path returned WILL contain the indicator though, e.g. ['foo.fits[1]'].

        Parameters
        ----------
        root : string
            The path to the root directory.
        path : string
            The path to the file within the root directory.

        Returns
        -------
        string or None
            The location that was found, or None if no location was found.
        """
        raise NotImplementedError

    def copyFile(self, fromLocation, toLocation):
        """Copy a file from one location to another on the local filesystem.

        Parameters
        ----------
        fromLocation : string
            Path and name of existing file.
         toLocation : string
            Path and name of new file.

        Returns
        -------
        None
        """
        copy_source = {
            'Bucket': self.bucketName,
            'Key': fromLocation
        }
        self.bucket.copy(copy_source, toLocation)

    def locationWithRoot(self, location):
        """Get the full path to the location.

        Parameters
        ----------
        location : string
            Path to a location within the repository relative to repository
            root.

        Returns
        -------
        string
            Absolute path to to the locaiton within the repository.
        """
        raise NotImplementedError

    @classmethod
    def getRepositoryCfg(cls, uri):
        """Get a persisted RepositoryCfg

        Parameters
        ----------
        uri : URI or path to a RepositoryCfg
            Description

        Returns
        -------
        A RepositoryCfg instance or None
        """
        storage = dafPersist.Storage.makeFromURI(uri)
        location = dafPersist.ButlerLocation(pythonType=dafPersist.RepositoryCfg,
                                             cppType=None,
                                             storageName=None,
                                             locationList=None,
                                             dataId={},
                                             mapper=None,
                                             storage=storage,
                                             usedDataId=None,
                                             datasetType=None)
        return storage.read(location)

    @classmethod
    def putRepositoryCfg(cls, cfg, loc=None):
        """Serialize a RepositoryCfg to a location.

        When loc == cfg.root, the RepositoryCfg is to be written at the root
        location of the repository. In that case, root is not written, it is
        implicit in the location of the cfg. This allows the cfg to move from
        machine to machine without modification.

        Parameters
        ----------
        cfg : RepositoryCfg instance
            The RepositoryCfg to be serailized.
        loc : string, optional
            The URI location (can be relative path) to write the RepositoryCfg.
            If loc is None, the location will be read from the root parameter
            of loc.

        Returns
        -------
        None
        """
        storage = dafPersist.Storage.makeFromURI(cfg.root if loc is None else loc, create=True)
        location = dafPersist.ButlerLocation(pythonType=dafPersist.RepositoryCfg,
                                             cppType=None,
                                             storageName=None,
                                             locationList=None,
                                             dataId={},
                                             mapper=None,
                                             storage=storage,
                                             usedDataId=None,
                                             datasetType=None)
        storage.write(location, cfg)

    @classmethod
    def getMapperClass(cls, root):
        """Get the mapper class associated with a repository root.

        Parameters
        ----------
        root : string
            The location of a persisted RepositoryCfg is (new style repos).

        Returns
        -------
        A class object or a class instance, depending on the state of the
        mapper when the repository was created.
        """
        cfg = cls.getRepositoryCfg(root)
        if cfg is None:
            raise dafPersist.NoRepositroyAtRoot(root)
        return cfg.mapper

dafPersist.Storage.registerStorageClass(scheme='s3', cls=S3Storage)
dafPersist.Storage.registerStorageClass(scheme='S3', cls=S3Storage)

