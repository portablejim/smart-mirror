#!/usr/bin/env python3

from sqlite3 import *
import bz2
import gzip
import os
import tempfile
import urllib.request

mirror = "http://mirror.aarnet.edu.au/pub/ubuntu/archive/"
releases = ["precise-updates", "precise-security", "precise"]
archs = ["i386", "amd64"]

categories = ["main", "universe"]
popconLimit = { "main": 50000, "universe": 20000 }

settingsDir = os.path.join( os.getenv('HOME'), '.smart-mirror' )
tempDir = os.path.join( tempfile.gettempdir(), "smart-mirror" )

def init():
  dir = os.path

def setup():
    if not os.path.isdir( settingsDir ):
        os.mkdir( settingsDir )
    if not os.path.isdir( tempDir ):
        os.mkdir( tempDir )
        #print( "Creating " + tempDir )

    db = connect( os.path.join( settingsDir, 'lists.sqlite' ) )

    cursor = db.cursor();

    cursor.execute("DROP TABLE IF EXISTS packages")
    cursor.execute("DROP TABLE IF EXISTS package_info")
    cursor.execute("DROP TABLE IF EXISTS popcon")
    db.commit()

    cursor.execute("CREATE TABLE packages ( \
            pack_id INTEGER CONSTRAINT packPK PRIMARY KEY AUTOINCREMENT, \
            arch TEXT, \
            release TEXT, \
            package TEXT, \
            UNIQUE ( arch, release, package ) )")
    cursor.execute("CREATE TABLE package_info ( \
            pack_id INTEGER REFERENCES packages(pack_id), \
            size INTEGER, \
            filename TEXT NOT NULL ON CONFLICT ABORT )")
    cursor.execute("CREATE TABLE popcon ( \
            package TEXT PRIMARY KEY, \
            category TEXT, \
            num_installs INTEGER NOT NULL )")
    db.commit()

    return db

def getPackages(db):
    cursor = db.cursor();
    for release in releases:
        for category in categories:
            for arch in archs:
                packageListZipURL = \
                        mirror + \
                        'dists/' + \
                        release + '/' + \
                        category + '/' + \
                        'binary-' + arch + '/' + \
                        'Packages.bz2'
                packageListFilename = os.path.join( tempDir, 'packages-' + \
                        release + '-' + category + '-' + arch ) + '.bz2'

                packageListZip = urllib.request.urlretrieve(packageListZipURL,
                        packageListFilename)
                packageList = bz2.BZ2File(packageListFilename, buffering=1000)

                packName = ""
                packFilename = ""
                packArch = ""
                packSize = 0

                while True:
                    lineRaw = packageList.readline()
                    line = lineRaw.decode("utf-8" ).rstrip("\n")
                    if not lineRaw:
                        #print( line )
                        break;

                    lineParts = line.split(" ")
                    if lineParts[0] == "Package:":
                        packName = lineParts[1]
                    if lineParts[0] == "Filename:":
                        packFilename = lineParts[1]
                    if lineParts[0] == "Architecture:":
                        packArch = lineParts[1]
                    if lineParts[0] == "Size:":
                        packSize = int( lineParts[1] )
                    if len(line) == 0:
                        try:
                            vals = ( packArch, release, packName )
                            cursor.execute("INSERT INTO " + 
                                    "packages( pack_id, arch, release, package )" +
                                    " VALUES( NULL, ?, ?, ? )", vals)
                            vals = ( cursor.lastrowid, packSize, packFilename )
                            cursor.execute("INSERT INTO " +
                                    "package_info( pack_id, size, filename ) " +
                                    "VALUES ( ?, ?, ? )", vals)
                        except IntegrityError:
                            # We tried to insert a duplicate row.
                            # This is normal. This will happen for packages
                            #  that are common across arches and categories.
                            pass

                db.commit()

def getPopcon(db):
    cursor = db.cursor()
    for category in categories:
        popconZipUrl = "http://popcon.ubuntu.com/" + category + "/by_inst.gz"
        popconFilename = os.path.join( tempDir, 'popcon-' + category + ".gz" )

        popconZip = urllib.request.urlretrieve( popconZipUrl, popconFilename )

        popconFile = gzip.open( popconFilename )

        for lineRaw in popconFile:
            line = lineRaw.decode("utf-8").rstrip("\n")

            if line[0] == "#" or line[0] == "-":
                continue

            packageName = line.split()[1]
            packageInstalls = line.split()[2]

            # There is a "Total" row, so exclude it
            if packageName == "Total":
                continue

            try:
                vals = ( packageName, category, packageInstalls )
                cursor.execute("INSERT INTO popcon( package, category, num_installs ) " +
                            "VALUES ( ?, ?, ? )", vals)

                #print( packageName + " - " + packageInstalls )

            except IntegrityError:
                # We tried to insert a duplicate value
                # This should not happen, so raise an error
                #print( "Duplicate Row" )
                pass

        db.commit()

def getUrls(db):
    cursor = db.cursor()
    cursor.execute('SELECT package, num_installs, filename FROM (SELECT * FROM popcon WHERE category == "main" AND num_installs > 5000 UNION SELECT * FROM popcon WHERE category == "universe" AND num_installs > 10000) NATURAL JOIN packages NATURAL JOIN package_info ORDER BY num_installs DESC')
    for row in cursor:
        print ( mirror + row[2] )

#database = connect( os.path.join( settingsDir, 'lists.sqlite' ) )
database = setup()
getPackages(database)
getPopcon(database)
getUrls(database)



