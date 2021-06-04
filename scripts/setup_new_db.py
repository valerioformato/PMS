from getpass import getpass

import pymongo
import urllib
import argparse
import subprocess
import sys

def main():
    parser = argparse.ArgumentParser(description='Create a new user in the admin DB.')
    parser.add_argument('-H' , '--dbhost'     , type=str, nargs=1  , help='MongoDB host. If not specified hostname will be used')
    parser.add_argument('-u' , '--dbuser'     , type=str, nargs=1  , help='MongoDB user. Mandatory')
    parser.add_argument('-p' , '--dbpass'     , type=str, nargs=1  , help='MongoDB password. Mandatory')
    parser.add_argument('-d' , '--db'         , type=str, nargs=1  , help='MongoDB db name. Mandatory')
    args = parser.parse_args()

    if not args.dbuser:
        print("Error: you need to provide a user to log on the DB (-U option)")
        sys.exit(1)
        pass

    if not args.db:
        print("Error: you need to provide the name of the DB (-D option)")
        sys.exit(1)
        pass

    dbuser = args.dbuser[0]
    if args.dbpass:
        dbpass = args.dbpass[0]
    else:
        dbpass = getpass("Type in your old password: ")

    dbname = args.db[0]
    hostname = str(subprocess.Popen(['hostname',''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.read().strip())

    global dbhost
    if args.dbhost:
        dbhost = str( args.dbhost[0] )
    else:
        dbhost = hostname



    # connect to db
    client = pymongo.MongoClient(
        'mongodb://{}:{}@{}/{}'.format(dbuser, urllib.parse.quote(dbpass), dbhost, dbname))
    db = client[dbname]

    if not 'jobs' in db.collection_names():
        db['jobs'].create_index(
            [('hash', pymongo.ASCENDING)], unique=True)
        db['jobs'].create_index(
            [('task', pymongo.ASCENDING)], unique=False)
        pass

    if not 'datasets' in db.collection_names():
        db['datasets'].create_index(
            [('dataset', pymongo.ASCENDING), ('subset', pymongo.ASCENDING)], unique=True)
        pass

    if not 'ntuples' in db.collection_names():
        db['ntuples'].create_index(
            [('dataset', pymongo.ASCENDING), ('subset', pymongo.ASCENDING)], unique=True)
        pass

if __name__ == "__main__":
    main()
