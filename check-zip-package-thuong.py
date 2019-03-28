import ConfigParser
import ast
import json
import shutil
import sys
import time
import traceback
import zipfile
# from common.exportExcel import Excel
from os import listdir
from os.path import isfile, join, isdir

from sqlalchemy.sql import text

from common.dbConnection import DbConnection

print 'Start...'
zipPath = 'zip'
mypath = 'json'
txt = ConfigParser.ConfigParser()
txt.read('check-zip-package.ini')
servers = ast.literal_eval(txt.get('input', 'servers'))


def checkExistTitle(titles, title):
    try:
        # dbc = DbConnection(server)
        # dbn = dbc.dbName
        # params = {'title': title}
        # valuePercent = dbc.runQueryWithSession(session,txt.get('sql','check_exist_json').replace('{dbn}',str(dbn)),params).fetchone()
        # valuePercent = dbc.runQueryCore(connection,text(txt.get('sql','check_exist_json').replace('{dbn}',str(dbn))),params).fetchone()

        if title in titles.keys():
            return True
        return False

    except Exception, e:
        print e
        #    raise e
        traceback.print_exc()
        return False


def checkUploaded(titles):
    with open(mypath + '/__super__.json') as data_file:
        superData = json.load(data_file)
    countFileSuper = 0
    fileContents = {}

    files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    files.remove('__super__.json')

    for fileItem in files:
        if fileItem != '' and fileItem[0] != '.':
            with open(mypath + '/' + fileItem) as data_file:
                fileContents[fileItem] = json.load(data_file)
        else:
            files.remove(fileItem)

    boolean_result = True
    count_none_existed_title = 0
    count_existed_title = 0
    total_files = len(fileContents)
    title_json = []

    for key, file in fileContents.iteritems():
        if not checkExistTitle(titles, key[:-5]):
            # print '\t' + key[:-5] + ' not imported'
            boolean_result = False
            count_none_existed_title += 1
        else:
            # print '\t' + key[:-5] + ' imported'
            title_json.append(key[:-5])
            count_existed_title += 1
    result_tuple = (boolean_result, count_none_existed_title, count_existed_title, total_files, title_json)
    return result_tuple


def checkProcessAndSignature(process_names, signatures):
    with open(mypath + '/__super__.json') as data_file:
        superData = json.load(data_file)
    countFileSuper = 0
    fileContents = {}
    files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    files.remove('__super__.json')

    for fileItem in files:
        if fileItem != '' and fileItem[0] != '.':
            with open(mypath + '/' + fileItem) as data_file:
                fileContents[fileItem] = json.load(data_file)
        else:
            files.remove(fileItem)

    result = True
    count_existed_process = 0
    count_existed_signature = 0
    process_json = []
    signature_json = []
    for key, filejson in fileContents.iteritems():
        if ("process_name" in filejson.keys()) and len(filejson["process_name"]) > 0 and filejson["process_name"].lower() in process_names:
            # print '\t' + key[:-5] + ' process imported'
            process_json.append(key[:-5])
            result = False
            count_existed_process += 1
        elif len(filejson["signature"]) > 0 and filejson["signature"] in signatures:
            # print '\t' + key[:-5] + ' signature imported'
            signature_json.append(key[:-5])
            result = False
            count_existed_signature += 1
        # else:
        #     print '\t' + key[:-5] + ' process or signature NOT imported'

    return result, count_existed_process, count_existed_signature, process_json, signature_json


def getAllZipTitles(zipFiles):
    titles = []
    for zipFile in zipFiles:
        # delete json
        if isdir(mypath):
            shutil.rmtree(mypath)
        zipItem = zipfile.ZipFile(join(zipPath, zipFile))
        zipItem.extractall(mypath)
        superData = None
        with open(mypath + '/__super__.json') as data_file:
            superData = json.load(data_file)
        if superData is not None:
            for item in superData:
                titles.append(item["title"])
    return titles


def prepareSQLQueryFromTitles(titles):
    sql = 'select gc_error from geolocations_text_fields join geolocations g force index( time) using (geolocation_id) where (gc_error like \"%'
    for title in titles[:-1]:
        sql += title + '%\" or gc_error like   \"%'
    sql += titles[-1] + '%\") and g.time > \"2016-10-25 09:30:00\" '
    print sql


def checkTitleInDB(checkTitle):
    for server in servers:
        print server
        t0 = time.time()
        dbc = DbConnection(server, False)
        dbn = dbc.dbName
        # params = {'title': title}
        connection = dbc.Engine.connect()
        result = dbc.runQuery(text(txt.get('sql', 'check_exist_json').replace('{dbn}', str(dbn)))).fetchall()
        titles = {}
        process_names = {}
        signatures = {}
        for row in result:
            titles[row["title"]] = 1
            process_names[row["process_name"]] = row["title"]
            signatures[row["value"]] = row["title"]
        # print titles
        # session = dbc.Session()
        for zipFile in zipFiles:
            # delete json
            if isdir(mypath):
                shutil.rmtree(mypath)
            zipItem = zipfile.ZipFile(join(zipPath, zipFile))
            zipItem.extractall(mypath)
            print '\nChecking ' + zipFile
            if checkTitle:
                array = checkUploaded(titles)
                isFine = array[0]
                print 'Total files: ', array[3]
                # print 'Not imported title of json files: ', array[1]
                # print 'Imported title: ', array[2]
                # if not isFine and array[1] > 0:
                # print zipFile + (' has NOT IMPORTED files')
                if array[2] > 0:
                    print 'Imported title: ', array[2]
                    title_json = array[-1]
                    for titlejson in title_json:
                        print '\t' + titlejson + ' title IMPORTED'
                else:
                    array[2] == 0
                    print '\n' + zipFile + ' title NOT IMPORTED'

                isFine = checkProcessAndSignature(process_names, signatures)
                if isFine[1] > 0:
                    print 'Imported process: ', isFine[1]
                    for json_value in isFine[3]:
                        print '\t' + json_value + ' process IMPORTED'
                else:
                    print '\n' + zipFile + ' process NOT IMPORTED'
                if isFine[2] > 0:
                    print 'Imported signature: ', isFine[2]
                    for json_value in isFine[4]:
                        print '\t' + json_value + ' signature IMPORTED'
                else:
                    print '\n' + zipFile + ' signature NOT IMPORTED'

        # session.close()
        connection.close()
        t1 = time.time()
        print "Total time = {0}\n\n".format(t1 - t0)
    print 'Done...'


zipFiles = [f for f in listdir(zipPath) if (isfile(join(zipPath, f)) and f[-4:] == '.zip')]

if __name__ == '__main__':
    checkTitle = True
    if len(sys.argv) > 1:
        if sys.argv[1] == 'sql':
            titles = getAllZipTitles(zipFiles)
            prepareSQLQueryFromTitles(titles)
            exit(1)
        elif sys.argv[1] == 'exist':
            checkTitle = False
    checkTitleInDB(checkTitle)
