import os
import json
import csv
import sys

# json files
json_dir = sys.argv[1]
json_files = os.listdir(json_dir)

# global hashmap
paperId2Index = {}
authorIds2Id = {} # an author may have multiple Ids
authorId2Index = {} 

# global index
paperIndex = 0
authorIndex = 0

for json_file in json_files:
    print('--------Processing {}-----------'.format(json_file))
    pcsv = open('paper.csv', 'a')
    wcsv = open('write_raw.csv' ,'a')
    ccsv = open('cite_raw.csv', 'a')
    paperWriter = csv.writer(pcsv, delimiter=',')
    writeWriter = csv.writer(wcsv, delimiter=',')
    citeWriter = csv.writer(ccsv, delimiter=',')
    with open('{}/{}'.format(json_dir, json_file)) as f:
        for line in f:
            data = json.loads(line)
            # clean
            if 'id' in data:
                paperId = data['id']
            else:
                print('id not exist')
                continue
            if 'authors' in data:
                authors = data['authors']
            else:
                print('author not exist')
                continue
            if 'year' in data:
                year = data['year']
            else:
                #print('year not exist')
                continue
            if 'inCitations' in data:
                inciteIds = data['inCitations']
            else:
                print('inCitations not exist')
                continue
            if 'outCitations' in data:
                outciteIds = data['outCitations']
            else:
                print('outCitations not exist')
                continue
            if paperId is None:
                print('Paper ID is None, record remove')
                continue
            if (authors is None) or (len(authors) == 0):
                #print('Authors is None, record remove')
                continue
            authors_cleaned = []
            for author in authors:
                remove = False
                if (author['ids'] is None) or (len(author['ids'])) == 0:
                    #print('Author IDs is empty, record remove')
                    remove = True
                if remove : continue
                authors_cleaned.append(author)
                unifiedAuthorId = author['ids'][0]
                for authorId in author['ids']:
                    if authorId not in authorIds2Id:
                        authorIds2Id[authorId] = unifiedAuthorId
            if year is None:
                #print('Year is None, record remove')
                continue
            # Indexing
            if paperId not in paperId2Index:
                paperId2Index[paperId] = paperIndex
                paperIndex += 1
            for author in authors_cleaned:
                if authorIds2Id[author['ids'][0]] not in authorId2Index:
                    authorId2Index[authorIds2Id[author['ids'][0]]] = authorIndex
                    authorIndex += 1
            # write paper.csv
            paperWriter.writerow([paperId2Index[paperId], year, len(authors)])
            
            # write write_raw.csv
            for author in authors_cleaned:
                writeWriter.writerow([authorId2Index[authorIds2Id[author['ids'][0]]], paperId2Index[paperId]])
            
            # write cite_raw.csv
            for inciteId in inciteIds:
                citeWriter.writerow([inciteId, paperId])
            for outciteId in outciteIds:
                citeWriter.writerow([paperId, outciteId])
    pcsv.flush()
    wcsv.flush()
    ccsv.flush()
    pcsv.close()
    wcsv.close()
    ccsv.close()

# log index
with open('paperIndex', 'w') as pIndexf:
    pIndexf.write(json.dumps(paperId2Index))

with open('authorIds2Id', 'w') as aIdsf:
    aIdsf.write(json.dumps(authorIds2Id))

with open('authorIndex', 'w') as aIdf:
    aIdf.write(json.dumps(authorId2Index))
