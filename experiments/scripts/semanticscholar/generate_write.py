import json
import csv

writeLog = {}

count = 0
with open('write_raw.csv', newline='') as csvf:
    csvreader = csv.reader(csvf, delimiter=',')
    for row in csvreader:
        print(count)
        count += 1
        key = (row[0], row[1])
        if key not in writeLog:
            writeLog[key] = 1

with open('write.csv', 'w') as f:
    writer = csv.writer(f, delimiter=',')
    for k, v in writeLog.items():
        writer.writerow([k[0], k[1]])
