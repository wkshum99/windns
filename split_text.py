import re
import csv

a = []
with open('1056556451_CI-table.txt', 'rb') as inf:
    for line in inf:
        if line.strip() != '':
            a.append(re.split('\s{3,}', line.strip()))

with open ('result.txt', 'wb') as outf:
    csvwriter = csv.writer(outf, delimiter='\t')
    for line in a:
        csvwriter.writerow(line)

