#!/usr/bin/env python3

"""Get Wikidata dump records as a JSON stream (one JSON object per line)"""
# Modified script taken from this link: "https://www.reddit.com/r/LanguageTechnology/comments/7wc2oi/does_anyone_know_a_good_python_library_code/dtzsh2j/"

# Execute:
# % python parse_json_bz2.py path/to/latest-all.json.bz2


import bz2
import json
import pandas as pd
import pydash
import os
from pathlib import Path
from sys import platform


#set home location
home = str(Path.home())

# platform specific file folder (mac for michael, win for satyam)
if platform == "darwin":
    # OS X
    folder="documents/projects-active/wikipedia_bio_data_feb2023"
elif platform == "win32":
    # Windows...
    folder="foobar"

ROOT = os.path.join(home,folder)


check_list = [23, 296, 297, 301, 352, 392, 504, 535, 557, 558, 762, 765, 767, 838, 930]
filename_id = 0;
i = 0
# an empty dataframe which will save items information
# you need to modify the columns in this data frame to save your modified data
columnames=('id', 'type', 'english_label', 'enwiki_title', 'occupation', 'gender', 'citizenship') #add DOB, active, etc here
df_record_all = pd.DataFrame(columns=columnames) 


def wikidata(filename):
    with bz2.open(filename, mode='rt') as f:
        f.read(2) # skip first two bytes: "{\n"
        for line in f:
            try:
                yield json.loads(line.rstrip(',\n'))
            except json.decoder.JSONDecodeError:
                continue

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=__doc__
    )
    parser.add_argument(
        'dumpfile',
        help=(
            'a Wikidata dumpfile from: '
            'https://dumps.wikimedia.org/wikidatawiki/entities/'
            'latest-all.json.bz2'
        )
    )
    args = parser.parse_args()
    for record in wikidata(args.dumpfile):
        try:
            check_id_str = pydash.get(record,'id')
            check_id = int(check_id_str[1:])
            # print(check_id)
            if pydash.has(record, 'sitelinks.enwiki') and pydash.has(record, 'claims.P31[0].mainsnak.datavalue.value.id') and (pydash.get(record, 'claims.P31[0].mainsnak.datavalue.value.id') == 'Q5'):
                print('i = '+str(i)+' item '+record['id']+'  started!'+'\n')
                if check_id in check_list:
                    print(check_id + "found in file with the following QID" + filename_id)
                # print(record)
                english_label = pydash.get(record, 'labels.en.value')
                enwiki_link_title = pydash.get(record, 'sitelinks.enwiki.title'  )
                item_id = pydash.get(record, 'id')
                item_type = pydash.get(record, 'type')
                p106occupation = pydash.get(record, 'claims.P106[0].mainsnak.datavalue.value.id')
                p21gender = pydash.get(record, 'claims.P21[0].mainsnak.datavalue.value.id')
                p27citizenship = pydash.get(record, 'claims.P27[0].mainsnak.datavalue.value.id')
                #DOB, active, etc =
                #Add DOB, active, etc to DF below
                df_record = pd.DataFrame({'id': item_id, 'type': item_type, 'english_label': english_label, 'enwiki_title': enwiki_link_title, 'occupation':p106occupation, 'gender':p21gender, 'citizenship':p27citizenship}, index=[i])
                # df_record_all = df_record_all.append(df_record, ignore_index=True)
                # dfallmaps = pd.concat([dfallmaps, dfthismap], ignore_index=True, sort=False)
                df_record_all = pd.concat([df_record_all, df_record], ignore_index=True, sort=False)

                # df_record_all = df_record_all.concat(df_record, ignore_index=True)
                i += 1
                # print(i)
                if (i % 10000 == 0):
                    savename = 'wikidata_output_'+str(i)+'_lastrecord_'+record['id']+'.csv'
                    savepath = os.path.join(ROOT, savename)
                    pd.DataFrame.to_csv(df_record_all, path_or_buf=savepath)
                    filename_id = record['id']
                    print('i = '+str(i)+' item '+record['id']+'  Done!')
                    print('CSV exported')
                    #there is a better way of zeroing the df
                    df_record_all = pd.DataFrame(columns=columnames)
                    # df_record_all.iloc[0:0]
                    # print(df_record_all)

                else:
                    # print('failed to save csv')
                    continue
        except:
            print('failed to add to df')
            continue
    pd.DataFrame.to_csv(df_record_all, path_or_buf='\\wikidata\\extracted\\final_csv_till_'+record['id']+'_item.csv')
    print('i = '+str(i)+' item '+record['id']+'  Done!')
    print('All items finished, final CSV exported!')
