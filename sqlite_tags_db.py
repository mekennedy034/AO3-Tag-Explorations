import sqlite3
import pandas as pd
import datetime

##estbablish the sqlite3 database connection
con = sqlite3.connect("ao3_tags.db") 
cur = con.cursor()

##data types for reading in the clean data
##these have to be enforced or else pandas starts
##making up animals
dtypes_w={"language": 'category',
        "restricted":"boolean",
        "complete":"boolean",
        "word_count":"Int64",
        "Rating":str,
        "Choose Not To Use Archive Warnings":bool,
        "No Archive Warnings Apply":bool,
        "Graphic Depictions Of Violence":bool,
        "Major Character Death":bool,
        "Rape/Non-Con":bool,
        "Underage":bool,
        "Gen":bool,
        "F/M":bool,
        "M/M":bool,
        "Other":bool,
        "F/F":bool,
        "Multi":bool}

dtypes_t = {"type": str,
            "name": str,
            "canonical": bool,
            "cached_count": "Int64",
            "num_merged_tags": "Int64",
            "merged_tag_ids": str,
            "merged_counts": "Int64",
            "total_counts": "Int64"
            }


print("Start script:", datetime.datetime.now())
print("Getting works....:", datetime.datetime.now())
##load the csv file
##must specifty utf-8 in order to keep the non-Latin characters readable
##must specify converters and dtypes because those are not frickin' integers!
##load in chunks to avoid choking the memory
with pd.read_csv("clean_works.csv",3
                 encoding="utf-8",
                 index_col=False, header=0,
                 converters = {"tags":str, "work_id":str}, 
                 parse_dates = ["creation date"],
                 dtype=dtypes_w,
                 chunksize=100000) as reader:

    chunk_no = 0
    for chunk in reader:
        chunk_no += 1
        chunk.rename_axis("work_id", inplace=True)

        ##separate the tag strings, split them into lists
        ##then use .explode() to turn each work_id:list pair
        ##into a work_id:list_item pair for every
        ##list item
        hunk = chunk["tags"].apply(lambda x: x.split("+")).reset_index()
        hunk = hunk.explode("tags") 

        hunk.to_sql("tag_lists", con, if_exists="append", index=False)

        ##now we can drop the tags column and send everything else to another
        ##table in the db
        chunk.drop(columns=["tags"], inplace=True)
        chunk.to_sql("works", con, if_exists="append", index=False)

        print("Chunk", chunk_no, "processed!", len(chunk), "works read in", len(hunk), "tags collected", datetime.datetime.now()) 

    print("Works complete!", datetime.datetime.now())

print("Getting tags...", datetime.datetime.now())
##same as loading the previous csv
with pd.read_csv("clean_tags.csv",
                 encoding="utf-8",
                 index_col=False, header=0,
                 converters = {"id": str, "merger_id": str},
                 dtype=dtypes_t,
                 chunksize=100000) as reader:
    chunk_no = 0
    for glob in reader:
        chunk_no += 1
        ##separate the parent tags from the ones that merge into them
        blob = glob[ glob["merger_id"] == ""]
        ##create two tables -- all tags, and merged parent tags only
        glob.to_sql("all_tags", con, if_exists="append", index=False)
        blob.to_sql("merged_tags", con, if_exists="append", index=False)
        print("Chunk", chunk_no, "processed!", len(glob),"tags consolidated to",len(blob))
    print("Tags complete!", datetime.datetime.now())


print("And now, we join!", datetime.datetime.now())

##create one big honkin' table
##with tag and date info in one place
cur.execute("""CREATE TABLE tags_dates_types AS
            SELECT works.'creation date' AS 'date',
            works.rating,
            tag_lists.work_id,
            tag_lists.tags,
            all_tags.type,
            all_tags.name
            FROM tag_lists, works, all_tags
            WHERE works.work_id = tag_lists.work_id
            AND all_tags.id = tag_lists.tags;""")

##close the db connection
con.commit()
con.close()
