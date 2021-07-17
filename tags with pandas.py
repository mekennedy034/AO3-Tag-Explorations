###########################################
##Tags processing
##in its own script
##which no longer takes six hours to run!
###########################################
import pandas as pd
import datetime

def main():
    print("Start script:", datetime.datetime.now())
    tags = load_tags()
    tags = combine_count(tags)
    write_tags(tags)

def load_tags():
    d_types = {"type": 'category',
               "name": str,
               "canonical": bool,
               "cached_count": 'Int64',
               }
    chunks = []
    print("Opening file:", datetime.datetime.now())
    with pd.read_csv("tags-20210226.csv",
                     dtype = d_types,
                     encoding = "utf-8",
                     converters = {"id": str, "merger_id":str},
                     engine = "c",
                     chunksize = 1000000) as reader:
        chunk_no = 0
        for chunk in reader:
            chunk_no +=1
            chunk.drop(chunk[(chunk['cached_count'] < 1) & (chunk['canonical'] == 'False')].index, inplace = True)            
            chunk.drop(chunk[(chunk['name'] == "Redacted") & (chunk['merger_id'] == "")].index, inplace = True)
            chunks.append(chunk)
            print("Chunk number", chunk_no, "processed;", len(chunk), "out of 1,000,000 tags retained.", datetime.datetime.now())
        tags = pd.concat(chunks, ignore_index = True)
    print(len(tags), "tags loaded", datetime.datetime.now())
    return tags

def combine_count(tags):
    print("Counting merged tags...", datetime.datetime.now())

    merger_tags = tags.loc[ tags["merger_id"] != "", ["id", "merger_id"]].astype(str)
    merger_tag_counts = merger_tags["merger_id"].value_counts(sort=False)
    merge_counts = merger_tag_counts.to_dict()
    tags["num_merged_tags"] = tags["id"].map(merge_counts)
     
    merged_tags = merger_tags.groupby("merger_id", sort=False)["id"].apply(list)
    merges = merged_tags.to_dict()
    tags["merged_tag_ids"] = tags["id"].map(merges)

    print("Done! Now consolidating tag counts...", datetime.datetime.now())
        
    counts_to_merge = tags.loc[ tags["merger_id"] != "", ["id", "merger_id", "cached_count"]]
    sub_groups = counts_to_merge.groupby("merger_id", sort=False).sum()
    equiv = sub_groups.to_dict()["cached_count"]
  
    tags["merged_counts"]=tags["id"].map(equiv)
    tags["total_counts"] = tags["cached_count"] + tags["merged_counts"].fillna(0)
    print("Done tag combining", datetime.datetime.now())
    return tags        

    
def write_tags(tags):

    tags.to_csv("clean_tags.csv", index = False)


    print("Saved!", datetime.datetime.now())


if __name__ == '__main__':
    main()
