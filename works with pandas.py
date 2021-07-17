###########################################
##Works processing
##in its own script
##which finally doesn't take six hours to run!
###########################################
import pandas as pd
import datetime

WARNINGS = {"14" : "Choose Not To Use Archive Warnings",
                "16" : "No Archive Warnings Apply",
                "17" : "Graphic Depictions Of Violence",
                "18" : "Major Character Death",
                "19" : "Rape/Non-Con",
                "20" : "Underage"
                }

CATEGORIES = {"21":"Gen", "22":"F/M", "23":"M/M", "24":"Other", "116":"F/F",
                  "2246": "Multi", "437667":"M/F", "708005":"M/M_2"}

def main():
    print("Start script:", datetime.datetime.now())
    works = load_works()
    write_works(works)

def load_works():
    d_types = {"language": 'category',
               "word_count":'Int64',
               "restricted": bool,
               "complete": bool,
               }
    works = pd.DataFrame()
    chunks = []
    print("Opening file:", datetime.datetime.now())
    with pd.read_csv("works-20210226.csv",
                     dtype = d_types,
                     encoding = "utf-8",
                     parse_dates = True,
                     converters = {"tags":str},
                     engine = "c",
                     chunksize = 1000000) as reader:
        chunk_no = 0
        for chunk in reader:
            chunk_no +=1
            chunk.dropna(axis=1, how="all", inplace=True)

            chunk["tags"] = chunk["tags"].apply(lambda x: x.split("+")
            chunk["Rating"] = chunk.apply(lambda row : ratings_row(row), axis=1) 
            chunk["Warnings"] = chunk.apply(lambda row : warn_row(row), axis=1) 
            chunk["Categories"] = chunk.apply(lambda row : cat_row(row), axis=1)
            chunk["Tag_Count"] = cunk.apply(lambda row: count_tags(row), axis=1)

            warn_split = pd.DataFrame(chunk["Warnings"].str.split(",",expand = True).values, columns = list(WARNINGS.values()), index=chunk.index)
            cat_split =  pd.DataFrame(chunk["Categories"].str.split(",", expand = True).values, columns= list(CATEGORIES.values()), index=chunk.index)
            chunk = pd.concat([chunk, warn_split, cat_split], axis=1)
     
            if "True" in chunk["M/F"]:
                chunk["F/M"] = chunk.apply(lambda row: collapse(row, "F/M", "M/F"), axis=1)
            if "True" in chunk["M/M_2"]:
                chunk["M/M"] = chunk.apply(lambda row: collapse(row, "M/M", "M/M_2"), axis=1)
                
            chunk.drop(columns = ["Warnings", "Categories", "M/F", "M/M_2"], inplace=True)
            chunks.append(chunk)
            print("Chunk number", chunk_no, "processed.", len(chunk), "items in chunk.", datetime.datetime.now())
        print("Done with chunks!",datetime.datetime.now())
        
        works = pd.concat(chunks, ignore_index = True)
    print(len(works), "works loaded", datetime.datetime.now())
    return works


    
def ratings_row(row):
    ratings = {"9" : "Not Rated", "10": "General Audiences", "11": "Teen and Up",
               "12":"Mature", "13":"Explict"}
    for t in row["tags"]:
        if t in ratings:
            return ratings[t]

def warn_row(row):
    result = ""
    for w in WARNINGS:
        if w in row["tags"]:
            if result == "":
                result = "True"
            else:
                result += ",True"
        else:
            if result == "":
                result = "False"
            else:
                result += ",False"
    return result

def cat_row(row):
    result = ""
    for c in CATEGORIES:
        if c in row["tags"]:
            if result == "":
                result = "True"
            else:
                result += ",True"
        else:
            if result == "":
                result = "False"
            else:
                result += ",False"
    return result

def count_tags(row):
    return len(row["tags"])

def collapse(row, col1, col2):
    if row[col2] == "True":
        return "True"
    else:
        return row[col1]

               
def write_works(works):
    works.to_csv("clean_works.csv", index_label = "work_id", chunksize=10000)
    print("Saved!", datetime.datetime.now())

if __name__ == '__main__':
    main()
