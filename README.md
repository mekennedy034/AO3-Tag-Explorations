# AO3-Tag-Explorations
Various kinds of analysis on a data set from the Organization for Transformative Works describing the metadata tags used on An Archive of Our Own (AO3). Using Python and SQLite to clean and organize the data, then sending it to Tableau for visualization.

The original data can be downloaded from the OTW here: https://archiveofourown.org/admin_posts/18804

"tags with pandas.py" uses the Pandas package to clean and organize the tags csv, "works with pandas.py" does the same with the works. 
"sqlite_tags_db.py" loads the clean data in from csv files and creates four tables in an SQLite3 database.
