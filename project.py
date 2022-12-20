from pyspark.sql import SparkSession, Row
import sys

# Arguments that the user can put in
# put the sysarg in a variable
type = sys.argv[1] # movie or serie 1 or 2.  
genre = sys.argv[2] # genre of the movie or serie
nbmax = sys.argv[3] # number of movies or series to display

# If type not 1 or 2 end 
# If type is 1 then movie if type is 2 then serie
if type != "1" and type != "2":
    print("Error: type must be 1 or 2")
    exit()
elif type == "1":
    type = "Feature Film"
elif type == "2":
    type = "TV Series"


spark = SparkSession.builder.appName("project").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read the csv file
df = spark.sparkContext.textFile("hdfs:///user/maria_dev/project/ratings_v2.csv")

# Convert to RDD (split the data using quotes as separator) Skip the first line
RDD = df.map(lambda x: x.split(";"))

# Convert to dataframe
df = RDD.toDF()

# Test if there is 16 columns, rename the columns
df = df.withColumnRenamed("_1", "Position")
df = df.withColumnRenamed("_2", "Const")
df = df.withColumnRenamed("_3", "Created") # We dont need this column
df = df.withColumnRenamed("_4", "Modified") # We dont need this column
df = df.withColumnRenamed("_5", "Description")
df = df.withColumnRenamed("_6", "Title")
df = df.withColumnRenamed("_7", "Title type")
df = df.withColumnRenamed("_8", "Directors")
df = df.withColumnRenamed("_9", "You rated")
df = df.withColumnRenamed("_10", "IMDb Rating")
df = df.withColumnRenamed("_11", "Runtime (mins)")
df = df.withColumnRenamed("_12", "Year")
df = df.withColumnRenamed("_13", "Genres")
df = df.withColumnRenamed("_14", "Num. Votes")
df = df.withColumnRenamed("_15", "Release Date (month/day/year)")
df = df.withColumnRenamed("_16", "URL")

# Remove the columns we dont need
df = df.drop("Created")
df = df.drop("Modified")

# Remove the first line
df = df.filter(df.Position != "Position")

# Only keeps the lines where Title type = type
df = df.filter(df["Title type"] == type)

# Sort by IMDb Rating
df = df.sort(df["IMDb Rating"].desc())

# Only keep the line where Genres contains genre to lower case
df = df.filter(df["Genres"].contains(genre.lower()))

# Show only title and all genres and imdb rating only if there is some data
if df.count() > 0:
    df = df.select("Title", "Genres", "IMDb Rating", "Title type").show(int(nbmax),False)
else:
    print("No data found for the genre " + genre + " and the type " + type + "")


spark.stop()




