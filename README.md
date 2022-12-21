# Rating TV shows and movies by genre

The file ratings_v2.csv is the data set of flims and tv series. The data comes from this GitHub : https://github.com/sharkdp/imdb-ratings/blob/master/ratings.csv.

The CSV has the following columns :

- Position 
- Const 
- Created 
- Modified 
- Description 
- Title 
- Title type 
- Directors 
- You rated 
- IMDb Rating 
- Runtime (mins) 
- Year 	
- Genres 
- Num. Votes 	
- Release Date (month/day/year) 
- URL

Tools used :

The data is retrieved and copied into HDFS to then be treated. 

Code :

In the project.py the data is retrieved and ables the user to chose between TV shows or movies. He can chose the genre and the number of ratings shown. 
If the user enters the wrong argument, a message of error is shown.

The columns Created and Modified were empty so they deleted. 
