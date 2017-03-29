hadoop-student-homework
=======================

starting project for students to do homework

Instructions
 
Part #1:
* Develop a MapReduce application to show which actors played in each movie.  The input data is imdb.tsv.
* The output data would have two columns: movie title and a semicolon separated list of actor names.
 
Each record in imdb.tsv represents actor, movie title, and year 
McClure, Marc (I)       Freaky Friday   2003


A: KEy: Movie Title; value --> 
Freaky Friday McClure, Marc (I); Name2; Name3; Name4

Coach Carter Name1; Name2


 
Part #2:
* Develop a MapReduce application to count how many movies each actor have played in. The input data to this homework is imdb.tsv file.
* The output data would have two columns: count and actor 
* We want the output to be sorted by count in descending order

25 Name1
20 Name2

 
Use attached hadoop-student-homework.zip project as a starting project. Part 1 should go into Homework2Part1.java and part 2 should go into Homework2Part2.java
 
For extra credit, write a few test cases using MRUnit - http://mrunit.apache.org/. 
 

p.s.
How to enabled MapReduce on Windows in local mode by modifying Apache open source file?

For those of you interested in running MapReduce on Windows in local mode but got the "File permission" error. Here is my workaround.
Copy the the attached FileUtil.class to your target\classes\org\apache\hadoop\fs\ , then it should work.
At run time it will also print "FileUtil: modified by Michael Hsing 8/24/2014" to the console when this modified class is loaded. I have also attached the java file for your reference.
Regards,
Mike
