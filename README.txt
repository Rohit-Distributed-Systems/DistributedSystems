1. Synopsis

The code will calculate the top 10 page ranks of the given input adjacency wep page list. Adjacency list is in the form of wep page and its corresponding outgoing0 links.
e.g.
0
1 2
2 1
3 0 1
4 1 3 5
5 1 4
6 1 4
7 1 4
8 1 4
9 4
10 4
2nd line of the input adjacency list given above suggests that web page 1 has one outgoing link and its to web page 2.

2. Code Example

public void loadInput() throws IOException {
}
Above function is responsible for loading the data from flat file to adjacency map list data structure.
In this function we are reading the input flat file line by line and putting it into the adjacency map data structure.
At the same time we are assigning initial page rank to all web pages. Initial Pagerank is 1/total number of pages.

public void calculatePageRank() {
}
Above function is responsible for calculating the page rank for given number of iterations.
we have implemented the code to calculate the pagerank of each page as per the given formula.


3. How to Run
The java file that you need to run is SequentialPageRank.java. 
Run the SequentialPageRank.java file with following command on commandpromt
Step1 Compilation: Javac SequentialPageRank.java
Step2 Run: Java SequentialPageRank <Input File Name> <Output file Name> <Iterations> <Damping Factor>

Note: On Git the File is under Ass1.PageRank/CodeAndData/ Folder.
	  There is alternate code under Ass1.PageRank/CodeAndData/DistributedSystemsProj/src/main/java/com/iub/fall2016/ds/project1/.
	  Please consider the first code i.e. under Ass1.PageRank/CodeAndData/


4. Contributors

Abhijit Karanjkar: aykaranj
Rohit Nair: ronair
