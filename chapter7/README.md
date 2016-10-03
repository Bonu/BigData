hdfs dfs -put *.txt input
brew install wget
wget -w 2 -m -H "http://www.gutenberg.org/robot/harvest?filetypes[]=txt"
cp www.gutenberg.lib.md.us/etext00/* input/

mvn clean compile package

hadoop jar target/invertedIndex.jar /Users/jbonu/data/sandbox/hadoop/hadoopcookbook/chapter7/input /Users/jbonu/data/sandbox/hadoop/hadoopcookbook/chapter7/output






   
   