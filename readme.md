#Inverted-index example 

MapReduce job written in JAVA to generate an inverted index.

##Create the jar

```
mvn clean package
```

##Usage

```
hadoop -jar jarfile com.globant.training.invertedIndex.InvertedIndexDriver <hdfs input folder> <hdfs output folder>
```

##Output format

The output format is the following:

```
Word	(filename, index, index, index...) ... (filename, index, ... index)
....
Word ....
```

An example:

```
does	(the_adventures_of_sherlock_holmes.txt,581635),(alices_adventures_in_wonderland.txt,164134),(siddhartha.txt,227897),(dracula.txt,869839),(leaves_of_grass.txt,7623
43),(the_prince.txt,292590),(les_miserables.txt,3309377),(the_adventures_of_tom_sawyer.txt,408791),(treasure_island.txt,378289),(don_quixote.txt,2334459),(the_scarlet_let
ter.txt,503134),(the_picture_of_dorian_gray.txt,448790),(the_republic.txt,1225687),(a_christmas_carol.txt,173531)
doorway;	(alices_adventures_in_wonderland.txt,157418)
(drum)	(les_miserables.txt,2034743)
(duds)	(les_miserables.txt,2241498)
(duke)	(les_miserables.txt,2235987)
(easily	(leaves_of_grass.txt,437698)
(eg	(the_republic.txt,3288)
especially	(treasure_island.txt,369268)
even	(les_miserables.txt,1380260),(leaves_of_grass.txt,446932,753138),(don_quixote.txt,1988755)
ever-enlarging	(leaves_of_grass.txt,413722)
excuse	(don_quixote.txt,1178590)
floating	(leaves_of_grass.txt,88152)
```

If you want to quickly run a manual search to make sure it worked:

```
hdfs dfs -cat <output-folder>/part-r-00000 | grep 'SearchTerm' | more
```
