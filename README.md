HADOOP-AHEAD
============
Advanced Hadoop Exact Algorithm for Distances - Calculates diameter and radius of an unweighted graph.

Data input
==========
The input file must be a plain text file with a graph represented as follows:

- Each line of the file must have the character "#" followed by a vertex, a space and then the vertex's list of neighbours. Each neighbour must have be separated by semicolons. 
 
Example:

#vertex_name1 neighbour_name1;neighbour_name2;...;last_neighbour
#vertex_name2 neighbour_name1;neighbour_name2;...;last_neighbour
                                .
                                .
                                .
#vertex_nameN neighbour_name1;neighbour_name2;...;last_neighbour

The neighbour's names should be the same as the vertex's names that follows the "#" character, meaning that each neighbour may have its own list of neighbours.

How to run AHEAD
================

This first version was tested using scripts that automatically execute usual Hadoop command lines. Therefore, the script that starts AHEAD should be edit/adjusted accordingly with your Hadoop setup.

The script's name is "run_AHEAD.sh" and you can use it by typing "./run_PBFS.sh -r <numero de reducers> -c <caminho do arquivo>" where "numero de reducers" is the number of reducers desired and "caminho do arquivo" is the path of the Graph file (which format was decribed above).
