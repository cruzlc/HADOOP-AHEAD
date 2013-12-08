/*

AHEAD - Advanced Hadoop Exact Algorithm for Distances
Programa desenlvovido para o projeto de pesquisa entitulado "Um Algoritmo Paralelo Eficiente para Cálculo da Centralidade em Grafos."

Referências:

CRUZ, L. C. ; MURTA, C. D. . Um Algoritmo Paralelo Eficiente para Cálculo da Centralidade em Grafos.
In: XIV Simpósio em Sistemas Computacionais (WSCAD-SSC), 2013, Porto de Galinhas PE. 
Anais do XIV Simpósio em Sistemas Computacionais (WSCAD-SSC). Porto Alegre: Sociedade Brasileira de Computação, 2013. p. 3-10.

Leonardo Carlos da Cruz. Um Algoritmo Paralelo Eficiente para Cálculo da Centralidade em Grafos. 2013. 
Dissertação (Mestrado em Modelagem Matemática e Computacional)
Centro Federal de Educação Tecnológica de Minas Gerais, . Orientador: Cristina Duarte Murta.

*/


/*
 * HadoopPartitioner.java
 *
 * Created on 19/12/2012, 15:05:45
 */

package AHEAD;

import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class HashPartitionerDistribuiGrafo extends Partitioner<LongWritable,Text> {
    
    @Override
    public int getPartition(LongWritable key, Text value, int numReduceTasks) {
    	
    	String valor = value.toString().substring((value.toString().indexOf('#')) + 1, value.toString().indexOf('\t', value.toString().indexOf('#')));
        Text VerticeId = new Text (valor);
        return (VerticeId.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
    
}
