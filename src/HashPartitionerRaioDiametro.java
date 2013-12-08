/*

AHEAD - Advanced Hadoop Exact Algorithm for Distances
Programa desenlvovido para o projeto de pesquisa entitulado "Um Algoritmo Paralelo Eficiente para C�lculo da Centralidade em Grafos."

Refer�ncias:

CRUZ, L. C. ; MURTA, C. D. . Um Algoritmo Paralelo Eficiente para C�lculo da Centralidade em Grafos.
In: XIV Simp�sio em Sistemas Computacionais (WSCAD-SSC), 2013, Porto de Galinhas PE. 
Anais do XIV Simp�sio em Sistemas Computacionais (WSCAD-SSC). Porto Alegre: Sociedade Brasileira de Computa��o, 2013. p. 3-10.

Leonardo Carlos da Cruz. Um Algoritmo Paralelo Eficiente para C�lculo da Centralidade em Grafos. 2013. 
Disserta��o (Mestrado em Modelagem Matem�tica e Computacional)
Centro Federal de Educa��o Tecnol�gica de Minas Gerais, . Orientador: Cristina Duarte Murta.

*/


package AHEAD;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class HashPartitionerRaioDiametro extends Partitioner <Text, IntWritable> {
	
	//Metodo getPartition adaptado para as chaves dos pares <chave, valor> 
	//na saida Map da classe MapperCalculaRaioDiametro.
	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		
		if(key.toString().equals("R"))
			return 0; //Envia para o reducer que recebe candidatos a valores de raio do grafo.
		else
			return 1; //Envia para o reducer que recebe candidatos a valores de diametro do grafo.	
	
	}// fim metodo getPartition


}//Fim da classe HashPartitioner adaptada
