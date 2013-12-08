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

/* Essa classe implementa no HashPartitioner Default do Hadoop: coloca
 * #VerticeId (chave) como parametro de entrada da funcao de particao.
 * O metodo getPartition dessa classe deve realizar a mesma operacao
 * do HashPartitioner usado no Job que realiza a distribuicao dos grafos,
 * pois assim deve ser para o uso da tecnica "Schimmy".
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class HashPartitionerMenorCaminho extends Partitioner <Text,Text>{
	
	//Metodo getPartition adaptado para as chaves dos pares <chave, valor> 
	//na saida Map da classe MapperCalculaMenorCaminho.
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		
		String[] chave = key.toString().split("\\.");
		Text VerticeId = new Text(chave[0]);
		return (VerticeId.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	
	}// fim metodo getPartition

}//Fim  da classe HashPartitioner adaptada
