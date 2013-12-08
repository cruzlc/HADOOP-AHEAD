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
