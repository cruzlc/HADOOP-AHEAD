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

//Classe Reducer para contar as linhas de cada bloco. A contagem ja foi feita indiretamente 
//no mapper (MapperNovaChave) e portanto basta selecionar aqui a maior contagem de linha
//feita na fase map. Essa funcao Reducer tambem eh usada como combiner.
//OBS: Uma possibilidade eh nao usa-la como combiner, implementando o IN-MAPPER combiner
//no mapper => Feito!
//Classe adaptada do codigo escrito por Gordon Linoff da empresa Data Miners, Inc.
//(http://www.data-miners.com)
//Referencia - http://blog.data-miners.com (Postado em 25 Nov. 2009)
//Objetivo: Selecionar maior numeracao de linha do idBloco.

package AHEAD;

import java.io.IOException;
//import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerNovaChave extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
	
	public void reduce(IntWritable chave, Iterable<LongWritable> valores, Context Reducing) throws IOException, InterruptedException {
		LongWritable valMax = new LongWritable(Long.MIN_VALUE);
		while (valores.iterator().hasNext()) {//Percorre lista de valores ate o final
			long val = valores.iterator().next().get();//Armazena um valor da lista
			if (valMax.get() < val) {//Compara com o maximo encontrado ate o momento
				valMax.set(val);//Armazena o maximo do idBloco.
			}// fim se
		}// fim enquanto
		Reducing.write(chave, valMax);
	}// fim metodo reduce
}// fim da classe ReducerNovaChave
