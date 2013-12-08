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

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombinerCalculaMenorCaminho extends Reducer <Text, Text, Text, Text> {
	/*
	 * O objetivo do combiner sera eliminar as repeticoes de chaves devido a limitacoes da memoria:
	 * quanto maior a memoria, menor sera a quantidade de pares chave/valor emitidos na saida do
	 * MapperCalculaMenoresDistancias. Aqueles que forem repetidos na saida do mapper devido as, 
	 * limitacoes de memoria, serao elimidados por esse combiner, quando o mesmo for utilizado.
	 * A chamada do combiner depende exclusivamente da plataforma Hadoop, nao depende do codigo
	 * do combiner.
	 */
	
	Text valor_saida;
	
	public void setup (Context Combining) {
		valor_saida = new Text();
	}// fim setup
	
	public void reduce(Text chave, Iterable<Text> valores, Context Combining) throws IOException, InterruptedException{
		valor_saida.set(valores.iterator().next().toString());
		Combining.write(chave, valor_saida); 
		valor_saida.clear();
	}//Fim metodo reduce
	
	public void cleanup (Context closeReducing){
		valor_saida = null;
	}//fim cleanup

}//Fim classe CombinerCalculaMenorCaminho
