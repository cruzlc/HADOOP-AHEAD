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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerCalculaRaioDiametro extends Reducer <Text, IntWritable, Text, IntWritable>{

	public void reduce(Text chave, Iterable<IntWritable> valores, Context Reducing) throws IOException, InterruptedException {
		int idReducer, valMax = Integer.MIN_VALUE, valMin = Integer.MAX_VALUE;
		idReducer = (Reducing.getConfiguration().getInt("mapred.task.partition",0));
		if (idReducer == 0){
			while (valores.iterator().hasNext()) {//Percorre lista de valores ate o final
				int val = valores.iterator().next().get();//Armazena um valor da lista
				if (valMin > val) {//Compara com o minimo encontrado ate o momento
					valMin = val;//Armazena o minimo do idBloco.
				}// fim se
			}// fim enquanto
			Reducing.write(new Text ("Raio:") , new IntWritable(valMin));
		}//Fim se
		else{
			  while (valores.iterator().hasNext()) {//Percorre lista de valores ate o final
				 int val = valores.iterator().next().get();//Armazena um valor da lista
				 if (valMax < val) {//Compara com o maximo encontrado ate o momento
					  valMax = val;//Armazena o maximo do idBloco.
				}// fim se
			}// fim enquanto
			Reducing.write(new Text ("Diametro:") , new IntWritable(valMax));
		}//Fim else
			
	}//Fim do metodo reduce
	
}//Fim da classe ReducerCalculaRaioDiametro
