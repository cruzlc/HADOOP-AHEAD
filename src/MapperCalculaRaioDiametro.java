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
import org.apache.hadoop.mapreduce.Mapper;

public class MapperCalculaRaioDiametro extends Mapper<Text, Text, Text, IntWritable> {
	
	int [] min_max;
	
	public void setup (Context setMapping) {
	  
	  min_max = new int [2];
	  min_max[0] = Integer.MAX_VALUE; //Armazena o menor valor de excentricidade (raio).
	  min_max[1] = Integer.MIN_VALUE; //Armazena o maior valor de excentricidade (diametro).
	
	}// Fim de setup
    
	public void map(Text chave, Text valor, Context Mapping) throws IOException, InterruptedException {
		
		String[] sub_linha_origens;
		int excentricidade = Integer.MIN_VALUE;
		
		sub_linha_origens = valor.toString().substring(valor.toString().indexOf('x',valor.toString().indexOf('\t')), valor.toString().length()).split("x");
			
		for (int j = 1; j <= (sub_linha_origens.length)-1; j++){
		  if(!(sub_linha_origens[j].equals("d")) && !(sub_linha_origens[j].equals("0"))){	
			if(Integer.parseInt(sub_linha_origens[j]) > excentricidade)  
				excentricidade = Integer.parseInt(sub_linha_origens[j]);
		  }// fim se distancia = d (infinita) ou = 0 (origem = destino)
		}//fim de for
		
		if(excentricidade < min_max[0] && excentricidade!=Integer.MIN_VALUE)
			min_max[0] = excentricidade;
		if(excentricidade > min_max[1])
			min_max[1] = excentricidade;
		
	}//fim do metodo map()
	
	public void cleanup (Context closeMapping) throws IOException, InterruptedException {
		
		Text chave = new Text();
		IntWritable valor = new IntWritable();
		
		chave.set("R");
		valor.set(min_max[0]);
		closeMapping.write(chave, valor);
		
		chave.set("D");
		valor.set(min_max[1]);
		closeMapping.write(chave, valor);
		
	}// Fim metodo clean up
	
}//fim classe MapperCalculaExcentricidade
