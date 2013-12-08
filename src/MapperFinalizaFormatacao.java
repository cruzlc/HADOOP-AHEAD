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


//Classe Mapper para adicionar offset das linhas de cada bloco. Cada arquivo de entrada corresponde 
//a um bloco gerado na parte 1.
//Adaptado do codigo escrito por Gordon Linoff da empresa Data Miners, Inc. (http://www.data-miners.com)
//Referencia - http://blog.data-miners.com (Postado em 25 Nov. 2009)
//Objetivo - Adiciona valor de offset em cada linha do arquivo original de entrada.
//Escrever outras funcionalidades!

package AHEAD;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperFinalizaFormatacao extends Mapper<Text, Text, LongWritable, Text> {
	
	//HashMap para guardar valores acumulados de linhas de cada bloco
	HashMap<String, Long> offsets = new HashMap<String, Long>();
	//Variavel para armazenar numero offset de uma linha
	LongWritable chave_saida = new LongWritable(0);
	
	public void setup(Context setMapping) {
		
		//Preenche HashMap com valores acumulados ate inicio de cada bloco
		int num_blocos = 0;//controle de loop
		//Recebe numero de blocos vindo de main() por parametroda configuracao
		num_blocos = setMapping.getConfiguration().getInt("PARAMETRO_somacumulativa_num_blocos",0);
		offsets.clear();
		for (int i = 0; i < num_blocos; i++) {
			String idBloco_numLinhasBloco_linAcumBloco = setMapping.getConfiguration().get("PARAMETRO_somacumulativa_bloco_n" + i);
			String[] campos = idBloco_numLinhasBloco_linAcumBloco.split("\t");
			offsets.put(campos[0], Long.parseLong(campos[2]));//armazena no hashmap o valor acumulado ate inicio do bloco n
		}// fim loop for
		
	} // fim do setup
	
	public void map(Text chave, Text valor, Context Mapping) throws IOException, InterruptedException {
		
		long numero_linha;		
		
		//Calcula numero da linha
		String[] campos = chave.toString().split("-");
		numero_linha = Long.parseLong(campos[1]) + offsets.get(campos[0]);
		
		//Guarda numero da linha como chave do par
		chave_saida.set(numero_linha);
		
		//Emite linha numero da linha juntamente com a linha original
		Mapping.write(chave_saida, valor);
		
	} //fim metodo map()

}// fim da classe MapperFinalizaFormatacao
