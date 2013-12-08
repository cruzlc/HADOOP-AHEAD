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


/* Classe Mapper para criar chaves 'idBloco' e 'numLinhaLocal': adaptado do codigo escrito por 
 * Gordon Linoff da empresa Data Miners, Inc. (http://www.data-miners.com)
 * Referencia - http://blog.data-miners.com (Postado em 25 Nov. 2009)
 * Objetivo - Adiciona em cada linha do bloco em que atua duas chaves: a identificacao do bloco
 * (idBloco) e o numero da posicao de cada linha no bloco (numLinhaLocal). A saida consiste
 * em um arquivo para cada bloco, onde as linhas dos blocos possuem as novas chaves.
 */
package AHEAD;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperNovaChave extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
	        
	static SequenceFile.Writer sfw; //Classe para escrever arquivo de saida do map
	static IntWritable idBloco = new IntWritable(0); //Identificacao do bloco do mapper
	static Text chaveSaida = new Text(""); //Nova chave
	static long numLinhaLocal = 0; //Armazena numeracao local das linhas
	static LongWritable valorNumLinhaLocal = new LongWritable(0); //Classe do tipo de dado na saida map
	
	//Metodo executado pela plataforma hadoop para configurar a tarefa map
	//Extrai informacao sobre a tarefa map e cria arquivo de saida map (sequence file)
	public void setup (Context setMapping) {
		String DirSalvaRegistros; 
		idBloco.set(setMapping.getConfiguration().getInt("mapred.task.partition", 0)); //Extrai id do bloco.
		DirSalvaRegistros = new String(setMapping.getConfiguration().get("PARAMETRO_DirSalvaRegistros"));//Recebe caminho HDFS de saida.
		if (DirSalvaRegistros.endsWith("/")) {
			DirSalvaRegistros.substring(0, DirSalvaRegistros.length());
		}//fim se
		try {
			 //Classe de sistema de arquivos usada para acesso ao HDFS
			 FileSystem fs = FileSystem.get(setMapping.getConfiguration());
			 //Prepara o escritor de sequence file na saida map. 
			 //Obs: nome do sequence file contem numero do bloco.
			 sfw = SequenceFile.createWriter(fs, setMapping.getConfiguration(), new Path(DirSalvaRegistros+"/"+String.format("registro%05d", idBloco.get())), Text.class, Text.class);
		   } catch (Exception e) {
			   e.printStackTrace();
	    }//fim try-catch
	} //fim setup
	
	//Metodo Map executado pela plataforma sobre cada linha do bloco.
	public void map(LongWritable chave, Text valor, Context Mapping) throws IOException, InterruptedException {
		valorNumLinhaLocal.set(++numLinhaLocal);//Primeira acao: Calcula offset de linha na bloco local
		chaveSaida.set(idBloco.toString() + '-' + numLinhaLocal);
		sfw.append(chaveSaida, valor);//Segunda acao: Escreve arquivo de saida contendo novas chaves (nao usa fase reducer)
		//Obs: verificar se saida esta compactada antes de checar o arquivo. Caso positivo, use a opcao -text 
		//no comando de leitura de arquivo: hadoop fs -text <caminho no hdfs> 
	} //fim do metodo map()
	
	//Metodo executado ao fim da tarefa mapper.		
	public void cleanup (Context closeMapping) throws IOException, InterruptedException {
		//Usando in-mapper combiner: emite pares apos metodo map().
		//Envia somente o maior numero de linha contada no bloco,
		//evitando fluxo na fase shuffle.
		closeMapping.write(idBloco, valorNumLinhaLocal);		    
		sfw.close(); //fecha o escritor arquivo na saida map.
	} // fim metodo cleanup()

} //fim classe MapperNovaChave
