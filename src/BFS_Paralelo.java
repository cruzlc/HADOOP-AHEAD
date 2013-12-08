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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.conf.Configuration;

 public class BFS_Paralelo {
	 
	//Contadores globais 
	static enum CondicoesDeParada { 
	  VerticesSemDistancia, //Deve ser usado no Mapper.
	  AtualizacoesDeDistancias, //Deve ser usado no Reducer.
	}
	 
	//Diretorio de trabalho no HDFS
	 String dir_trabalho = "/user/hdfs/processamento_grafo";
	
	//Diretorio de saida no HDFS da segunda parte da formatacao do grafo/lista de adjacencia
	//Sera o caminho de entrada para o calculo de menores caminhos.
	String resultados_primeira_fase = dir_trabalho + "/passo2_resultado_final" +"/0";
	 
	//Metodo distribuidor da estrutura do grafo: Modificacao do codigo escrito por Gordon Linoff 
	//da empresa Data Miners, Inc. (http://www.data-miners.com). O codigo de G. Linoff
	//numera sequencialmente as linhas do arquivo de entrada, mesmo ele estando distribuido.
	//Referencia - http://blog.data-miners.com (Postado em 25 Nov. 2009)
	//Objetivo - Modificado para acrescentar valores iniciais das distancias (diagonal da matriz zerada).
	//Adicionalmente, aplica a tecnica "partitioning" e "in-mapper combiner" na distribuicao do 
	//grafo entre os computadores do cluster.
	void DistribuiGrafo(String N_Reducers, String Caminho_Entrada) throws IOException, InterruptedException, ClassNotFoundException{
	    
		//PRIMEIRA PARTE (PRIMEIRO JOB): CONTA AS LINHAS EM CADA BLOCO DE DADO CRIADO NO HDFS A
		//PARTIR DO CARREGAMENTO DO ARQUIVO DE ENTRADA (LISTA DE ADJACENCIAS DO GRAFO).  
		
		//Cria configuracao do primeiro Job (sao duas partes => dois jobs)
		Configuration conf_NovasChaves = new Configuration();
		
		//Armazena numero de reducers passados por linha de comando
		int Numero_Reducers = Integer.parseInt(N_Reducers);
		
		//Apaga arquivos de saida antes de executar o Job 'NovasChaves'
		FileSystem sistema_arquivos;
		sistema_arquivos = FileSystem.get(conf_NovasChaves);//Construtor: retorna configuracao do sistema
	    sistema_arquivos.delete(new Path(dir_trabalho), true);
		String saida_sumario_chave = dir_trabalho + "/passo1_sumario_chave";
		String saida_registros = dir_trabalho + "/passo1_registros";
		conf_NovasChaves.set("PARAMETRO_DirSalvaRegistros", saida_registros);//Passa parametro para Mapper e Reducer
		
		//Criacao do Job para primeira parte de formatacao dos dados (NovasChaves).
		Job NovasChaves = new Job(conf_NovasChaves);
		NovasChaves.setJobName("Formata Lista - Parte 1");
		
		//Determina numero de reducers a ser usado no cluster
		NovasChaves.setNumReduceTasks(Numero_Reducers);
		
		//Informa a plataforma qual JAR distribuir.
		NovasChaves.setJarByClass(AHEAD.BFS_Paralelo.class);
		
		//Tipo do formato dos dados ao converter arquivo de entrada em blocos no HDFS
		//Obs: O formato default TextInputFormat.class, linha abaixo pode ser removida.
		NovasChaves.setInputFormatClass(TextInputFormat.class);
		
		//Tipo do formato dos dados ao converter arquivo de saida em arquivos no HDFS
	  	//Obs: O default eh TextInputFormat.class, linha abaixo pode ser removida.
		//Obs. Importante: Nao necessita de especificacao pois a saida ja foi escrita 
		//no formato sequencefile atraves de operacoes com arquivos no MapperNovasChaves.
		
		//Configuracao de caminhos no HDFS para entrada e saida de arquivos do Job 'NovasChaves'
		FileInputFormat.addInputPath(NovasChaves, new Path(Caminho_Entrada));//caminho do arquivo ou diretorio de entrada. 
		FileOutputFormat.setOutputPath(NovasChaves, new Path(saida_sumario_chave));//caminho do diretorio de saida (nao deve ser criado antes).
		
		//Configuracao do Mapper, do Combiner e do Reducer a serem distribuidos
		//Obs: Uso de in-mapper combiner em substituicao do combiner.
		NovasChaves.setMapperClass(AHEAD.MapperNovaChave.class);
		NovasChaves.setReducerClass(AHEAD.ReducerNovaChave.class);
		
		//Configuracao das classes usadas nos pares <chave,valor> de saida do Job.
		//<chave, valor> na saida do Job (reducer):
		NovasChaves.setOutputKeyClass(Text.class);
		NovasChaves.setOutputValueClass(Text.class);
		//Classe <chave, valor> na saida mapper:
		NovasChaves.setMapOutputKeyClass(IntWritable.class);
		NovasChaves.setMapOutputValueClass(LongWritable.class);
		
		//Submissao e monitoramento do Job Particionador do Grafo
		NovasChaves.waitForCompletion(true);
		
		//SEGUNDA PARTE (SEGUNDO JOB): CALCULA OFFSET DE CADA LINHA EM RELACAO A PRIMEIRA 
		//LINHA. COM ESSA INFORMACAO, AS COLUNAS REFERENTES AS DISTANCIAS DE CADA VERTICE 
		//ORIGEM SAO INICIADAS COM O VALOR ZERO (QUANDO ORIGEM = VERTICE DA LINHA) OU  
		//"INFINITO" (QUANDO NAO SE CONHECE A DISTANCIA PARA A ORIGEM). NESSE JOB OCORRE  
		//A PRIMEIRA E UNICA DISTRIBUICAO DA ESTRUTURA DO GRAFO USANDO O PARTITIONER.
		
		//Cria configuracao do segundo Job usado para formatacao final dos dados (FinalizaFormatacao).
		Configuration conf_FinalizaFormatacao = new Configuration();
		
		//Le arquivos de saida da primeira parte (saida sumario de chaves),
		//sendo a mesma uma saida reduce => nome de arquivos comecam com p.
		FileStatus[] arquivos = sistema_arquivos.globStatus(new Path(saida_sumario_chave + "/p*"));
		int num_blocos = 0;//controla a identificacao do bloco
		long soma_acumulada = 0;//guarda soma acumulada de numero de linhas dos blocos
		for(FileStatus statusArq : arquivos){
			//encapsulamentos necessarios para ler arquivo direto no HDFS
			DataInputStream dados = new DataInputStream(sistema_arquivos.open(statusArq.getPath()));
			BufferedReader leitor = new BufferedReader(new InputStreamReader(dados));
			String linha_arquivo = "";
			while ((linha_arquivo = leitor.readLine()) != null){
				//Debug
				//System.out.println("linha numero: " + num_blocos);
				conf_FinalizaFormatacao.set("PARAMETRO_somacumulativa_bloco_n" + num_blocos++, linha_arquivo + "\t" + soma_acumulada);
				String[] parcial = linha_arquivo.split("\t");
				soma_acumulada += Long.parseLong(parcial[1]);
			}//fim loop while
		leitor.close();
		//sistema_arquivos.close();
		}// fim loop for sobre nome de arquivos
		
		//Armazena numero de blocos cujas linhas foram contadas 
		conf_FinalizaFormatacao.setInt("PARAMETRO_somacumulativa_num_blocos", num_blocos);
		
		//Armazena o numero de vertices do grafo
		conf_FinalizaFormatacao.setLong("PARAMETRO_numero_vertices", soma_acumulada);
		
		//Criacao do Job segunda parte de formatacao dos dados (FinalizaFormatacao)
		Job FinalizaFormatacao = new Job(conf_FinalizaFormatacao);
		FinalizaFormatacao.setJobName("Formata Lista - Parte 2 (final)");
		
		//Determina numero de reducers a ser usado no cluster
		FinalizaFormatacao.setNumReduceTasks(Numero_Reducers);
		
		//Informa a plataforma qual JAR distribuir.
		FinalizaFormatacao.setJarByClass(AHEAD.BFS_Paralelo.class);
		
		//debug
		//System.out.println("Parametro1 (linha registro) = " + conf_FinalizaFormatacao.get("PARAMETRO_somacumulativa_bloco_n0"));
		//System.out.println("Parametro2 (num_blocos) = " + conf_FinalizaFormatacao.getInt("PARAMETRO_somacumulativa_num_blocos",0));
		//System.out.println("Parametro3 (num_vertices) = " + conf_FinalizaFormatacao.getLong("PARAMETRO_numero_vertices",0));
		
		//Tipo do formato dos dados ao converter arquivo de entrada em blocos no HDFS
		FinalizaFormatacao.setInputFormatClass(SequenceFileInputFormat.class);
		
		//Tipo do formato dos dados ao converter arquivo de saida em arquivos no HDFS
		//Obs: O default eh TextInputFormat.class, linha abaixo pode ser removida.
		FinalizaFormatacao.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		//Configuracao de caminhos no HDFS para entrada e saida de arquivos do Job 'Finalizaformatacao'
		FileInputFormat.addInputPath(FinalizaFormatacao, new Path(saida_registros));
		FileOutputFormat.setOutputPath(FinalizaFormatacao, new Path(resultados_primeira_fase));
		
		//Configuracao do Mapper, Particionador e Reducer a serem distribuidos
		FinalizaFormatacao.setMapperClass(AHEAD.MapperFinalizaFormatacao.class);
		FinalizaFormatacao.setPartitionerClass(AHEAD.HashPartitionerDistribuiGrafo.class);
		FinalizaFormatacao.setReducerClass(AHEAD.ReducerFinalizaFormatacao.class);
		
		//Configuracao das classes usadas nos pares <chave,valor> de saida do Job.
		//Classe <chave, valor> na saida mapper:
		FinalizaFormatacao.setMapOutputKeyClass(LongWritable.class);
		FinalizaFormatacao.setMapOutputValueClass(Text.class);
		//Classe <chave, valor> na saida do Job (reducer):
		FinalizaFormatacao.setOutputKeyClass(Text.class);
		FinalizaFormatacao.setOutputValueClass(Text.class);
		
		//Submissao e monitoramento do Job Particionador do Grafo
		//System.exit(FinalizaFormatacao.waitForCompletion(true) ? 0 : 1);
		FinalizaFormatacao.waitForCompletion(true);
		
	}//Fim Metodo Ditribuidor da estrutura do grafo
	
	//METODO PARA CALCULO DE MENORES CAMINHOS (TERCEIRO JOB). INVOCA-SE ESTE METODO ENQUANTO
	//HOUVER DETECCAO DE ATUALIZACAO VINDA DO CONTADOR PRESENTE NO REDUCER.
	long CalculaMenoresCaminhos(String N_Reducers, Integer num_dir) throws IOException, InterruptedException, ClassNotFoundException{
	   
	    //Criacao de configuracao para o Job de calcula distancias.
	    Configuration conf_CalculaDistancias = new Configuration();
		
	    //Primeiro diretorio do primeiro ciclo map-shuffle-reduce
	    String diretorio_corrente = resultados_primeira_fase.substring(0, resultados_primeira_fase.length()-1) + "/" + num_dir.intValue();
	    String diretorio_subsequente = resultados_primeira_fase.substring(0, resultados_primeira_fase.length()-1) + "/" + (num_dir.intValue()+1);
	 		
	    //Passa para o Reducer os caminhos dos diretorios de entrada e saida do grafo 
	    //formatado e particionado em arquivos. O diretorio de entrada (corrente) possui
	    //grafo ja particionado e cuja saida de processamento sera a entrada (subsequente) 
	    //para o proximo ciclo map-shuffle-reduce.
	    conf_CalculaDistancias.set("PARAMETRO_Diretorio_Corrente", diretorio_corrente);
	    conf_CalculaDistancias.set("PARAMETRO_Diretorio_Subsequente", diretorio_subsequente);
	   
	    //Armazena numero de reducers passados por linha de comando
	    int Numero_Reducers = Integer.parseInt(N_Reducers);
	   
	    //Criacao do Job para calculo dos menores caminhos entre todos os vertices.
	    Job CalculaDistancias = new Job(conf_CalculaDistancias);
	    CalculaDistancias.setJobName("Calcula Distancias");
	    
	    //Determina numero de reducers a ser usado no cluster
	    CalculaDistancias.setNumReduceTasks(Numero_Reducers);
	  		
	    //Informa a plataforma qual JAR distribuir.
	    CalculaDistancias.setJarByClass(AHEAD.BFS_Paralelo.class);
	    
	    //Tipo do formato dos dados ao converter arquivo de entrada em blocos no HDFS
	  	CalculaDistancias.setInputFormatClass(SequenceFileInputFormat.class);
	  		
	  	//Tipo do formato dos dados ao converter arquivo de saida em arquivos no HDFS
	  	//Obs: O default eh TextInputFormat.class, linha abaixo pode ser removida.
	  	CalculaDistancias.setOutputFormatClass(SequenceFileOutputFormat.class);
	  		
	    //Configuracao de caminhos no HDFS para entrada e saida de arquivos do Job 'CalculaDistancias'
	    FileInputFormat.addInputPath(CalculaDistancias, new Path(diretorio_corrente));//caminho do arquivo ou diretorio de entrada. 
	    FileOutputFormat.setOutputPath(CalculaDistancias, new Path(diretorio_subsequente));//caminho do diretorio de saida (nao deve estar criado antes).
	 
	    //Configuracao do Mapper, do Particionador e do Reducer a serem distribuidos
	    //Obs: Uso de in-mapper combiner e de combiner
	    CalculaDistancias.setMapperClass(AHEAD.MapperCalculaMenorCaminho.class);
	    CalculaDistancias.setPartitionerClass(AHEAD.HashPartitionerMenorCaminho.class);
	    CalculaDistancias.setCombinerClass(AHEAD.CombinerCalculaMenorCaminho.class);
	    CalculaDistancias.setReducerClass(AHEAD.ReducerCalculaMenorCaminho.class);
	  		
	    //Configuracao das classes usadas nos pares <chave,valor> de saida do Job.
	    //Classe <chave, valor> na saida do Job (serve para mapper e reducer):
	    CalculaDistancias.setOutputKeyClass(Text.class);
	    CalculaDistancias.setOutputValueClass(Text.class);
	   
	    //Configuracao das classes usadas nos pares <chave,valor> de saida do Job.
	    //Classe <chave, valor> na saida mapper:
	    CalculaDistancias.setMapOutputKeyClass(Text.class);
	    CalculaDistancias.setMapOutputValueClass(Text.class);
	  		
	    //Submissao e monitoramento do Job Particionador do Grafo
	    CalculaDistancias.waitForCompletion(true);
	   
	    //Apaga diretorio de entrada (ou saida do ciclo map-shuffle-reduce anterior) 
	    //apos atualizacoes das distancias.
	    FileSystem sistema_arquivos;
	    sistema_arquivos = FileSystem.get(conf_CalculaDistancias);
	    sistema_arquivos.delete(new Path(diretorio_corrente), true);
	    sistema_arquivos.close();
	   
	    long contador = CalculaDistancias.getCounters().findCounter(CondicoesDeParada.AtualizacoesDeDistancias).getValue();
       
	    return contador;
	   
	}//Fim metodo que calcula os menores caminhos.

	//METODO PARA CALCULO DE RAIO E DIAMETRO DO GRAFO (QUARTO JOB). ESSE METODO DEVE SER INVOCADO DEPOIS
	//QUE TODOS OS MENORES CAMINHOS JA TIVEREM SIDO CALCULADOS.
	void CalculaRaioDiametro(Integer num_dir) throws IOException, InterruptedException, ClassNotFoundException{
	    
		//Criacao de configuracao para o Job que calcula raio e diametro do grafo.
	    Configuration conf_CalculaRaioDiametro = new Configuration();
        
	    //Primero diretorio do primeiro ciclo map-shuffle-reduce
	    String diretorio_corrente = resultados_primeira_fase.substring(0, resultados_primeira_fase.length()-1) + "/" + num_dir.intValue();
	    String diretorio_saida = resultados_primeira_fase.substring(0, resultados_primeira_fase.length()-1) + "/" + "resultado_diametro_raio";
	    
	    //Criacao do Job para calculo do raio e diametro.
	    Job CalculaRaioDiametro = new Job(conf_CalculaRaioDiametro);
	    CalculaRaioDiametro.setJobName("Calcula Raio e Diametro");
	    
	    //Determina o numero de reducers a ser usado no cluster. Nesse caso nao ha
	    //necessidade de mais do que dois reducers: um deles seleciona a menor das 
	    //menores distancias e ou outro seleciona a maior das maiores distancias.
	    CalculaRaioDiametro.setNumReduceTasks(2);
	    
	    //Informa a plataforma qual JAR distribuir.
	    CalculaRaioDiametro.setJarByClass(AHEAD.BFS_Paralelo.class);
	    
	    //Tipo do formato dos dados ao converter arquivo de entrada em blocos no HDFS
	  	CalculaRaioDiametro.setInputFormatClass(SequenceFileInputFormat.class);
	  	
	    //Tipo do formato dos dados ao converter arquivo de saida em arquivos no HDFS
	  	CalculaRaioDiametro.setOutputFormatClass(SequenceFileOutputFormat.class);
	  	
	    //Configuracao de caminhos no HDFS para entrada e saida de arquivos do Job 'CalculaDistancias'
	    FileInputFormat.addInputPath(CalculaRaioDiametro, new Path(diretorio_corrente));//caminho do arquivo ou diretorio de entrada. 
	    FileOutputFormat.setOutputPath(CalculaRaioDiametro, new Path(diretorio_saida));//caminho do diretorio de saida (nao deve estar criado antes).
	
	    //Configuracao do Mapper, do Particionador e do Reducer a serem distribuidos
	    //Obs: Uso de in-mapper combiner em substituicao do combiner.
	    CalculaRaioDiametro.setMapperClass(AHEAD.MapperCalculaRaioDiametro.class);
	    CalculaRaioDiametro.setPartitionerClass(AHEAD.HashPartitionerRaioDiametro.class);
	    CalculaRaioDiametro.setReducerClass(AHEAD.ReducerCalculaRaioDiametro.class);
	    
	    //Configuracao das classes usadas nos pares <chave,valor> de saida do Job.
	    //Classe <chave, valor> na saida do Job (serve para mapper e reducer):
	    CalculaRaioDiametro.setOutputKeyClass(Text.class);
	    CalculaRaioDiametro.setOutputValueClass(IntWritable.class);
	    
	    //Submissao e monitoramento do Job Particionador do Grafo
	    CalculaRaioDiametro.waitForCompletion(true);
	    
	}//Fim do metodo para calculo de raio e diametro.
	
	//METODO MAIN()
	public static void main(String[] args) throws Exception { 
		long contador_atualizacoes = 0;
		boolean flag = true;
		int NumeroDaPasta = 0;
		try {		
	
			if(args.length != 2) {
				System.err.println("Comando: hadoop jar AHEAD.jar AHEAD.BFS_Paralelo <numero de reducers> <caminho de entrada no HDFS>");
				System.exit(-1); 
			}// fim se	
	        
			//Cria instancia do "driver program".
			BFS_Paralelo processa_grafo = new BFS_Paralelo(); 	
			
			//Coloca arquivo com a lista de adjacencias no formato requerido pelo
			//algoritmo AHEAD e distribui o grafo entre particoes, onde cada particao
			//constitui um arquivo (part-r-00000 ate part-r-n, n sendo o numero de reducers).
			processa_grafo.DistribuiGrafo(args[0], args[1]);
			
			//Calcula distancias entre os vertices.
			while (flag){
			 contador_atualizacoes = 0;	
			 contador_atualizacoes = processa_grafo.CalculaMenoresCaminhos(args[0], NumeroDaPasta);
			 if (contador_atualizacoes == 0)
				 flag = false;
			 NumeroDaPasta++;
			}// fim enquanto
			
			//Calcula Raio e Diametro do Grafo
			processa_grafo.CalculaRaioDiametro(NumeroDaPasta);
					
		}// fim do bloco try
		catch (Exception e) {
			e.printStackTrace();
		}// fim do bloco catch
	}//fim metodo main()
}//fim da Classe BFS_Paralelo
