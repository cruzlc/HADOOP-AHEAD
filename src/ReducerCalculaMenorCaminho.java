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
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import AHEAD.BFS_Paralelo.CondicoesDeParada;

public class ReducerCalculaMenorCaminho extends Reducer <Text, Text, Text, Text> {
	
	//Armazena as distancias para serem atualizadas.
	HashMap<String, String> OrigensDistancias; 
	
	//Armazena identificacao do bloco da instancia Reducer.
	Integer idBloco;
	
	//Armazena caminho dos arquivos referentes a particao do grafo.
	String DirGrafoCorrente;
	
	//Armazena a porcentagem do Memory Heap a ser usado por HashMap
	static double porcentagem_HashMap = 1;
	
	//Armazena quantidade de memoria livre
	double memoria_livre = 0;
	
	public int maximo_itens() {
		  
		  //Calcula tamanho de maximo da memoria em termos itens Hash.
			  
		  int capacidade_hash_max;
		  double memoria_livre = 0;
		  memoria_livre = Runtime.getRuntime().freeMemory();
     	          
		  //Para HashMap
		  capacidade_hash_max = (int) ((memoria_livre)*(porcentagem_HashMap) / (64 + 30 + 4));  
			  
		  return(capacidade_hash_max);
			
	}//Fim metodo maximo_itens
	
	public void setup (Context setReducing) {
	  
	  //Aloca objeto HashMap que armazena distancias.
	  OrigensDistancias = new HashMap<String, String>(this.maximo_itens());
	  
	  //Identifica o numero da particao que essa instancia do Reducer processa.
	  //Essa identificacao deve ser a mesma gerada pelo metodo getPartitionId do particionador.
	  idBloco = (setReducing.getConfiguration().getInt("mapred.task.partition", 0));
	  
	  //Recebe diretorio do grafo processado corrente.
	  DirGrafoCorrente = new String(setReducing.getConfiguration().get("PARAMETRO_Diretorio_Corrente"));
	  if (DirGrafoCorrente.endsWith("/")){
	     DirGrafoCorrente.substring(0, DirGrafoCorrente.length());
	  }//fim se
	  
	  //val_distancia_min = new StringBuilder();

	}// Fim metodo Setup
	
	
    public void flush (Context Flushing) throws IOException{
		
    	        //Classe para ler arquivo da instancia Reducer.
		SequenceFile.Reader sfr = null; 
		
		//Classe para escrever arquivo atualizado da instancia Reducer.
		SequenceFile.Writer sfw = null; 
		
		//Armazena configuracoes referentes ao sistema de arquivos HDFS.
		FileSystem fs = FileSystem.get(Flushing.getConfiguration());
			    
		//Variaveis para receber conteudo do arquivo
		Text VerticeId = new Text(), ListaVizinhosDistancias = new Text();
		StringBuilder ch_VerticeId = new StringBuilder(), raizVerticeId = new StringBuilder();
		StringBuilder distancia_atualizada = new StringBuilder(), 
				      caminho_arquivo_antigo = new StringBuilder(), 
				      caminho_arquivo_atualizado = new StringBuilder();
		String[] array_completo = null;
		
		//Instancia escritores e leitores de arquivos.
	        //Prepara o leitor de sequence file, sendo o mesmo um arquivo Reduce: 
		//assim o nome do sequence file contem part-r-*.
	    try{
		    sfr = new SequenceFile.Reader(fs, new Path(DirGrafoCorrente + "/" + String.format("part-r-%05d", idBloco.intValue())), Flushing.getConfiguration());
	        sfw = SequenceFile.createWriter(fs, Flushing.getConfiguration(), new Path(DirGrafoCorrente + "/" + String.format("atualizacao%05d", idBloco.intValue())), Text.class, Text.class);
		
		    while (sfr.next(VerticeId, ListaVizinhosDistancias)){
			
		           //Separa lista de vizinho (posicao [0]) da lista de origens (o restante das posicoes)
			    array_completo = ListaVizinhosDistancias.toString().split("x");
			    //Separa o numero do vertice que esta na linha do arquivo
			    raizVerticeId.append(VerticeId.toString().substring((VerticeId.toString().indexOf('#')) + 1, VerticeId.toString().length())); 
			    //Reserva memoria para string de arquivo atualizada
			    distancia_atualizada.ensureCapacity(VerticeId.getLength() + String.valueOf('\t').getBytes().length + ListaVizinhosDistancias.getLength());
			    //Coloca lista de vizinhos no inicio da string
			    distancia_atualizada.append(array_completo[0]);	
			
			    for (int i=1;i<=(array_completo.length)-1;i++){
				    distancia_atualizada.append("x");
				    Flushing.progress();
				    //Remove os vertices recem descobertos no job anterior (nao sao mais recem descobertos)
				    if((array_completo[i].contains("*"))){
					    distancia_atualizada.append(array_completo[i].split("\\*")[0]);
				    }else{
					    if(array_completo[i].equals("d")){ //se ja foi atualizado antes, nao entra.
					      ch_VerticeId.setLength(0);
					      ch_VerticeId.append(raizVerticeId);
					      ch_VerticeId.append('.');
					      ch_VerticeId.append(i);
					      //System.out.println("DEBUG3");
					      //System.out.println("chVerticeId: " + ch_VerticeId + " Distancia: " + distancia);
					      if (OrigensDistancias.containsKey(ch_VerticeId.toString()) == true){//se nao ha mensagem de atualizacao, nao entra.
				        	    distancia_atualizada.append(OrigensDistancias.get(ch_VerticeId.toString()));
				        	    distancia_atualizada.append("^");
					            Flushing.getCounter(CondicoesDeParada.AtualizacoesDeDistancias).increment(1);
				          }else{distancia_atualizada.append(array_completo[i]);}//Fim se ha distancia para atualizar.
					    }else{distancia_atualizada.append(array_completo[i]);}//Fim se ja foi atualizado.
				    }//Fim Se houve atualizacao em job imediatamente anterior
			    }//Fim for
			    distancia_atualizada.append("x");
			    ListaVizinhosDistancias.set(distancia_atualizada.toString());
			    sfw.append(VerticeId, ListaVizinhosDistancias);
			
			    //Limpa variaveis
			    array_completo = null;
			    raizVerticeId.setLength(0);
			    ch_VerticeId.setLength(0);
			    distancia_atualizada.setLength(0);
			    ListaVizinhosDistancias.clear();
			    VerticeId.clear();
			
		    }//fim while
	    }//fim try
	    finally {
	    	IOUtils.closeStream(sfw);
	    	IOUtils.closeStream(sfr);
	    }//fecha ponteiros de arquivos
		
		OrigensDistancias.clear();
		distancia_atualizada = null;
		ch_VerticeId = null;
		raizVerticeId = null;
		
		//Substitui conteudo do arquivo part-r-n por conteudo de atualizacao.
		caminho_arquivo_antigo.append(DirGrafoCorrente + "/" + String.format("part-r-%05d", idBloco.intValue()));
		caminho_arquivo_atualizado.append(DirGrafoCorrente + "/" + String.format("atualizacao%05d", idBloco.intValue()));
		fs.delete(new Path(caminho_arquivo_antigo.toString()), false);
		fs.rename(new Path(caminho_arquivo_atualizado.toString()), new Path(DirGrafoCorrente + "/" + String.format("part-r-%05d", idBloco.intValue())));
		caminho_arquivo_antigo.setLength(0);
		caminho_arquivo_atualizado.setLength(0);
	
    }//fim flush()
	
	public void reduce(Text chave, Iterable<Text> valores, Context Reducing) throws IOException, InterruptedException {
		
		//Variavel para armazenar o menor valor associado a uma dada chave, onde
		//Chave = destino.origem, Valor = lista de distancias.
		//A primeira distancia calculada na fase map para uma determinada origem e destino sera
		//a menor distancia, pois trata-se de grafo nao ponderado. Alem disso, todas as distancias 
		//calculadas para uma origem/destino em particular serao iguais, pois trata-se de busca por
		//expansao de fronteiras, ou seja,todos os vertices estao equidistantes de uma determinada
		//origem. Assim, basta selecionar um valor da lista recebida no reducer.
		
		//Armazena distancia para ser gravada no arquivo posteriormente.
		if(OrigensDistancias.size() < (this.maximo_itens())){
		  OrigensDistancias.put(chave.toString(), valores.iterator().next().toString());
		}//fim Se
		else{
			OrigensDistancias.put(chave.toString(), valores.iterator().next().toString());
			this.flush(Reducing);
		}//Fim Senao teste de memoria
	
	}//Fim metodo reduce

	public void cleanup (Context closeReducing) throws IOException, InterruptedException {
		
		//Classe para leitura de arquivo.
		SequenceFile.Reader sfr_final = null;
		
		//Classe usada para armazenar configuracoes de sistema de arquivos necessarias para acesso ao HDFS
		FileSystem fs_final = FileSystem.get(closeReducing.getConfiguration());
		
		//Variaveis para receber conteudo do arquivo
		Text VerticeId = new Text(), ListaVizinhosDistancias = new Text();
		StringBuilder ch_VerticeId = new StringBuilder(), raizVerticeId = new StringBuilder();
		StringBuilder distancia_atualizada = new StringBuilder();
		String[] array_completo = null;
		
		//Prepara o leitor de sequence file, sendo o mesmo um arquivo Reduce: 
	    //assim o nome do sequence file contem part-r-*.
		try{
	        sfr_final = new SequenceFile.Reader(fs_final, new Path(DirGrafoCorrente + "/" + String.format("part-r-%05d", idBloco.intValue())), closeReducing.getConfiguration());
		
		    while (sfr_final.next(VerticeId, ListaVizinhosDistancias)){
			
			    //Separa lista de vizinho (posicao [0]) da lista de origens (o restante das posicoes)
			    array_completo = ListaVizinhosDistancias.toString().split("x");
			    //Separa o numero do vertice que esta na linha do arquivo
			    raizVerticeId.append(VerticeId.toString().substring((VerticeId.toString().indexOf('#')) + 1, VerticeId.toString().length())); 
			    //Reserva memoria para string de arquivo atualizada
			    distancia_atualizada.ensureCapacity(VerticeId.getLength() + String.valueOf('\t').getBytes().length + ListaVizinhosDistancias.getLength());
			    //Coloca lista de vizinhos no inicio da string
			    distancia_atualizada.append(array_completo[0]);
			
			    for (int i=1;i<=(array_completo.length)-1;i++){
				    distancia_atualizada.append("x");
				    closeReducing.progress();//para evitar failed to report
				    //Remove os vertices recem descobertos no job anterior (nao sao mais recem descobertos)
				    if((array_completo[i].contains("^"))){
					    distancia_atualizada.append(array_completo[i].split("\\^")[0]);
					    distancia_atualizada.append("*");	
				    }else{ 
					       if((array_completo[i].contains("*"))) {
						       distancia_atualizada.append(array_completo[i].split("\\*")[0]);
					       }else{
						       if(array_completo[i].equals("d")){ //se ja foi atualizado antes, nao entra.
						         ch_VerticeId.setLength(0);
					             ch_VerticeId.append(raizVerticeId);
					             ch_VerticeId.append('.');
					             ch_VerticeId.append(i);
					             //distancia = OrigensDistancias.get(ch_VerticeId.toString());
					             //System.out.println("DEBUG3");
					             //System.out.println("chVerticeId: " + ch_VerticeId + " Distancia: " + distancia);
					             if (OrigensDistancias.containsKey(ch_VerticeId.toString()) == true){//se foi atualizado antes, nao entra
				                    distancia_atualizada.append(OrigensDistancias.get(ch_VerticeId.toString()));
				                    distancia_atualizada.append("*");
					                closeReducing.getCounter(CondicoesDeParada.AtualizacoesDeDistancias).increment(1);
							//closeReducing.progress(); => primeira tentativa de resolver failed to report
				                 }else{distancia_atualizada.append(array_completo[i]);}//Fim Se distancia infinita
					           }else{distancia_atualizada.append(array_completo[i]);}//Fim se ja foi atualizado. 
				          }//Fim senao contem *
				    }//Fim senao contem ^
			    }//Fim for
			    distancia_atualizada.append("x");
			    ListaVizinhosDistancias.set(distancia_atualizada.toString());
			    closeReducing.write(VerticeId, ListaVizinhosDistancias);
			    //Limpa variaveis
			    array_completo = null;
			    raizVerticeId.setLength(0);
			    //raizVerticeId = null;
			    ch_VerticeId.setLength(0);
			    distancia_atualizada.setLength(0);
			    ListaVizinhosDistancias.clear();
			    VerticeId.clear();
		
		    }//fim while
		}//fim try
		finally {
	    	IOUtils.closeStream(sfr_final);
	    }//fecha ponteiros de arquivos
		
		OrigensDistancias.clear();
		OrigensDistancias = null;
		distancia_atualizada = null;
		ch_VerticeId = null;
		raizVerticeId = null;
	
		
	}//Fim Metodo Cleanup
	
}// Fim classe ReducerCalculaMenorCaminho
