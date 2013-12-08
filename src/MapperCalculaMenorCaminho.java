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
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperCalculaMenorCaminho extends Mapper<Text, Text, Text, Text> {
	
	//Colecoes HashMap para controle de pares chave/valor no MapReduce.
	HashMap<String, Integer> InMapperCombiner;
	HashSet<String> Visitados;
	
	//Variaveis usadas nos metodos map() e cleanup().
	StringBuilder ch, ch_visitado, destino_visitado;
	Text chave_saida, valor_saida;
	
	//Registra a porcentagem do Memory Heap livre a ser usado pelo HashMap.
	static double porcentagem_HashMap = 1;
	
	public int itensHash (int tipo){
		
	  //Calcula tamanho potencial do Hash.
	  int capacidade_Hash;
	  double memoria_livre = 0;
	  memoria_livre = Runtime.getRuntime().freeMemory();
	  
	  if (tipo == 0){
		  //Para HashMap
		  capacidade_Hash = (int) ((memoria_livre)*(porcentagem_HashMap) / (64 + 30 + 4));  
	  }else{
		  //Para HashSet
		  capacidade_Hash = (int) ((memoria_livre)*(porcentagem_HashMap) / (64 + 30));
	  }//fim if-else
	  
	  return(capacidade_Hash);
	  
	}//Fim metodo ItensHash
	
	public int maximo_itens (int tipo) {
	  
	  //Calcula tamanho de maximo da memoria em termos itens Hash.
		  
		  int capacidade_hash_max;
		  double memoria_livre = 0;
		  memoria_livre = Runtime.getRuntime().freeMemory();
		  
		  if (tipo == 0){
			  //Para HashMap
			  capacidade_hash_max = (int) ((memoria_livre)*(porcentagem_HashMap) / (64 + 30 + 4));  
		  }else{
			  //Para HashSet
			  capacidade_hash_max = (int) ((memoria_livre)*(porcentagem_HashMap) / (64 + 30));
		  }//fim if-else
		  
		  return(capacidade_hash_max);
		
	}//Fim metodo maximo_itens

	//Calcula porcentagem de memoria livre.
	public double porcentagem_livre(){
	  double p = 0;
	  p = Runtime.getRuntime().freeMemory() / Runtime.getRuntime().maxMemory();
	  //p = (Runtime.getRuntime().freeMemory()+(Runtime.getRuntime().maxMemory()-Runtime.getRuntime().totalMemory()))/ Runtime.getRuntime().maxMemory();
	  //p = Runtime.getRuntime().totalMemory() / Runtime.getRuntime().maxMemory();
	  return(p);
	}//Fim metodo porcentagem_livre.
	
	public void setup (Context setMapping) {
	   
	   //Aloca objeto HashMap que armazena distancias e vertices ja visitados.
	   InMapperCombiner = new HashMap<String, Integer>(this.itensHash(0));
	   Visitados = new HashSet<String>(this.itensHash(1));
	   
	   //Instancia variaveis usadas em map() e em cleanup().
	   ch = new StringBuilder(); 
	   ch_visitado = new StringBuilder(); 
	   destino_visitado = new StringBuilder();
	   chave_saida = new Text();
	   valor_saida = new Text();
	   
	}// Fim de setup
	
	public void map(Text chave, Text valor, Context Mapping) throws IOException, InterruptedException {
		
		String[] sub_linha_destinos, sub_linha_origens;
		
		//Uso de string para manipular linha limita uso do programa em grafos de aproximadamente 1 bilhao de vertices. 
		sub_linha_destinos = valor.toString().substring((valor.toString().indexOf('\t')+1), valor.toString().indexOf('x', valor.toString().indexOf('\t'))).split(";");
		sub_linha_origens = valor.toString().substring(valor.toString().indexOf('x',valor.toString().indexOf('\t')), valor.toString().length()).split("x");
		destino_visitado.append(chave.toString().substring((chave.toString().indexOf('#')) + 1, chave.toString().length()));
		int d = 0;
		double p = 0;

		Set<Map.Entry<String, Integer>> pares;
	    
	    //Cria mensagens com as distancias e vertices descobertos
		for (int i = 0; i <= (sub_linha_destinos.length)-1; i++){
			for (int j = 1; j <= (sub_linha_origens.length)-1; j++){
				
				if(i == 0){//verificacao das origens para o vertice id# da linha: precisa ser feita uma unica vez.	
				  if(sub_linha_origens[j].contains("d") == false){
					//if(this.itensHash(1) / this.maximo_itens(1) > 0.10){
				    p = this.porcentagem_livre();
				    if(p > 0.10){  
				      ch_visitado.append(destino_visitado);
				      ch_visitado.append('.');  
				      ch_visitado.append(Integer.toString(j));
				      if(Visitados.contains(ch_visitado) == false){ //testa se caminho ja foi registrado => linha repetida?
				        Visitados.add(ch_visitado.toString());
				      }//fim teste se ja foi registrado.
				      ch_visitado.setLength(0);
				    }//Fim se tem espaco para ser usado em HashSet visitados.
				  }//fim de teste se distancia ja foi calculada.
				}//Fim de verificacao das origens para o vertice id# da linha.
				
				//O asterisco indica que foi descoberto recentemente um valor de distancia.
				//Assim, a execucao entra na condicao abaixo se a origem j para o vertice 
				//#id da linha for um numero com asterisco. Ou seja, o vertice vizinhos i
				//do vertice %id da linha pode ter sua distancia atualizada no reducer.
				if (sub_linha_origens[j].contains("*")){
				   //Anota qual vertice #id da linha do arquivo esta sendo mapeado. Se a 
				   //execucao entrou aqui, significa que o vertice #Id (vertice da linha)
				   //tem a origem j com valor diferente de infinito (d) e que foi atualizado
				   //recentemente. Isso significa que nao ha necessidade de enviar mensagem
				   //para ele, portanto sera marcado como ja visitado. O ideal seria se 
				   //fosse possivel detectar todos os visitados do grafos, mas possuimos
				   //apenas a informacao dos vertices visitados internos a esse mapper.
				   	
				   
			       	   //Calcula a distancia na lista de vizinhanca do vertice #id da linha
				   //a partir da distancia recentemente descorberta para a origem j e
				   //prepara mensagem para ser enviada para o reducer.
				   
				   ch.append(sub_linha_destinos[i]);
				   ch.append('.');
				   ch.append(Integer.toString(j));
				   
				   /*
				    * 
				     O grafo tratado por esse programa deve ser nao-ponderado, ou seja, o peso de todas as arestas
				     sao iguais. Nessas condicoes, a primeira vez em que um vertice tem sua distancia calculada
				     para uma determinada origem, essa ja sera a minima. Alem disso, como a expansao ocorre por
				     fronteira, qualquer distancia calculada para uma mesma origem e destino terao sempre o mesmo
				     valor, mesmo para os diferentes caminhos descobertos. Com isso nao ha necessidade de fazer 
				     comparacoes de qual distancia apresenta-se menor para as chaves destino-origem que sao iguais. 
				     Portanto o InMapperCombiner tem a funcao de apenas evitar mensagens repetidas e evitar a criacao 
				     de objetos antes do tempo necessario. Consequentemente, os dados destino.origem/distancia sao
				     colodados diretamente no HashMap, dispensando a verificacao de sua minimalidade.
				   
				     Aqui tambem sera aplicada uma adaptacao da tecnica In-Mapper Combiner, chamada por
				     nos de In-Mapper Buffer Combiner. Basicamente a tecnica tem como objetivo liberar
				     o conteudo do HashMap para a fase shuffle quando a memoria ocupada no nodo computacional 
				     estiver com tamanho critico.
				   *
				   */
				   
				   //Usa memoria livre para InMapperCombiner. 
				   //if(this.itensHash(0) / this.maximo_itens(0) > 0.20){
				   p = this.porcentagem_livre();
				   if(p > 0.20){
                                     if(Visitados.contains(ch.toString()) == false){  
				       d = Integer.parseInt(sub_linha_origens[j].split("\\*")[0]);
				       InMapperCombiner.put(ch.toString(), (d + 1));
				     }//fim Se foi visitado     
				   }//fim Se ha espaco 
				   else //Diminui quantidades de itens no Hashmap, liberando-os para o shuffle
				      {
					   if (Visitados.contains(ch.toString()) == false){//Descarta vertices atualizados no job anterior
					      chave_saida.set(ch.toString());
					      d = Integer.parseInt(sub_linha_origens[j].split("\\*")[0]);
					      valor_saida.set(String.valueOf((d + 1)));
					      Mapping.write(chave_saida, valor_saida);
					      chave_saida.clear();
					      valor_saida.clear();
					   }// fim se visitados
					   if(!(InMapperCombiner.isEmpty())){//se hashmap nao esta vazio, esvazia
					     pares = InMapperCombiner.entrySet();
					     for(Map.Entry<String, Integer> elemento_hashmap : pares) {
						    if (Visitados.contains(elemento_hashmap.getKey()) == false){//Descarta vertices atualizados no job anterior
						      //Emite pares intermediarios com menor distancia encontrada no bloco.  
						      chave_saida.set(elemento_hashmap.getKey());
						      valor_saida.set(elemento_hashmap.getValue().toString());
						      Mapping.write(chave_saida, valor_saida);
						      chave_saida.clear();
						      valor_saida.clear();
						    }//Fim se visitados.
					     }//Fim de for para percorrer e esvaziar o InMapperCombiner.
					     pares.clear();
					     pares = null;
					     Runtime.getRuntime().gc();
					     InMapperCombiner.clear();
					   }//Fim se HashMap nao vazio.
				   }//Fim Senao libera/esvazia HashMap. 
				}//Fim Se distancia para origem j foi atualizada recentemente.
				ch.setLength(0);
			 }//fim for interno (para cada origem).
		     destino_visitado.setLength(0);
		}//fim for externo (para cada destino).
		//Dando uma dica para o GC ...
		pares = null; // => afeta o hashmap InMapperCombiner?
		sub_linha_destinos = null;
		sub_linha_origens = null;
	} //fim do metodo map()
	
	public void cleanup (Context closeMapping) throws IOException, InterruptedException {
	    
		if(!(InMapperCombiner.isEmpty())){//se hashmap nao esta vazio, esvazia
		  //Percorre o HashMap, emitindo pares <chave, valor> pre-selecionados
		  Set<Map.Entry<String, Integer>> pares = InMapperCombiner.entrySet();
		  
		  for(Map.Entry<String, Integer> par : pares){
	        if (Visitados.contains(par.getKey()) == false){//Descarta vertices atualizados no job anterior
	    	  //Emite pares intermediarios com menor distancia encontrada no bloco.
	    	  //System.out.println("Destino.Origem: " + par.getKey().toString() + " Distancia para origem: " + par.getValue().toString());  
	          chave_saida.set(par.getKey());
	          valor_saida.set(par.getValue().toString());
	    	  closeMapping.write(chave_saida, valor_saida);
	    	  chave_saida.clear();
	    	  valor_saida.clear();
	        }////Fim se HashMap nao vazio.  
	      }//Fim de for para percorrer e esvaziar o InMapperCombiner.
	      //Dando uma dica para o GC ...
	      pares.clear();
	      pares = null;
	      InMapperCombiner.clear();
	      InMapperCombiner = null;
	    }//Fim se hashmap nao vazio
		Visitados.clear();
	    Visitados = null;
	    
	    //Da uma dica para o GC sobre as variaveis usadas em map() e em cleanup().
	    ch = null;
		ch_visitado = null;
		destino_visitado = null;
		chave_saida = null;
		valor_saida = null;
		
	}// fim metodo cleanup()
		
}//fim classe MapperCalculaMenorCaminho
