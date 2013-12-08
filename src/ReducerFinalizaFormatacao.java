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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerFinalizaFormatacao extends Reducer<LongWritable, Text, Text, Text> {
	
	StringBuilder BuilderListaDistancias = new StringBuilder();
	final static String separador = "xd";// implica em dois char por vertice

	public void setup(Context setReducing) {
	
	    //Constroi lista de distancias
		long numero_Vertices;
		numero_Vertices = setReducing.getConfiguration().getLong("PARAMETRO_numero_vertices", 0);
		//Tentando ganhar performance ao alocar buffer para cria lista de distancias
		//Situacao com bom desempenho:
		if ((2*numero_Vertices) <= Integer.MAX_VALUE) {
			//StringBuilder para anexar lista de distancias
			BuilderListaDistancias.ensureCapacity((int) (2*numero_Vertices));
			for (long i=1; i <= numero_Vertices; i++){
			  BuilderListaDistancias.append(separador);
			}//fim loop for	
		}// fim se
		BuilderListaDistancias.append('x');
	
	}// fim setup
	
	//Saida do reduce: #verticeID+TAB+Vizinho1;Vizinho2; ... ;Vizinho_n|-|-| ... |-|
	public void reduce(LongWritable chave, Iterable<Text> valores, Context Reducing) throws IOException, InterruptedException {
		
		long numero_linha = chave.get();
		long numero_Vertices = Reducing.getConfiguration().getLong("PARAMETRO_numero_vertices", 0);
		String lista_Vizinhos, verticeID, linha_Arquivo;
		StringBuilder listaDistancias = new StringBuilder(BuilderListaDistancias.toString());		
		
		//Inicia lista de distancia com valor 0
		//Situacao com bom desempenho:
		if ((2*numero_Vertices + 1) <= Integer.MAX_VALUE){//cada linha tem pelo menos um asterisco => + 1 
		  listaDistancias.setCharAt( (int) (2*numero_linha - 1), '0');// substitui d por 0
		  listaDistancias.insert(2*(int)numero_linha, '*'); //coloca marcador de recem descoberto
		} else {//Situacao com desempenho ruim
			//StringBuilder para anexar lista de distancias de tamanho Long
			BuilderListaDistancias.ensureCapacity(Integer.MAX_VALUE);//ensureCapacity => tamanho minimo, logo MAX_VALUE sem problemas.
			for (long i=1; i<= numero_Vertices; i++){
				if(i == numero_linha){
					BuilderListaDistancias.append(separador.substring(1));
					BuilderListaDistancias.append("0*");
				}// fim se
				BuilderListaDistancias.append(separador);
			}//fim loop for
			BuilderListaDistancias.append('x');
		} // fim senao externo */
		
		for (Text valor : valores){//caso a funcao de particao gere mais elementos com mesma chave
			lista_Vizinhos = valor.toString().substring(valor.toString().indexOf("\t",valor.toString().indexOf('#')) + 1, valor.toString().length()); //seleciona um elemento da lista de valores
			verticeID = valor.toString().substring(valor.toString().indexOf('#'), valor.toString().indexOf("\t", valor.toString().indexOf('#')));
			linha_Arquivo = new String(lista_Vizinhos + listaDistancias); 
			Reducing.write(new Text(verticeID), new Text(linha_Arquivo));
		}//fim do laco for
		
		listaDistancias.setLength(0);
		listaDistancias = null; 
	} //fim metodo reduce()

}//fim classe ReducerFinalizaFormatacao
