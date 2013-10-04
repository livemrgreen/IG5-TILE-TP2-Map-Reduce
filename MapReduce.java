import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.Mongo;

/**
 * @author maroua
 *
 */

public class MapReduce {
	public static void main(String[] args) throws ParseException {
		StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_40);
		Directory index = new RAMDirectory();
		IndexWriter w;
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_40, analyzer);

		String cheminfichier = null; // La chaine de caracteere qui contient le chemin vers le fichier texte qu'on veut lire
		String lignecourante = null; //La ligne courante du
		fichier
		try {
			IndexWriter indexWriter = new IndexWriter(index, config);
			cheminfichier ="C:\\Users\\maroua\\Desktop\\movies.dat"; BufferedReader bufferedReader = new BufferedReader(new
			FileReader(cheminfichier));

			// Tout d'abord, il faut creeer un mongo serveur, et Java joue
			// le rôle d'un mongo client
			// Se connecter à un serveur local Mongo
			// Cette commande permet de se connecter au serveur mongo, et vu que le
			// serveur est en local, l'adresse hôte est 127.0.0.1
			Mongo mongo = new Mongo("127.0.0.1"); //On crée une base de données
								 DB db = mongo.getDB("database_film");
								 //Creeer une collection de films
			DBCollection filmCollection = db.getCollection("Film"); /****Lecture du fichier et enregistrement des documents dans la base Mongo****/
			while ((lignecourante = bufferedReader.readLine()) != null) {
				String splittedLine[] = lignecourante.split("::");
									//Un document est représenté par la classe DBObject
				DBObject document = new BasicDBObject();
				//Il faut stocker le genre des films dans un arrayLise, on va faire un
				￼split sur le pipe
				String genre[] = splittedLine[2].split("\\|");
				document.put("FilmID",splittedLine[0]);
				document.put("FilmTitre",splittedLine[1]);
				document.put("Filmgenre",genre);
					//Insérer le document créer dans la collection
													 filmCollection.insert(document);
			}

			
			/***********************************************************************/
			/**************************** Map-Reduce *******************************/

			// Dans les string map et reduce, il y a du code JavaScript qui sera
			// interprete par MapReduceCommand

			String map ="function () {"+
					"for(var i=0;i<this.Filmgenre.length;i++){ "+
																	"emit(this.Filmgenre[i],{count: 1});"+
													 "}";
			String reduce = "function (key, values) { "+
																		 " total = 0; "+
			" for (var i in values) { "+
			" total += values[i].count; "+
																		 " } "+
																		 " return {count:total} }";
			MapReduceCommand cmd = new MapReduceCommand(filmCollection, map, reduce, null, 
				MapReduceCommand.OutputType.INLINE, null);
			MapReduceOutput out = filmCollection.mapReduce(cmd);
			for (DBObject o : out.results()) {
				System.out.println(o.toString());
			}
			System.out.println("Done");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}