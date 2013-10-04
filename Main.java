package com.company;

import com.mongodb.*;

import java.io.BufferedReader;
import java.io.FileReader;


public class Main {

    public static void main(String[] args) throws Exception {

        String inputFilePath = null;
        String currentLine = null;
        String currentTitle = null;
        String currentMovieID = null;
        inputFilePath = "/Users/yannick/Dropbox/Polytech/IG5/TILE/TP_IG5/movies.dat";
        BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFilePath));

        Mongo mongo = new Mongo("127.0.0.1");
        DB db = mongo.getDB("filmDB");
        DBCollection filmCollection = db.getCollection("film");

        while ((currentLine = bufferedReader.readLine()) != null) {
            String[] splittedLine = currentLine.split("::");
            currentMovieID = splittedLine[0];
            currentTitle = splittedLine[1];

            String[] currentGenres = splittedLine[2].split("\\|");

            DBObject currentDocument = new BasicDBObject();

            currentDocument.put("movieID", currentMovieID);
            currentDocument.put("title", currentTitle);
            currentDocument.put("genre", currentGenres);

            filmCollection.insert(currentDocument);
        }

        String map = "function() {"
                        + "for (var i = 0; i < this.genre.length; i++) {"
                            + "emit(this.genre[i], {count: 1});"
                        + '}'
                    + '}';

        String reduce = "function (key, values) {"
                            + "total = 0; "
                            + "for (var i in values) {"
                                + "total += values[i].count;"
                            + '}'
                            + "return {count: total}"
                        + '}';

        MapReduceCommand mapReduceCommand = new MapReduceCommand(filmCollection, map, reduce, null,
                                                                    MapReduceCommand.OutputType.INLINE, null);

        MapReduceOutput mapReduceOutput = filmCollection.mapReduce(mapReduceCommand);
        for (DBObject dbObject : mapReduceOutput.results()) {
            System.out.println(dbObject);
        }
        System.out.println("Done");
    }
}
