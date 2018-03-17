import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.nio.file.Paths;

import static java.util.stream.Collectors.counting;


public class wordcount
{
    public static void main ( String args[] )
    {
        String fileName = "/home/biny/IdeaProjects/assignment1/WikipediaPages_oneDocPerLine_1000Lines_small.txt" ;

        try( Stream<String> filestream = Files.lines(Paths.get(fileName)) )
        {
            //
            // From the example provided here
            // https://github.com/kiat/metcs755/blob/master/Assignment-1-Template/src/main/java/edu/bu/cs755/Main.java
            //

            // From notes on the subject
            Map<String, Long> dict = filestream.flatMap(line -> Arrays.stream(line.trim().split("\\s+")))
                   .map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase().trim() )
                   .map(word -> word.trim().toUpperCase())
                   .filter(word -> word.length() > 0)
                   .map(word -> new AbstractMap.SimpleEntry< >(word,1))
                   .collect(Collectors.groupingBy(AbstractMap.SimpleEntry::getKey,counting()));

           // Do not use parallelStream because the word count isn't accurate
            Map<String,Long> result = dict.entrySet().stream().sorted( Map.Entry.comparingByValue(Comparator.reverseOrder()))
                   .limit(5000)
                   .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue() , (oldValue, newValue) -> oldValue, LinkedHashMap::new) ) ;

            List<String> wordIndexes = new ArrayList<String>(result.keySet()) ;

            System.out.println( "Frequency position for 'during': " + wordIndexes.indexOf("DURING"));
            System.out.println( "Frequency position for 'and': " + wordIndexes.indexOf("AND"));
            System.out.println( "Frequency position for 'time': " + wordIndexes.indexOf("TIME"));
            System.out.println( "Frequency position for 'protein': " + wordIndexes.indexOf("PROTEIN"));
            System.out.println( "Frequency position for 'car': " + wordIndexes.indexOf("CAR"));
        }
        catch (IOException e )
        {
            e.printStackTrace();
        }
    }
}
