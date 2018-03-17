import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.counting;

public class pagerank
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

            try( Stream<String> filestream2 = Files.lines(Paths.get(fileName)) )
            {
                System.out.println("Reading from the file again!");
                List<String> pages = filestream2.flatMap(line -> Arrays.stream(line.trim().split("doc id=")))
                        .map(word -> word.replaceAll( "[^a-zA-Z0-9]", " ").trim())
                        .filter(word -> word.length() > 0)
                        .limit(10)
                        .collect(Collectors.toList());

                // pages.forEach(System.out::println);

                //.map(word -> new AbstractMap.SimpleEntry< >(word,1));
                //.collect(Collectors.groupingBy(AbstractMap.SimpleEntry::getKey,1));
                //.filter(word -> word.equalsIgnoreCase("DOC id "))
                    //.map( w -> System.out::println)
            }
            catch (IOException e )
            {
                e.printStackTrace();
            }
        }
        catch (IOException e )
        {
            e.printStackTrace();
        }
    }
}
