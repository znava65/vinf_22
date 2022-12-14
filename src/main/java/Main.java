import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import java.io.*;
import java.util.*;

public class Main {
    public static HashMap<String, ArrayList<Integer>> index;
    public static final SparkConf config = new SparkConf()
            .setMaster("local[*]")
            .setAppName("AnimalParser");

    public static final JavaSparkContext jsc = new JavaSparkContext(config);
    public static final SQLContext sqlc = new SQLContext(jsc);

    /**
     * With argument "p", it processes the data.
     * With argument "s", it provides the search functionality.
     * @param args
     */
    public static void main(String[] args) {
        if (args.length > 0) {
            if (args[0].equals("p"))
                processData();
            else if (args[0].equals("s"))
                search();
            else System.err.println("Unknown argument: " + args[0]);
        }
        else System.err.println("Please specify the action: 'p' for processing or 's' for searching");
    }

    /**
     * Distributed computing on the whole english Wikipedia.
     */
    public static void processData() {
        Preprocessor preprocessor = new Preprocessor("data/enwiki-latest-pages-articles.xml");
        try {
            JavaRDD<Animal> animals = preprocessor.parsePages();
            List<Animal> animalsList = animals.collect();
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("animals.bin"));
            oos.writeObject(animalsList);
            oos.close();
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }

        jsc.close();
    }

    /**
     * Handles the searching part of the program. Asks the user whether to search a single animal
     * or find if two animals can meet each other.
     */
    public static void search() {
        try {
            System.out.println("Reading data...");
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream("animals.bin"));
            List<Animal> animals = (List<Animal>) ois.readObject();
            ois.close();
            System.out.println("Number of items: " + animals.size());
            System.out.println("Indexing...");
            index = new HashMap<>();
            for (int i=0; i < animals.size(); i++) {
                String[] tokens = animals.get(i).getTitle().split("\\s");
                for (String token : tokens) {
                    if (!index.containsKey(token)) index.put(token, new ArrayList<>());
                    index.get(token).add(i);
                }
            }

            Search search = new Search(index, animals);
            Scanner scanner = new Scanner(System.in);
            while (true) {
                System.out.println("Type 's' for single search, 'm' to see if two animals can meet each other or 'q' to quit:");
                String choice = scanner.nextLine();
                if (choice.equalsIgnoreCase("s")) {
                    System.out.println("Search:");
                    search.handleSearch(scanner.nextLine());
                }
                else if (choice.equalsIgnoreCase("m")) {
                    System.out.println("Search the first animal:");
                    Animal a1 = search.pickAnimal(scanner.nextLine());
                    if (a1 == null) {
                        System.out.println("No animal chosen");
                        continue;
                    }
                    System.out.println("Search the second animal:");
                    Animal a2 = search.pickAnimal(scanner.nextLine());
                    if (a2 == null) {
                        System.out.println("No animal chosen");
                        continue;
                    }
                    search.canTheyMeet(a1, a2);
                }
                else if (choice.equalsIgnoreCase("q")) {
                    return;
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
