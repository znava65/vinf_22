import org.apache.orc.impl.ConvertTreeReaderFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static HashMap<String, ArrayList<Animal>> index;
    public static final SparkConf config = new SparkConf()
            .setMaster("local[*]")
            .setAppName("SoccerParser");

    public static final JavaSparkContext jsc = new JavaSparkContext(config);
    public static final SQLContext sqlc = new SQLContext(jsc);
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

    public static void processData() {
        Preprocessor preprocessor = new Preprocessor("data/enwiki-latest-pages-articles.xml");
        try {
            JavaRDD<Animal> animals = preprocessor.parsePages();
            List<Animal> animalsList = animals.collect();
            System.out.println(animalsList.get(0));
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("animals.bin"));
            oos.writeObject(animalsList);
            oos.close();
//            animals.foreach(a -> {
//                System.out.println(a.getTitle());
//                String[] tokens = a.getTitle().split("\\s");
//                for (String token : tokens) {
//                    if (!index.containsKey(token)) index.put(token, new ArrayList<>());
//                    index.get(token).add(a);
//                }
//            });

//            System.out.println(index.get("bald").get(0).getTitle());
//            System.out.println(index.get("bald").get(0).getTitle());

        }
        catch (Exception e) {
            System.out.println(e.toString());
        }

        jsc.close();
    }

    public static void search() {
        try {
            System.out.println("Reading data...");
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream("animals.bin"));
            List<Animal> animals = (List<Animal>) ois.readObject();
            ois.close();
            System.out.println("Number of items: " + animals.size());
            System.out.println("Indexing...");
            index = new HashMap<>();
            for (Animal animal : animals) {
                String[] tokens = animal.getTitle().split("\\s");
                for (String token : tokens) {
                    if (!index.containsKey(token)) index.put(token, new ArrayList<>());
                    index.get(token).add(animal);
                }
            }
//            System.out.println("Arctic:");
//            for (Animal a : index.get("arctic")) {
//                System.out.println(a.getTitle());
//                System.out.println(a.getLocations().toString());
//                System.out.println(a.getHabitats().toString());
//            }
            Scanner scanner = new Scanner(System.in);
            while (true) {
                System.out.println("Type 's' for single search, 'm' to see if two animals can meet each other or 'q' to quit:");
                String choice = scanner.nextLine();
                if (choice.equalsIgnoreCase("s")) {
                    System.out.println("Search:");
                    handleSearch(scanner.nextLine());
                }
                else if (choice.equalsIgnoreCase("m")) {
                    System.out.println("Search the first animal:");
                }
                else if (choice.equalsIgnoreCase("q")) {
                    return;
                }
            }
        }
        catch (Exception e) {
            System.err.println("File not found");
        }
    }

    public static void handleSearch(String searchTerm) {

    }

    public
}
