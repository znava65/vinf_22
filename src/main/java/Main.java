import org.apache.orc.impl.ConvertTreeReaderFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.io.*;
import java.lang.reflect.Array;
import java.sql.SQLOutput;
import java.util.*;

public class Main {
    public static HashMap<String, ArrayList<Integer>> index;
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
            for (int i=0; i < animals.size(); i++) {
                String[] tokens = animals.get(i).getTitle().split("\\s");
                for (String token : tokens) {
                    if (!index.containsKey(token)) index.put(token, new ArrayList<>());
                    index.get(token).add(i);
                }
            }
//            for (Integer i : index.get("giraffe")) {
//                Animal a = animals.get(i);
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
                    handleSearch(scanner.nextLine(), animals);
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

    public static void handleSearch(String searchTerm, List<Animal> animals) {
        String[] tokens = searchTerm.split("\\s");
        if (tokens.length == 1) {
            for (Integer i : index.get(tokens[0].toLowerCase())) {
                Animal a = animals.get(i);
                System.out.println("Title: " + a.getTitle());
                System.out.println("Locations: " + a.getLocations().toString());
                System.out.println("Habitats: " + a.getHabitats().toString());
                System.out.println("Activity: " + a.getActivityTime().toString());
                System.out.println();
            }
        }
        else {

        }
    }

    public static ArrayList<Integer> intersect(ArrayList<Integer> p1, ArrayList<Integer> p2) {
        Iterator<Integer> i_p1 = p1.iterator();
        Iterator<Integer> i_p2 = p2.iterator();
        Integer cur_p1 = i_p1.next();
        Integer cur_p2 = i_p2.next();
        ArrayList<Integer> result = new ArrayList<>();

        while (cur_p1 != null && cur_p2 != null) {
            if (cur_p1.equals(cur_p2)) {
                result.add(cur_p1);
                cur_p1 = i_p1.next();
                cur_p2 = i_p2.next();
            }
            else if (cur_p1 < cur_p2) {
                cur_p1 = i_p1.next();
            }
            else cur_p2 = i_p2.next();
        }

        return result;
    }

    public static ArrayList<Integer> intersect(ArrayList<ArrayList<Integer>> postingLists) {
        postingLists.sort((a1, a2) -> a2.size() - a1.size());
        ArrayList<Integer> result = new ArrayList<>(postingLists.get(0));
        ArrayList<ArrayList<Integer>> terms = new ArrayList<>(postingLists.subList(1, postingLists.size()));

        while (terms.size() != 0 && !result.isEmpty()) {
            result = intersect(result, new ArrayList<>(postingLists.get(0)));
            terms = new ArrayList<>(postingLists.subList(1, postingLists.size()));
        }

        return result;
    }
}
