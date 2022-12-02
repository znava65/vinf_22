import org.apache.commons.lang.StringUtils;
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
            e.printStackTrace();
        }
    }

    public static void handleSearch(String searchTerm, List<Animal> animals) {
        String[] tokens = searchTerm.split("\\s");
        int order = 1;
        Animal a;
        ArrayList<Integer> result;

        if (tokens.length == 0) return;

        if (tokens.length == 1) {
            result = index.get(tokens[0].toLowerCase());
        }
        else if (tokens.length == 2){
            result = intersect(index.get(tokens[0].toLowerCase()), index.get(tokens[1].toLowerCase()));
        }
        else {
            ArrayList<ArrayList<Integer>> postingLists = new ArrayList<>();
            for (String token : tokens) {
                postingLists.add(index.get(token.toLowerCase()));
            }
            result = intersect(postingLists);
        }
        for (Integer i : result) {
            a = animals.get(i);
            printAnimal(a, order);
            order++;
        }
    }

    public static ArrayList<Integer> intersect(ArrayList<Integer> p1, ArrayList<Integer> p2) {
        ArrayList<Integer> result = new ArrayList<>();

        if (p1 == null || p2 == null) return result;

        Iterator<Integer> i_p1 = p1.iterator();
        Iterator<Integer> i_p2 = p2.iterator();
        Integer cur_p1 = i_p1.hasNext() ? i_p1.next() : null;
        Integer cur_p2 = i_p2.hasNext() ? i_p2.next() : null;

        while (cur_p1 != null && cur_p2 != null) {
            if (cur_p1.equals(cur_p2)) {
                result.add(cur_p1);
                cur_p1 = i_p1.hasNext() ? i_p1.next() : null;
                cur_p2 = i_p2.hasNext() ? i_p2.next() : null;
            }
            else if (cur_p1 < cur_p2) {
                cur_p1 = i_p1.hasNext() ? i_p1.next() : null;
            }
            else cur_p2 = i_p2.hasNext() ? i_p2.next() : null;
        }

        return result;
    }

    public static ArrayList<Integer> intersect(ArrayList<ArrayList<Integer>> postingLists) {
        for (ArrayList<Integer> l : postingLists) {
            if (l == null) return new ArrayList<>();
        }

        postingLists.sort((a1, a2) -> a2.size() - a1.size());
        ArrayList<Integer> result = new ArrayList<>(postingLists.get(0));
        ArrayList<ArrayList<Integer>> terms = new ArrayList<>(postingLists.subList(1, postingLists.size()));

        while (terms.size() != 0 && !result.isEmpty()) {
            result = intersect(result, new ArrayList<>(terms.get(0)));
            terms = new ArrayList<>(terms.subList(1, terms.size()));
        }

        return result;
    }

    public static void printAnimal(Animal a, int order) {
        System.out.print(order);
        System.out.println(". " + StringUtils.capitalize(a.getTitle()));

        System.out.print("Locations:");
        if (!a.getLocations().isEmpty()) {
            for (String l : a.getLocations().subList(0, a.getLocations().size()-1)) {
                System.out.print(" " + StringUtils.capitalize(l) + ",");
            }
            System.out.println(" " + StringUtils.capitalize(a.getLocations().get(a.getLocations().size()-1)));
        }

        System.out.print("Habitats:");
        if (!a.getHabitats().isEmpty()) {
            for (String h : a.getHabitats().subList(0, a.getHabitats().size()-1)) {
                System.out.print(" " + h + ",");
            }
            System.out.println(" " + a.getHabitats().get(a.getHabitats().size()-1));
        }

        System.out.print("Activity time:");
        for (String t : a.getActivityTime().subList(0, a.getActivityTime().size()-1)) {
            System.out.print(" " + t + ",");
        }
        System.out.println(" " + a.getActivityTime().get(a.getActivityTime().size()-1));
        System.out.println();
    }
}
