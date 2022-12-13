import org.apache.commons.lang.StringUtils;
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
                    Animal a1 = pickAnimal(scanner.nextLine(), animals);
                    if (a1 == null) {
                        System.out.println("No animal chosen");
                        continue;
                    }
                    System.out.println("Search the second animal:");
                    Animal a2 = pickAnimal(scanner.nextLine(), animals);
                    if (a2 == null) {
                        System.out.println("No animal chosen");
                        continue;
                    }
                    canTheyMeet(a1, a2);
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

    /**
     * Gets and prints the results of the search of a single animal.
     * @param searchTerm The user's input
     * @param animals List of all animals
     */
    public static void handleSearch(String searchTerm, List<Animal> animals) {
        int order = 1;
        Animal a;
        ArrayList<Integer> result = getResult(searchTerm);

        Scanner scanner = new Scanner(System.in);
        String choice;

        if (result == null || result.isEmpty()) {
            System.out.println("No results found");
            return;
        }

        for (Integer i : result) {
            a = animals.get(i);
            printAnimal(a, order);

            if (order % 5 == 0 && result.size() > order) {
                System.out.println(order + "/" + result.size() + " results fetched");
                System.out.println("Type 'n' for the next page or any other key for closing the results:");
                choice = scanner.nextLine();
                if (!choice.equalsIgnoreCase("n")) break;
            }
            order++;
        }

        if (order-1 == result.size()) {
            System.out.println(order-1 + "/" + result.size() + " results fetched");
        }
        System.out.println();
    }

    /**
     * Similar to handleSearch(), searches the animal based on the user's input.
     * This method is used to pick one of two animals for which we want to find out if they can meet.
     * @param searchTerm The user's input
     * @param animals List of all animals
     * @return The chosen animal
     */
    public static Animal pickAnimal(String searchTerm, List<Animal> animals) {
        int order = 1;
        Animal a;
        ArrayList<Integer> result = getResult(searchTerm);

        Scanner scanner = new Scanner(System.in);
        String inp;

        if (result == null || result.isEmpty()) return null;

        for (Integer i : result) {
            a = animals.get(i);
            printAnimal(a, order);

            if (order % 5 == 0 && result.size() > order) {
                System.out.println(order + "/" + result.size() + " results fetched");
                System.out.println("Type 'n' for the next page, number of the animal you want to pick, or any other key for closing the results:");
                inp = scanner.nextLine();
                if (inp.equalsIgnoreCase("n")) {
                    order++;
                    continue;
                }
                else {
                    try {
                        int pick = Integer.parseInt(inp)-1;
                        return animals.get(result.get(pick));
                    }
                    catch (Exception e) {
                        return null;
                    }
                }

            }
            order++;
        }

        if (order-1 == result.size()) {
            System.out.println(order-1 + "/" + result.size() + " results fetched");
        }
        System.out.println();
        System.out.println("Type number of the animal you want to pick, or any other key for closing the results:");
        inp = scanner.nextLine();
        try {
            int pick = Integer.parseInt(inp)-1;
            return animals.get(result.get(pick));
        }
        catch (Exception e) {
            return null;
        }
    }

    /**
     * Finds out if the two specified animals can meet each other.
     * The mechanism is based of common locations and common habitats of the two animals.
     * If there are common both locations and habitats, there is high probability that they can meet.
     * If there are only common locations, the probability is still quite solid.
     * If there are only common habitats, the probability is low.
     * Otherwise, they cannot meet each other.
     * @param a1 The first animal
     * @param a2 The second animal
     */
    public static void canTheyMeet(Animal a1, Animal a2) {
        printAnimal(a1, 1);
        printAnimal(a2, 2);

        ArrayList<String> commonLocations = new ArrayList<>(a1.getLocations());
        commonLocations.retainAll(a2.getLocations());

        ArrayList<String> commonHabitats = new ArrayList<>(a1.getHabitats());
        commonHabitats.retainAll(a2.getHabitats());

        System.out.print("Common locations:");
        if (!commonLocations.isEmpty()) {
            for (String l : commonLocations.subList(0, commonLocations.size()-1)) {
                System.out.print(" " + StringUtils.capitalize(l) + ",");
            }
            System.out.println(" " + StringUtils.capitalize(commonLocations.get(commonLocations.size()-1)));
        }
        else System.out.println(" none");

        System.out.print("Common habitats:");
        if (!commonHabitats.isEmpty()) {
            for (String h : commonHabitats.subList(0, commonHabitats.size()-1)) {
                System.out.print(" " + h + ",");
            }
            System.out.println(" " + commonHabitats.get(commonHabitats.size()-1));
        }
        else System.out.println(" none");

        if (!commonLocations.isEmpty() && !commonHabitats.isEmpty()) {
            System.out.println(StringUtils.capitalize(a1.getTitle()) + " and " + a2.getTitle() + " can meet each other because they have common locations and habitats.\n");
        }
        else if (!commonLocations.isEmpty()) {
            System.out.println(StringUtils.capitalize(a1.getTitle()) + " and " + a2.getTitle() + " can potentially meet each other, but they do not have common habitats.\n");
        }
        else if (!commonHabitats.isEmpty()) {
            System.out.println("There is little probability that " + a1.getTitle() + " and " + a2.getTitle() + " will meet each other, but they do have common habitats.\n");
        }
        else {
            System.out.println(StringUtils.capitalize(a1.getTitle()) + " and " + a2.getTitle() + " cannot meet each other.\n");
        }
        System.out.println();
    }

    /**
     * Based on the created index, the method gets indices of all the pages relevant to the search term.
     * @param searchTerm The user's input
     * @return List of indices of relevant pages
     */
    public static ArrayList<Integer> getResult(String searchTerm) {
        String[] tokens = searchTerm.split("\\s");
        ArrayList<Integer> result;

        if (tokens.length == 0) return null;

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

        return result;
    }

    /**
     * Finds the intersection of two specified posting lists.
     * @param p1 The first posting list
     * @param p2 The second posting list
     * @return Intersection of the two specified posting lists
     */
    public static ArrayList<Integer> intersect(ArrayList<Integer> p1, ArrayList<Integer> p2) {
        if (p1 == null || p2 == null) return null;

        ArrayList<Integer> result = new ArrayList<>();
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

    /**
     * Finds the intersection of multiple specified posting lists.
     * @param postingLists List of postings lists
     * @return Intersection of the specified posting lists
     */
    public static ArrayList<Integer> intersect(ArrayList<ArrayList<Integer>> postingLists) {
        if (postingLists == null || postingLists.isEmpty()) return null;
        postingLists.sort((l1, l2) -> {
            if (l1 == null && l2 == null) return 0;
            else if (l1 == null) return -1;
            else if (l2 == null) return 1;
            else return l1.size() - l2.size();
        });
        ArrayList<Integer> result = postingLists.get(0);
        ArrayList<ArrayList<Integer>> terms = new ArrayList<>(postingLists.subList(1, postingLists.size()));

        while (terms.size() != 0 && result != null && !result.isEmpty()) {
            result = intersect(result, new ArrayList<>(terms.get(0)));
            terms = new ArrayList<>(terms.subList(1, terms.size()));
        }

        return result;
    }

    /**
     * Prints the information about the specified animal.
     * @param a The animal whose information are about to be printed
     * @param order Order of the animal in the list of search results
     */
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
        else System.out.println();

        System.out.print("Habitats:");
        if (!a.getHabitats().isEmpty()) {
            for (String h : a.getHabitats().subList(0, a.getHabitats().size()-1)) {
                System.out.print(" " + h + ",");
            }
            System.out.println(" " + a.getHabitats().get(a.getHabitats().size()-1));
        }
        else System.out.println();

        System.out.print("Activity time:");
        for (String t : a.getActivityTime().subList(0, a.getActivityTime().size()-1)) {
            System.out.print(" " + t + ",");
        }
        System.out.println(" " + a.getActivityTime().get(a.getActivityTime().size()-1));
        System.out.println();
    }
}
