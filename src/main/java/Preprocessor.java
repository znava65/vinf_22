import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.codehaus.janino.Java;

public class Preprocessor implements Serializable {

//    private final Pattern IS_ANIMAL_CATEGORY_PATTERN = Pattern.compile("\\[\\[Category:.*\\b(mammals|vertebrates|invertebrates|reptiles|amphibians|insects)\\b.*?]]", Pattern.CASE_INSENSITIVE);
    private final Pattern IS_ANIMAL_PATTERN = Pattern.compile("\\b\\[{0,2}(animalia|inhabits?\\b|carnivor|herbivor|omnivor|live|behaviou?r|chordata|vertebrate|herd|mammal|fish|bird|swim|run\\b|fly|hunt|move)|={1,3}[^\\n]*?habitat", Pattern.CASE_INSENSITIVE);
    private final Pattern IS_EXTINCT_PATTERN = Pattern.compile("extinct|saurs?", Pattern.CASE_INSENSITIVE);
    private final Pattern LOCATION_SENTENCE_PATTERN = Pattern.compile("(\\.\\s)?\\n?[A-Z][^.={};]*?(\\bdistrib|present\\s(in|on)|found\\s(on|in|through|from)|occurs?\\s(on|in|through|from|off)|\\blocat|\\bhabit|\\blives?\\s|native)[^.={};]*\\b[A-Z][^.={};]*\\.?\\n?");
    private final Pattern LOCATION_INFO_PATTERN = Pattern.compile("(\\bdistrib|present\\s(in|on)|found\\s(in|on)|\\blocat|\\bhabit|\\blives?\\s)[^.]*\\b[A-Z][^.]*\\.?\\n?");
    private final Pattern LOCATION_PATTERN = Pattern.compile("([^.,\\s]*?\\s){0,3}\\[{0,2}[A-Z](\\p{L}|'|-)*\\b(\\s|]|,|\\.)([A-Z](\\p{L}|'|-)*\\s?)*");
    private final String[] acceptedLocations = {
      "america",
      "europe",
      "asia",
      "eurasia",
      "antarctica",
      "arctic",
      "africa",
      "australia",
      "madagascar",
      "new zealand",
      "new guinea",
      "china",
      "iceland",
      "japan",
      "pacific",
      "atlantic",
      "middle east",
      "tasmania",
      "mediterranean",
    };
    private final String[] acceptedSubLocations = {
            "west",
            "east",
            "north",
            "south",
            "central",
            "sub-saharan"
    };

    private final Pattern ACCEPTED_HABITATS_PATTERN = Pattern.compile("\\b\\[{0,2}(savannah?|woodland|shrubland|grassland|bushland|desert|tundra|seas?(\\s|])|river|lake|marsh|forest|rainforest|mountain|ground)", Pattern.CASE_INSENSITIVE);

    private String path;

    private final JavaRDD<Row> rdd;
    public Preprocessor(String path) {
        this.path = path;
        this.rdd = Main.sqlc.read().format("com.databricks.spark.xml").option("rowTag", "page").load(path).toJavaRDD();
    }

    public JavaRDD<Animal> parsePages() {
        return this.rdd.map(page -> {
            String title = page.getAs("title");
            String content = clearContent(page.toString());
            if (isAnimal(content)) {
                Animal animal = new Animal(title.toLowerCase(), content);
                animal.setLocations(findLocations(animal));
                animal.setHabitats(findHabitats(animal));
                animal.setActivityTime(findActivityTime(animal));
                System.out.println(animal.getTitle());

                return animal;
            }
            return null;
        }).filter(Objects::nonNull);
    }

    private String clearContent(String content) {
        content = Pattern.compile("<page>.*?<text.*?>", Pattern.DOTALL).matcher(content).replaceAll("");
        content = Pattern.compile("</text.*?</page>.*?", Pattern.DOTALL).matcher(content).replaceAll("");
        content = content.replaceAll("&lt;ref.*?/ref&gt;", "");
        content = content.replaceAll("&lt;ref.*?/&gt;", "");
        content = content.replaceAll("\\[\\[File:.*?\\n", "");

        return content;
    }

    private boolean isAnimal(String content) {
        String line;
        ArrayList<String> matches = new ArrayList<>();
        String group;
        short extinctCounter = 0;

        if (!Pattern.compile("wikispecies", Pattern.CASE_INSENSITIVE).matcher(content).find())
            return false;

        BufferedReader br = new BufferedReader(new StringReader(content));
        try {
            while ((line = br.readLine()) != null) {
                Matcher matcher = IS_ANIMAL_PATTERN.matcher(line);
                Matcher isExtinctMatcher = IS_EXTINCT_PATTERN.matcher(line);
                if (matcher.find() && !matches.contains((group = matcher.group().toLowerCase()))) {
                    matches.add(group);
                }
                if (isExtinctMatcher.find()) {
                    extinctCounter++;
                }
            }
        }
        catch (Exception e) {
            System.err.println("Error reading content");
        }

        if (extinctCounter >= 15) {
            return false;
        }

        Matcher firstParagraphMatcher = Pattern.compile("\\n[^'\\n]*?'{3,5}[^']+'{3,5}.*?(is|are).*?\\.").matcher(content);
        if (firstParagraphMatcher.find() && Pattern.compile("extinct|early").matcher(firstParagraphMatcher.group()).find() && !Pattern.compile("extant").matcher(firstParagraphMatcher.group()).find()) {
            return false;
        }

        return matches.size() >= 2;
    }

    private ArrayList<String> findLocations(Animal animal) {
        ArrayList<String> locations = new ArrayList<>();
        String content = animal.getContent();
        Matcher firstParagraphMatcher = Pattern.compile("\\n[^'\\n]*?'{3,5}[^']+'{3,5}.*?(is|are).*?(\\n|except|\\bnot\\b)").matcher(content);
        Matcher locationMatcher;
        Matcher subLocationMatcher;
        boolean subLocationFlag = false;
        String foundLocation;

        if (firstParagraphMatcher.find()) {
            for (String acceptedLocation : acceptedLocations) {
                locationMatcher = Pattern.compile("([^.,\\s]*?\\s){0,3}\\b\\[{0,2}" + acceptedLocation + "s?n?]{0,2}\\b", Pattern.CASE_INSENSITIVE).matcher(firstParagraphMatcher.group());
                while (locationMatcher.find()) {
                    for (String acceptedSubLocation : acceptedSubLocations) {
                        subLocationMatcher = Pattern.compile(acceptedSubLocation, Pattern.CASE_INSENSITIVE).matcher(locationMatcher.group());
                        if (subLocationMatcher.find()) {
                            subLocationFlag = true;
                            foundLocation = (subLocationMatcher.group() + " " + acceptedLocation).toLowerCase();
                            if (!locations.contains(foundLocation)) {
                                locations.add(foundLocation);
                            }
                        }
                    }
                    if (!subLocationFlag) {
                        resolveWholeLocation(acceptedLocation, locations);
                    }
                    subLocationFlag = false;
                }
            }
        }
        if (locations.isEmpty()) {
            locations = findLocationsInContent(animal);
        }

        return locations;
    }


    private ArrayList<String> findLocationsInContent(Animal animal) {
        ArrayList<String> locations = new ArrayList<>();
        String content = animal.getContent();
        Matcher sentenceMatcher = LOCATION_SENTENCE_PATTERN.matcher(content);
        String sentenceGroup;
        Matcher possibleLocationMatcher;
        Matcher subLocationMatcher;
        boolean locationFlag = false;
        boolean subLocationFlag = false;
        String[] tokens = animal.getTitle().split("\\s");
        String foundLocation;

        while (sentenceMatcher.find()) {
            sentenceGroup = sentenceMatcher.group();
            sentenceGroup = sentenceGroup.replaceAll("not.*", "").replaceAll("except.*", "");
            if (!Pattern.compile("(" + tokens[tokens.length-1].replaceAll("\\(?\\)?", "") + "|they|species|\\bit\\b)", Pattern.CASE_INSENSITIVE).matcher(sentenceMatcher.group()).find()) {
                continue;
            }
            possibleLocationMatcher = LOCATION_PATTERN.matcher(sentenceGroup);
            while (possibleLocationMatcher.find()) {
                for (String acceptedLocation : acceptedLocations) {
                    if (Pattern.compile("\\b\\[{0,2}" + acceptedLocation, Pattern.CASE_INSENSITIVE).matcher(possibleLocationMatcher.group()).find()) {
                        locationFlag = true;
                        for (String acceptedSubLocation : acceptedSubLocations) {
                            subLocationMatcher = Pattern.compile(acceptedSubLocation, Pattern.CASE_INSENSITIVE).matcher(possibleLocationMatcher.group());
                            if (subLocationMatcher.find()) {
                                subLocationFlag = true;
                                foundLocation = (subLocationMatcher.group() + " " + acceptedLocation).toLowerCase();
                                if (!locations.contains(foundLocation)) {
                                    locations.add(foundLocation);
                                }
                            }
                        }
                        if (!subLocationFlag) {
                            resolveWholeLocation(acceptedLocation, locations);
                        }
                        subLocationFlag = false;
                        break;
                    }
                }
                if (!locationFlag) {
                    for (String country : CountriesLocations.getCountriesLocations().keySet()) {
                        if (Pattern.compile(country, Pattern.CASE_INSENSITIVE).matcher(possibleLocationMatcher.group()).find()
                                && !locations.contains(CountriesLocations.getCountriesLocations().get(country))) {
                            locations.add(CountriesLocations.getCountriesLocations().get(country));
                        }
                    }
                }
                locationFlag = false;
            }
        }

        return locations;
    }

    private ArrayList<String> findActivityTime(Animal animal) {
        String content = animal.getContent();
        ArrayList<String> activityTime = new ArrayList<>();
        String[] tokens = animal.getTitle().split("\\s");
        Pattern nocturnalPattern = Pattern.compile(" ((they|it|species|" + tokens[tokens.length-1].replaceAll("\\)?\\(?", "") +")[^.]*?nocturnal[^A-Za-z])(.*?\\.){1,2}", Pattern.CASE_INSENSITIVE);
        Matcher nocturnalMatcher = nocturnalPattern.matcher(content);

        if (nocturnalMatcher.find()) {
            activityTime.add("nocturnal");
            if (Pattern.compile("(diurnal.*?nocturnal)|(nocturnal.*?(although|though|however|but|whereas|except).*?(diurnal|day|crepuscular|morning|evening|noon|dawn|dusk))", Pattern.CASE_INSENSITIVE).matcher(nocturnalMatcher.group()).find()) {
                activityTime.add("diurnal");
            }
        }
        else {
            activityTime.add("diurnal");
        }

        return activityTime;
    }

    private ArrayList<String> findHabitats(Animal animal) {
        ArrayList<String> habitats = new ArrayList<>();
        HashMap<String, Integer> wordCounts = new HashMap<>();
        Matcher habitatMatcher;
        Matcher firstParagraphMatcher = Pattern.compile("\\n[^'\\n]*?'{3,5}[^']+'{3,5}.*?(is|are).*?\\n").matcher(animal.getContent());
        Matcher habitatParagraphMatcher = Pattern.compile("\\n={1,3}[^\\n]*?habitat.*?\\n{1,2}.*?(\\n\\n|\\bnot\\b|\\bavoid|\\bexcept)", Pattern.CASE_INSENSITIVE).matcher(animal.getContent());
        String group;

        if (firstParagraphMatcher.find()) {
            habitatMatcher = ACCEPTED_HABITATS_PATTERN.matcher(firstParagraphMatcher.group());
            while (habitatMatcher.find()) {
                group = habitatMatcher.group().toLowerCase();
                group = group.replaceAll("\\[?]?", "").replaceAll("s?(\\s|])", "");
                if (!habitats.contains(group)) habitats.add(group);
            }
        }

        if (habitatParagraphMatcher.find()) {
            habitatMatcher = ACCEPTED_HABITATS_PATTERN.matcher(habitatParagraphMatcher.group());
            while (habitatMatcher.find()) {
                group = habitatMatcher.group().toLowerCase();
                group = group.replaceAll("\\[?]?", "").replaceAll("s?(\\s|])", "");
                if (!habitats.contains(group)) habitats.add(group);
            }
        }

        if (habitats.isEmpty()) {
            habitatMatcher = ACCEPTED_HABITATS_PATTERN.matcher(animal.getContent());
            while (habitatMatcher.find()) {
                group = habitatMatcher.group().toLowerCase();
                group = group.replaceAll("\\[?]?", "").replaceAll("s?(\\s|])", "");
                if (wordCounts.containsKey(group)) {
                    wordCounts.put(group, wordCounts.get(group)+1);
                }
                else {
                    wordCounts.put(group, 1);
                }
            }

            for (String word : wordCounts.keySet()) {
                if (wordCounts.get(word) >= 3) {
                    habitats.add(word);
                }
            }
        }

        return habitats;
    }

    private void resolveWholeLocation(String location, ArrayList<String> locations) {
        String[] subLocations = {"west", "north", "east", "south", "central"};
        String locationToAdd;
        boolean flag = false;

        for (String loc : locations) {
            if (loc.contains(location)) {
                flag = true;
                break;
            }
        }
        if (!flag)
            for (String subLocation : subLocations) {
                locationToAdd = subLocation + " " + location;
                if (!locations.contains(locationToAdd)) locations.add(locationToAdd);
            }
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public JavaRDD<Row> getRdd() {
        return rdd;
    }
}
