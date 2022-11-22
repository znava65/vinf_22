import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Preprocessor {

    private final ArrayList<Animal> animals;
//    private final Pattern IS_ANIMAL_CATEGORY_PATTERN = Pattern.compile("\\[\\[Category:.*\\b(mammals|vertebrates|invertebrates|reptiles|amphibians|insects)\\b.*?]]", Pattern.CASE_INSENSITIVE);
    private final Pattern IS_ANIMAL_PATTERN = Pattern.compile("wikispecies|animalia|inhabits?\\b|speciesbox", Pattern.CASE_INSENSITIVE);
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

    private HashMap<String, ArrayList<Long>> index;
    public Preprocessor() {
        this.animals = new ArrayList<>();
        this.index = new HashMap<>();
    }

    public void parsePages(String path) throws IOException {
        StringBuilder stringBuilder;
        BufferedReader br = new BufferedReader(new FileReader(path));
        String line;
        String content;
        Animal animal;
        Pattern pageStartPattern = Pattern.compile("<page>");
        Pattern pageEndPattern = Pattern.compile("</page>");
        Pattern titlePattern = Pattern.compile("<title>.*?</title>");
        long i = 0;
        String[] tokens;

        while ((line = br.readLine()) != null) {
            if (pageStartPattern.matcher(line).find()) {
                animal = new Animal();
                stringBuilder = new StringBuilder();
                do {
                    stringBuilder.append(line).append("\n");
                    // check if there is a title in the line
                    if (titlePattern.matcher(line).find()) {
                        animal.setTitle(line.replaceAll(".*<title>", "").replaceAll("</title>.*", "").toLowerCase());
                    }
                    line = br.readLine();
                }
                while(!pageEndPattern.matcher(line).find());

                stringBuilder.append(line);
                content = clearContent(stringBuilder.toString());
                if (isAnimal(content)) {
                    animal.setContent(content);
                    animals.add(animal);
                    animal.setLocations(findLocations(animal));
                    animal.setActivityTime(findActivityTime(animal));
                    animal.setHabitats(findHabitats(animal));

                    System.out.println(animal.getTitle());
                    System.out.println("Locations: " + animal.getLocations().toString());
                    System.out.println("Activity: " + animal.getActivityTime().toString());
                    System.out.println("Habitats: " + animal.getHabitats().toString());
                    System.out.println();

                    //indexing
                    tokens = animal.getTitle().split("\\s");
                    for (String token : tokens) {
                        if (!this.index.containsKey(token)) this.index.put(token, new ArrayList<>());
                        this.index.get(token).add(i);
                    }
                    i++;
                }
            }
        }
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

        BufferedReader br = new BufferedReader(new StringReader(content));
        try {
            while ((line = br.readLine()) != null) {
                Matcher matcher = IS_ANIMAL_PATTERN.matcher(line);
                Matcher isExtinctMatcher = IS_EXTINCT_PATTERN.matcher(line);
                if (Pattern.compile("plantae|fungi|virae|\\[\\[Category:.*flora.*?]]", Pattern.CASE_INSENSITIVE).matcher(line).find()) {
                    return false;
                }
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

        if (extinctCounter >= 10) {
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
                locationMatcher = Pattern.compile("([^.,\\s]*?\\s){0,3}" + acceptedLocation + "s?n?\\b", Pattern.CASE_INSENSITIVE).matcher(firstParagraphMatcher.group());
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
            if (!Pattern.compile("(" + tokens[tokens.length-1] + "|they|species|\\bit\\b)", Pattern.CASE_INSENSITIVE).matcher(sentenceMatcher.group()).find()) {
                continue;
            }
            possibleLocationMatcher = LOCATION_PATTERN.matcher(sentenceGroup);
            while (possibleLocationMatcher.find()) {
                for (String acceptedLocation : acceptedLocations) {
                    if (Pattern.compile(acceptedLocation, Pattern.CASE_INSENSITIVE).matcher(possibleLocationMatcher.group()).find()) {
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
        Pattern nocturnalPattern = Pattern.compile("(((they|it|species|" + tokens[tokens.length-1] +")[^.]*?nocturnal[^A-Za-z])|(nocturnal[^.A-Za-z]*?" + animal.getTitle() + "))(.*?\\.){1,2}", Pattern.CASE_INSENSITIVE);
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

    public ArrayList<Animal> getAnimals() {
        return animals;
    }

    public HashMap<String, ArrayList<Long>> getIndex() {
        return index;
    }
}
