public class Main {
    public static void main(String[] args) {
        Preprocessor preprocessor = new Preprocessor();
        try {
            preprocessor.parsePages("data/enwiki-latest-pages-articles1.xml-p1p41242");
        }
        catch (Exception e) {
            System.err.println("Couldn't open file");
            System.out.println(e.toString());
        }
        System.out.println(preprocessor.getIndex().get("panda"));
        System.out.println(preprocessor.getIndex().get("frog"));
    }
}
