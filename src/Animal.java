import java.util.ArrayList;

public class Animal {
    private String title;
    private String content;
    private ArrayList<String> locations;
    private ArrayList<String> habitats;
    private ArrayList<String> activityTime;

    public Animal() {
        this.locations = new ArrayList<>();
        this.habitats = new ArrayList<>();
        this.activityTime = new ArrayList<>();
    }

    public String getTitle() {
        return this.title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return this.content;
    }

    public void setContent(String content) {

        this.content = content;
    }

    public ArrayList<String> getLocations() {
        return locations;
    }

    public void setLocations(ArrayList<String> locations) {
        this.locations = locations;
    }

    public ArrayList<String> getHabitats() {
        return habitats;
    }

    public void setHabitats(ArrayList<String> ecosystems) {
        this.habitats = ecosystems;
    }

    public ArrayList<String> getActivityTime() {
        return activityTime;
    }

    public void setActivityTime(ArrayList<String> activityTime) {
        this.activityTime = activityTime;
    }
}
