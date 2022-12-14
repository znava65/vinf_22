import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class SearchTest {
    private static Search search;
    @BeforeAll
    static void createSearchObject() {
        search = new Search(null, null);
    }

    /**
     * One of the posting lists is null.
     */
    @Test
    void testIntersectNull() {
        ArrayList<Integer> l1 = new ArrayList<>();
        assertNull(search.intersect(l1, null));
    }

    /**
     * Two valid posting lists.
     */
    @Test
    void testIntersectValid() {
        ArrayList<Integer> l1 = new ArrayList<>();
        ArrayList<Integer> l2 = new ArrayList<>();

        for (int i=0; i<3; i++) {
            l1.add(i);
            l2.add(i+1);
        }
        ArrayList<Integer> result = search.intersect(l1, l2);

        assertTrue(result.size() == 2 && result.contains(1) && result.contains(2));
    }

    /**
     * Different length of two posting lists.
     */
    @Test
    void testIntersectDifferentLength() {
        ArrayList<Integer> l1 = new ArrayList<>();
        ArrayList<Integer> l2 = new ArrayList<>();

        l2.add(0);

        for (int i=1; i<4; i++) {
            l1.add(i);
            l2.add(i+1);
        }

        ArrayList<Integer> result = search.intersect(l1, l2);

        assertTrue(result.size() == 2 && result.contains(2) && result.contains(3));
    }

    /**
     * Three valid posting lists.
     */
    @Test
    void testIntersectMultiple() {
        ArrayList<Integer> l1 = new ArrayList<>();
        ArrayList<Integer> l2 = new ArrayList<>();
        ArrayList<Integer> l3 = new ArrayList<>();

        for (int i=0; i<3; i++) {
            l1.add(i);
            l2.add(i+1);
            l3.add(i+2);
        }

        ArrayList<ArrayList<Integer>> postingLists = new ArrayList<>();
        postingLists.add(l1);
        postingLists.add(l2);
        postingLists.add(l3);

        ArrayList<Integer> result = search.intersect(postingLists);

        assertTrue(result.size() == 1 && result.contains(2));
    }

    /**
     * Three valid posting lists with empty intersection.
     */
    @Test
    void testIntersectMultipleEmpty() {
        ArrayList<Integer> l1 = new ArrayList<>();
        ArrayList<Integer> l2 = new ArrayList<>();
        ArrayList<Integer> l3 = new ArrayList<>();

        for (int i=0; i<3; i++) {
            l1.add(i);
            l2.add(i+1);
            l3.add(i+3);
        }

        ArrayList<ArrayList<Integer>> postingLists = new ArrayList<>();
        postingLists.add(l1);
        postingLists.add(l2);
        postingLists.add(l3);

        ArrayList<Integer> result = search.intersect(postingLists);

        assertTrue(result.isEmpty());
    }
}