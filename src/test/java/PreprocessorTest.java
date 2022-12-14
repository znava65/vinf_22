import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;

import static org.junit.jupiter.api.Assertions.*;

class PreprocessorTest {
    /**
     * 
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    @Test
    void testClearContent() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String content = "<page>THE PAGE DETAILS <text>CLEARED<ref>this is the reference</ref> CONTENT&lt;refmultiple references/ref&gt;[[File:file.txt]]\n</text></page>";
        Preprocessor p = new Preprocessor(null);
        assertEquals(p.getClearContentMethod().invoke(p, content), "CLEARED CONTENT");
    }
}