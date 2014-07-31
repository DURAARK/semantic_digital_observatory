package utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

public class TextUtils {

    public static String removeSpecialSymbols(String str) {
        String rst = str;
        rst = rst.replaceAll("&#224;", "à");
        rst = rst.replaceAll("&#243;", "ó");
        rst = rst.replaceAll("&#246;", "ö");
        rst = rst.replaceAll("&#232;", "è");
        rst = rst.replaceAll("&#237;", "í");
        rst = rst.replaceAll("&#225;", "á");
        rst = rst.replaceAll("&#252;", "ü");
        rst = rst.replaceAll("&#233;", "é");
        rst = rst.replaceAll("&#263;", "ć");
        rst = rst.replaceAll("&#353;", "š");

        return rst;
    }
    /*
     * Parses content by taking into account specific content which matches a specific regular expression.
     */

    public static void parseString(String regex, String content) {
        String[] lines = content.split("\n");

        Pattern p = Pattern.compile(regex);

        StringBuffer result = new StringBuffer();
        StringBuffer proceedings = new StringBuffer();

        for (String line : lines) {
            String tmp = line.trim();
            if (tmp.startsWith("<li style")) {

                Matcher m = p.matcher(tmp);
                if (m.find()) {
                    String str = tmp.subSequence(m.start(), m.end()).toString() + "/preflayout=flat/";
                    str = "http://dl.acm.org/" + str;
                    str = str.replaceAll("&", "&amp;");

                    String title = tmp.substring(tmp.indexOf(">", m.end()) + 1, tmp.indexOf("</a>"));

                    proceedings.append(title.trim());
                    proceedings.append("\t");
                    proceedings.append(str.trim());
                    proceedings.append("\n");

                    result.append(str);
                    result.append("\n");
                }
            }
        }

        FileUtils.saveText(result.toString(), "SeedURLProceedings");
        FileUtils.saveText(proceedings.toString(), "Proceedings");
    }

    /*
     * Split a string into tokens, and consider as well its stemming. 
     */
    public static Set<String> getTextTokenSet(String text, boolean isStemmed) {
        String[] tmp = StringUtils.split(text);
        Stemmer stm = new Stemmer();

        Set<String> set = new HashSet<String>();

        for (String t : tmp) {
            if (isStemmed) {
                set.add(stm.stripAffixes(t));
            } else {
                set.add(t);
            }
        }

        return set;
    }
    
     /*
     * Split a string into tokens, and consider as well its stemming. 
     */
    public static List<String> getTextTokenList(String text, boolean isStemmed) {
        String[] tmp = StringUtils.split(text);
        Stemmer stm = new Stemmer();

        List<String> set = new ArrayList<String>();

        for (String t : tmp) {
            if (isStemmed) {
                set.add(stm.stripAffixes(t));
            } else {
                set.add(t);
            }
        }

        return set;
    }

    public static boolean isTermContainedInSet(Set<String> values, String value) {
        if(values == null || values.isEmpty())
            return true;
        
        for (String valuecmp : values) {
            if (value.toLowerCase().trim().contains(valuecmp.toLowerCase().trim())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Measures the Jaccard distance between two sets of terms.
     *
     * We add a factor which takes into account the ratio of the overlapping
     * terms from the sets from a particular resources compared to its total
     * number of items in the set. This enables us to rank higher those
     * annotations, whose Jaccard index is low, due to the limited number of
     * items in its set, thus, proposing a normalized Jaccard index ratio.
     *
     * @param setA
     * @param setB
     * @return
     */
    public static double computeJaccardDistance(Set<String> setA, Set<String> setB) {
        double rst = 0;
        if (setA == null || setB == null) {
            return 0;
        }

        Set<String> tmpD = new HashSet<String>(setA);
        Set<String> tmpN = new HashSet<String>(setA);

        tmpD.retainAll(setB);
        tmpN.addAll(setB);

        //the normalized score of the Jaccard index, which takes into account 
        //the number of items shared by a resource with respect to an extracted annotation

        rst = (tmpD.size() / (double) tmpN.size());
        return rst;
    }
}
