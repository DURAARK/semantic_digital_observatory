package utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class PrintUtils {
    /*
     * Prints the tokens for the different size, with accompanying indexes.
     */

    public static String printTokens(HashMap<String, Set<String>> data) {
        StringBuffer sb = new StringBuffer();

        for (String token : data.keySet()) {
            sb.append(token);
            sb.append(": [");
            Set<String> metadocs = data.get(token);

            int i = 0;
            for (String doc : metadocs) {
                sb.append(doc);

                if (i < (metadocs.size() - 1)) {
                    sb.append(", ");
                }

                i++;
            }

            sb.append("]");
            sb.append("\n");
        }

        return sb.toString();
    }

    /*
     * Prints the tokens for the different size, with accompanying indexes.
     */
    public static String printResource(Map<String, Map<String, Set<String>>> data) {
        StringBuffer sb = new StringBuffer();

        for (String resource : data.keySet()) {
            Map<String, Set<String>> resourcedata = data.get(resource);
            for (String url : resourcedata.keySet()) {
                Set<String> categories = resourcedata.get(url);
                sb.append(resource);
                sb.append("\t");
                sb.append(url);
                sb.append("\t");
                sb.append(categories.toString());
                sb.append("\n");
            }

            sb.append("\n");
        }

        return sb.toString();
    }


    /*
     * Constructs the string representation of a term matrix, which in this case
     * contains the consecutive appearance of two terms.
     */
    public static String printFlatTermMatrix(Map<String, Map<String, Double>> termmatrix) {
        StringBuilder sb = new StringBuilder();

        for (String term : termmatrix.keySet()) {
            Map<String, Double> values = termmatrix.get(term);
            for (String termcmp : termmatrix.keySet()) {
                Double val = values.get(termcmp);
                if (val == null) {
                    val = 0.0;
                }
                sb.append(term).append("-").append(termcmp);
                sb.append("\t").append(val);
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    /*
     * Gets the string representation of a set.
     */
    public static String printSet(Set<String> set) {
        StringBuffer sb = new StringBuffer();

        for (String s : set) {
            sb.append(s);
            sb.append("\n");
        }

        return sb.toString();
    }

    /*
     * Gets the string representation of a set.
     */
    public static String printSetD(Set<Double> set) {
        StringBuffer sb = new StringBuffer();

        for (Double s : set) {
            sb.append(s);
            sb.append("\n");
        }

        return sb.toString();
    }

    /*
     * Constructs the string representation of a term matrix, which in this case
     * contains the consecutive appearance of two terms.
     */
    public static String printPOSMatrix(Map<String, Map<String, Double>> termmatrix) {
        StringBuffer sb = new StringBuffer();

        String cols = termmatrix.keySet().toString();
        cols = cols.replaceAll("\\,\\s{1,}", "\t");
        cols = cols.replaceAll("\\]", "");
        cols = cols.replaceAll("\\[", "");

        sb.append("\t" + cols + "\n");
        for (String term : termmatrix.keySet()) {
            sb.append(term);
            Map<String, Double> values = termmatrix.get(term);
            for (String termcmp : termmatrix.keySet()) {
                Double val = values.get(termcmp);
                if (val == null) {
                    val = 0.0;
                }
                sb.append("\t" + val);
            }

            sb.append("\n");
        }

        return sb.toString();
    }

    /*
     * Constructs the string representation of a term matrix, which in this case
     * contains the consecutive appearance of two terms.
     */
    public static String printTermMatrix(Map<String, Map<String, Integer>> termmatrix) {
        StringBuffer sb = new StringBuffer();

        String cols = termmatrix.keySet().toString();
        cols = cols.replaceAll("\\,\\s{1,}", "\t");
        cols = cols.replaceAll("\\]", "");
        cols = cols.replaceAll("\\[", "");

        sb.append("\t" + cols + "\n");
        for (String term : termmatrix.keySet()) {
            sb.append(term);
            Map<String, Integer> values = termmatrix.get(term);
            for (String termcmp : termmatrix.keySet()) {
                Integer val = values.get(termcmp);
                if (val == null) {
                    val = 0;
                }
                sb.append("\t" + val);
            }

            sb.append("\n");
        }

        return sb.toString();
    }


    /*
     * Prints a simple map datastructure.
     */
    public static String printMap(Map<String, String> map) {
        StringBuffer sb = new StringBuffer();
        for (String key : map.keySet()) {
            String val = map.get(key);

            sb.append(key);
            sb.append("\t");
            sb.append(val);
            sb.append("\n");
        }
        return sb.toString();
    }

    /*
     * Prints a simple map datastructure.
     */
    public static String printMapReverse(Map<String, String> map) {
        StringBuffer sb = new StringBuffer();
        for (String key : map.keySet()) {
            String val = map.get(key);

            sb.append(val);
            sb.append("\t");
            sb.append(key);
            sb.append("\n");
        }
        return sb.toString();
    }

    /*
     * Prints a simple map datastructure.
     */
    public static String printMapEntries(Map<String, Entry<String, String>> map) {
        StringBuffer sb = new StringBuffer();
        for (String key : map.keySet()) {
            Entry<String, String> val = map.get(key);

            sb.append(key);
            sb.append("\t");
            sb.append(val.getKey());
            sb.append("\t");
            sb.append(val.getValue());
            sb.append("\n");
        }
        return sb.toString();
    }

    /*
     * Prints a simple map datastructure.
     */
    public static String printMap(Map<String, String> map, int k) {
        StringBuffer sb = new StringBuffer();
        for (String key : map.keySet()) {
            String val = map.get(key);

            sb.append(key);
            sb.append(" - ");
            sb.append(val);
            sb.append("\n");
        }
        return sb.toString();
    }

    /*
     * Prints a simple map datastructure.
     */
    public static String printMapDouble(Map<String, Double> map) {
        StringBuffer sb = new StringBuffer();
        for (String key : map.keySet()) {
            Double val = map.get(key);

            sb.append(key);
            sb.append(" - ");
            sb.append(val);
            sb.append("\n");
        }
        return sb.toString();
    }

    public static String printNestedMap(Map<Object, Map<Object, Object>> map) {
        StringBuffer sb = new StringBuffer();
        for (Object key : map.keySet()) {
            Map<Object, Object> submap = map.get(key);

            sb.append("--------------------------------\n");
            sb.append(key);
            sb.append("\n");
            sb.append("--------------------------------\n");

            for (Object subkey : submap.keySet()) {
                Object val = submap.get(subkey);
                sb.append(subkey);
                sb.append("\t");
                sb.append(val);
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    public static String printNestedMapStrInt(Map<String, Map<String, Integer>> map) {
        StringBuffer sb = new StringBuffer();
        for (Object key : map.keySet()) {
            Map<String, Integer> submap = map.get(key);

            sb.append("--------------------------------\n");
            sb.append(key);
            sb.append("\n");
            sb.append("--------------------------------\n");

            for (String subkey : submap.keySet()) {
                Integer val = submap.get(subkey);
                sb.append(subkey);
                sb.append("\t");
                sb.append(val);
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    public static String printSimpleMapInt(Map<String, Integer> typeassoc) {
        // TODO Auto-generated method stub
        StringBuffer sb = new StringBuffer();
        for (String key : typeassoc.keySet()) {
            Integer val = typeassoc.get(key);

            sb.append(key);
            sb.append("\t");
            sb.append(val);
            sb.append("\n");
        }
        return sb.toString();
    }

    public static String printSimpleMapSet(Map<String, Set<String>> resourcetypeassoc) {
        // TODO Auto-generated method stub
        StringBuffer sb = new StringBuffer();
        for (String key : resourcetypeassoc.keySet()) {
            Set<String> val = resourcetypeassoc.get(key);

            sb.append(key);
            sb.append("\t");
            sb.append(val);
            sb.append("\n");
        }
        return sb.toString();
    }
    
    
    public static String printSimpleIntMapSet(Map<Integer, Set<String>> resourcetypeassoc) {
        // TODO Auto-generated method stub
        StringBuilder sb = new StringBuilder();
        for (int key : resourcetypeassoc.keySet()) {
            Set<String> val = resourcetypeassoc.get(key);

            sb.append(key);
            sb.append("\t");
            sb.append(val);
            sb.append("\n");
        }
        return sb.toString();
    }

    public static String printSimpleMapSetNewLine(Map<String, Set<String>> resourcetypeassoc) {
        // TODO Auto-generated method stub
        StringBuffer sb = new StringBuffer();
        for (String key : resourcetypeassoc.keySet()) {
            Set<String> vals = resourcetypeassoc.get(key);

            for (String val : vals) {
                sb.append(key);
                sb.append("\t");
                sb.append(val);
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    public static String printSimpleMapSetInteger(Map<String, Set<Integer>> resourcetypeassoc) {
        // TODO Auto-generated method stub
        StringBuffer sb = new StringBuffer();
        for (String key : resourcetypeassoc.keySet()) {
            Set<Integer> val = resourcetypeassoc.get(key);

            sb.append(key);
            sb.append("\t");
            sb.append(val.size());
            sb.append("\n");
        }
        return sb.toString();
    }
    
     public static String printSimpleMapSetInt(Map<String, Set<String>> resourcetypeassoc) {
        // TODO Auto-generated method stub
        StringBuffer sb = new StringBuffer();
        for (String key : resourcetypeassoc.keySet()) {
            Set<String> val = resourcetypeassoc.get(key);

            sb.append(key);
            sb.append("\t");
            sb.append(val.size());
            sb.append("\n");
        }
        return sb.toString();
    }

    public static String printSimpleMapSet(Map<String, Set<String>> resourcetypeassoc, boolean removeBrackets) {
        // TODO Auto-generated method stub
        StringBuffer sb = new StringBuffer();
        for (String key : resourcetypeassoc.keySet()) {
            Set<String> val = resourcetypeassoc.get(key);

            sb.append(key);
            sb.append("\t");
            if (!removeBrackets) {
                sb.append(val);
            } else {
                int counter = 0;
                for (String subval : val) {
                    sb.append(subval);

                    if (counter != val.size() - 1) {
                        sb.append(";");
                    }
                    counter++;
                }
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public static String printSimpleMapSetEnrichments(Map<String, Set<String>> resourcetypeassoc) {
        // TODO Auto-generated method stub
        StringBuffer sb = new StringBuffer();
        for (String key : resourcetypeassoc.keySet()) {
            Set<String> val = resourcetypeassoc.get(key);

            sb.append(key.trim());
            sb.append("\t");
            sb.append("[");
            sb.append(val.size());
            sb.append("]\t");
            sb.append(val);
            sb.append("\n");
        }
        return sb.toString();
    }

    public static String printSimpleMapSetEnrichmentsDataset(Map<String, Map<String, Integer>> resourcetypeassoc, Map<String, Integer> keyvalues, Map<String, String> mappings, Map<String, Set<String>> entities) {
        // TODO Auto-generated method stub
        StringBuffer sb = new StringBuffer();
        sb.append("ID\tTotal\t");
        for (String dataset : mappings.keySet()) {
            sb.append(dataset + "\t");
        }
        sb.append("#Datasets\n");
        for (String key : resourcetypeassoc.keySet()) {
            Map<String, Integer> val = resourcetypeassoc.get(key);

            sb.append(key);
            sb.append("\t");
            sb.append(keyvalues.get(key));
            sb.append("\t");

            double datasettotalval = 0;
            for (String dataset : mappings.keySet()) {
                Integer datasetval = val.get(dataset);
                if (datasetval == null) {
                    datasetval = 0;
                }

                int tmpentsize = 1;
                if (entities.get(dataset) != null) {
                    tmpentsize = entities.get(dataset).size();
                }

                sb.append((datasetval / (double) tmpentsize) + "\t");
                datasettotalval += (datasetval / (double) tmpentsize);
            }
            sb.append(datasettotalval);
            sb.append("\n");
        }
        return sb.toString();
    }

    public static String printSimpleMapSetEnrichmentsTypeDataset(Map<String, Map<String, Integer>> resourcetypeassoc, Map<String, Integer> keyvalues, Map<String, String> mappings, Map<String, Integer> entities) {
        // TODO Auto-generated method stub
        StringBuffer sb = new StringBuffer();
        sb.append("ID\tTotal\t");
        for (String dataset : mappings.keySet()) {
            sb.append(dataset + "\t");
        }
        sb.append("#Datasets\n");
        for (String key : resourcetypeassoc.keySet()) {
            Map<String, Integer> val = resourcetypeassoc.get(key);

            sb.append(key);
            sb.append("\t");
            sb.append(keyvalues.get(key));
            sb.append("\t");

            double datasettotalval = 0;
            for (String dataset : mappings.keySet()) {
                Integer datasetval = val.get(dataset);
                if (datasetval == null) {
                    datasetval = 0;
                }

                int tmpentsize = 1;
                if (entities.get(dataset) != null) {
                    tmpentsize = entities.get(dataset);
                }

                sb.append((datasetval / (double) tmpentsize) + "\t");
                datasettotalval += (datasetval / (double) tmpentsize);
            }
            sb.append(datasettotalval);
            sb.append("\n");
        }
        return sb.toString();
    }

    public static String printNestedMapEntries(Map<Object, Set<Entry<Object, Object>>> entities) {
        // TODO Auto-generated method stub
        StringBuffer sb = new StringBuffer();

        for (Object key : entities.keySet()) {
            Set<Entry<Object, Object>> entries = entities.get(key);
            if (entries.isEmpty()) {
                continue;
            }

            sb.append("\n-------------------------\n");
            sb.append(key);
            sb.append("\n-------------------------\n");

            for (Entry<Object, Object> entry : entries) {
                sb.append(entry.getKey());
                sb.append("\t");
                sb.append(entry.getValue());
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    public static String printNestedMapEntriesL(Map<String, List<Entry<String, Integer>>> entities) {
        // TODO Auto-generated method stub
        StringBuffer sb = new StringBuffer();

        for (Object key : entities.keySet()) {
            List<Entry<String, Integer>> entries = entities.get(key);
            if (entries.isEmpty()) {
                continue;
            }

            sb.append("\n-------------------------\n");
            sb.append(key);
            sb.append("\n-------------------------\n");

            for (Entry<String, Integer> entry : entries) {
                sb.append(entry.getKey());
                sb.append("\t");
                sb.append(entry.getValue());
                sb.append("\n");
            }
        }

        return sb.toString();
    }
}
