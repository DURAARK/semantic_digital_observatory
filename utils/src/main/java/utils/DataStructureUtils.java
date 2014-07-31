package utils;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.stat.inference.TTest;

public class DataStructureUtils {
    /*
     * Merges two Map<String, String> maps by adding new entries only when there is no 
     * other entry with the same *value* in the original map (map1). 
     */

    public static void mergeMaps(Map<String, String> map1, Map<String, String> map2) {
        if (map2 == null) {
            return;
        }

        for (String key : map2.keySet()) {
            String val = map2.get(key).trim();

            if (!map1.containsValue(val)) {
                map1.put(key, val);
            }
        }
    }

    /*
     * Reverses the key-value pairs in a map where each pair is unique.
     */
    public static Map<String, String> reverseMap(Map<String, String> map) {
        Map<String, String> result = new TreeMap<String, String>();

        for (String key : map.keySet()) {
            String value = map.get(key);

            result.put(value, key);
        }
        return result;
    }

    public static Map<String, String> filterMaps(Map<String, String> map) {
        Map<String, String> result = new TreeMap<String, String>();

        return result;
    }

    //================================== Data Analytics of Enrichments=======================
    /*
     * Get the id based on a mapping of individual resources to dataset names.
     */
    public static String getId(Map<String, String> mappings, String id) {
        for (String key : mappings.keySet()) {
            if (id.startsWith(key)) {
                return mappings.get(key);
            }
        }
        return id;
    }

    /*
     * Counts the number of different entity types assigned to a specific dataset.
     */
    public static void countEntityTypes(Map<String, String> mappings, String basedir, String outdir) {
        String content = FileUtils.readText(basedir + "ResourceTypeAssociations");
        String[] lines = content.split("\n");

        Map<String, Map<String, Integer>> restypes = new HashMap<String, Map<String, Integer>>();

        for (String line : lines) {
            String[] tmp = line.split("\t");
            if (tmp.length < 2) {
                continue;
            }

            String id = tmp[0].trim();
            id = getId(mappings, id);

            String typestr = tmp[1];
            typestr = typestr.replaceAll("\\[", "");
            typestr = typestr.replaceAll("\\[", "");

            String[] types = typestr.split(",");

            Map<String, Integer> subrestypes = restypes.get(id);
            if (subrestypes == null) {
                subrestypes = new HashMap<String, Integer>();
                restypes.put(id, subrestypes);
            }

            for (String type : types) {
                Integer value = subrestypes.get(type);
                if (value == null) {
                    value = 0;
                }
                value++;
                subrestypes.put(type, value);
            }
        }

        StringBuffer sb = new StringBuffer();
        for (String res : restypes.keySet()) {
            sb.append(res + "\t" + restypes.get(res).size() + "\n");
        }

        FileUtils.saveText(sb.toString(), outdir + "DatasetTypeIndex");
    }

    /*
     * Prints the different entity types assigned to a specific dataset.
     */
    public static void printEntityTypes(Map<String, String> mappings, String basedir, String outdir) {
        String content = FileUtils.readText(basedir + "ResourceTypeAssociations");
        String[] lines = content.split("\n");

        Map<String, Map<String, Integer>> restypes = new HashMap<String, Map<String, Integer>>();

        for (String line : lines) {
            String[] tmp = line.split("\t");
            if (tmp.length < 2) {
                continue;
            }

            String id = tmp[0].trim();
            id = getId(mappings, id);

            String typestr = tmp[1];
            typestr = typestr.replaceAll("\\[", "");
            typestr = typestr.replaceAll("\\[", "");

            String[] types = typestr.split(",");

            Map<String, Integer> subrestypes = restypes.get(id);
            if (subrestypes == null) {
                subrestypes = new HashMap<String, Integer>();
                restypes.put(id, subrestypes);
            }

            for (String type : types) {
                String tmptype = type;
                tmptype = tmptype.replaceAll("\\[", "");
                tmptype = tmptype.replaceAll("\\]", "").trim();

                Integer value = subrestypes.get(tmptype);
                if (value == null) {
                    value = 0;
                }
                value++;
                subrestypes.put(tmptype, value);
            }
        }

        StringBuffer sb = new StringBuffer();
        for (String res : restypes.keySet()) {
            if (res.trim().isEmpty()) {
                continue;
            }

            Map<String, Integer> subrestypes = restypes.get(res);
            sb.append("\n-------------------------------------------\n");
            sb.append(res);
            sb.append("\n");

            for (String key : subrestypes.keySet()) {
                if (key.trim().isEmpty()) {
                    continue;
                }
                sb.append(key + "\t" + subrestypes.get(key) + "\n");
            }
            sb.append("\n-------------------------------------------\n");
        }

        FileUtils.saveText(sb.toString(), outdir + "DatasetIndividualTypeIndex");
    }

    /*
     * Detects correlations based on the different entity types assigned to a specific dataset with respect to other datasets.
     */
    public static void correlationEntityTypes(Map<String, String> mappings, String basedir, String outdir, int threshold) {
        String content = FileUtils.readText(basedir + "ResourceTypeAssociations");
        String[] lines = content.split("\n");

        Map<String, Map<String, Integer>> restypes = new HashMap<String, Map<String, Integer>>();

        for (String line : lines) {
            String[] tmp = line.split("\t");
            if (tmp.length < 2) {
                continue;
            }

            String typestr = tmp[1];
            typestr = typestr.replaceAll("\\[", "");
            typestr = typestr.replaceAll("\\[", "");

            String[] types = typestr.split(",");

            String id = tmp[0].trim();
            id = getId(mappings, id);

            Map<String, Integer> subrestypes = restypes.get(id);
            if (subrestypes == null) {
                subrestypes = new HashMap<String, Integer>();
                restypes.put(id, subrestypes);
            }

            for (String type : types) {
                Integer val = subrestypes.get(type);
                if (val == null) {
                    val = 0;
                }
                val++;
                subrestypes.put(type, val);
            }
        }

        StringBuffer sb = new StringBuffer();
        for (String res : restypes.keySet()) {
            Map<String, Integer> typecounts = restypes.get(res);

            sb.append("----------------");
            sb.append("Clusters for Dataset: " + res + "--------------\n");
            nextcomparison:
            for (String rescmp : restypes.keySet()) {
                if (res != rescmp) {
                    Map<String, Integer> typecountscmp = restypes.get(rescmp);
                    Set<String> tmp = new HashSet<String>(typecounts.keySet());
                    tmp.retainAll(typecountscmp.keySet());
                    if (tmp.size() > 0) {
                        for (String key : tmp) {
                            Integer val1 = typecountscmp.get(key);
                            Integer val2 = typecounts.get(key);

                            if (val1 > threshold && val2 > threshold) {
                                sb.append(rescmp);
                                sb.append("\n");
                                continue nextcomparison;
                            }
                        }
                    }
                }
            }
            sb.append("\n");
        }

        FileUtils.saveText(sb.toString(), outdir + "DatasetTypeClustering");
    }

    /*
     * Prints the different entity categories assigned to a specific dataset.
     */
    public static void printEntityCategories(Map<String, String> mappings, String basedir, String outdir) {
        String content = FileUtils.readText(basedir + "EnrichmentCategories");
        String[] lines = content.split("\n");

        Map<String, Map<String, Integer>> restypes = new HashMap<String, Map<String, Integer>>();

        for (String line : lines) {
            String[] tmp = line.split("\t");
            if (tmp.length < 3) {
                continue;
            }

            String categorystr = tmp[2];
            categorystr = categorystr.replaceAll("\\[", "");
            categorystr = categorystr.replaceAll("\\[", "");

            String[] types = categorystr.split(",");

            String id = tmp[0].trim();
            id = getId(mappings, id);

            Map<String, Integer> subrestypes = restypes.get(id);
            if (subrestypes == null) {
                subrestypes = new HashMap<String, Integer>();
                restypes.put(id, subrestypes);
            }

            for (String type : types) {
                String tmptype = type;
                tmptype = tmptype.replaceAll("\\[", "");
                tmptype = tmptype.replaceAll("\\]", "").trim();

                Integer value = subrestypes.get(tmptype);
                if (value == null) {
                    value = 0;
                }
                value++;
                subrestypes.put(tmptype, value);
            }
        }

        StringBuffer sb = new StringBuffer();
        for (String res : restypes.keySet()) {
            if (res.trim().isEmpty()) {
                continue;
            }

            Map<String, Integer> subrestypes = restypes.get(res);
            sb.append("\n-------------------------------------------\n");
            sb.append(res);
            sb.append("\n");

            for (String key : subrestypes.keySet()) {
                if (key.trim().isEmpty()) {
                    continue;
                }
                sb.append(key + "\t" + subrestypes.get(key) + "\n");
            }
            sb.append("\n-------------------------------------------\n");
        }

        FileUtils.saveText(sb.toString(), outdir + "DatasetIndividualCategoryIndex");
    }

    /*
     * Prints the different entity categories assigned to a specific dataset.
     */
    public static void printResourceCategories(Map<String, String> mappings, String basedir, String outdir) {
        String content = FileUtils.readText(basedir + "EnrichmentCategories");
        String[] lines = content.split("\n");

        Map<String, Map<String, Integer>> restypes = new HashMap<String, Map<String, Integer>>();

        for (String line : lines) {
            String[] tmp = line.split("\t");
            if (tmp.length < 3) {
                continue;
            }

            String categorystr = tmp[2];
            categorystr = categorystr.replaceAll("\\[", "");
            categorystr = categorystr.replaceAll("\\[", "");

            String[] types = categorystr.split(",");

            String id = tmp[0].trim();
            id = getId(mappings, id);

            Map<String, Integer> subrestypes = restypes.get(id);
            if (subrestypes == null) {
                subrestypes = new HashMap<String, Integer>();
                restypes.put(id, subrestypes);
            }

            for (String type : types) {
                String tmptype = type;
                tmptype = tmptype.replaceAll("\\[", "");
                tmptype = tmptype.replaceAll("\\]", "").trim();

                Integer value = subrestypes.get(tmptype);
                if (value == null) {
                    value = 0;
                }
                value++;
                subrestypes.put(tmptype, value);
            }
        }

        StringBuffer sb = new StringBuffer();
        for (String res : restypes.keySet()) {
            if (res.trim().isEmpty()) {
                continue;
            }

            Map<String, Integer> subrestypes = restypes.get(res);
            for (String key : subrestypes.keySet()) {
                if (key.trim().isEmpty()) {
                    continue;
                }

                sb.append(res);
                sb.append("\t");
                sb.append(key);
                sb.append("\t");
                sb.append(subrestypes.get(key));
                sb.append("\n");
            }
        }

        FileUtils.saveText(sb.toString(), outdir + "DatasetIndividualCategoryIndexSimple");
    }

    /*
     * Prints the different entities assigned to a specific dataset.
     */
    public static void printEntityAssociations(Map<String, String> mappings, String basedir, String outdir) {
        String content = FileUtils.readText(basedir + "ResourceEntityAssociations");
        String[] lines = content.split("\n");

        Map<String, Map<String, Integer>> restypes = new HashMap<String, Map<String, Integer>>();

        for (String line : lines) {
            String[] tmp = line.split("\t");
            if (tmp.length < 2) {
                continue;
            }

            String entitystr = tmp[1];
            entitystr = entitystr.replaceAll("\\[", "");
            entitystr = entitystr.replaceAll("\\[", "");

            String[] types = entitystr.split(",");

            String id = tmp[0].trim();
            id = getId(mappings, id);


            Map<String, Integer> subrestypes = restypes.get(id);
            if (subrestypes == null) {
                subrestypes = new HashMap<String, Integer>();
                restypes.put(id, subrestypes);
            }

            for (String type : types) {
                String tmptype = type;
                tmptype = tmptype.replaceAll("\\[", "");
                tmptype = tmptype.replaceAll("\\]", "").trim();

                Integer value = subrestypes.get(tmptype);
                if (value == null) {
                    value = 0;
                }
                value++;
                subrestypes.put(tmptype, value);
            }
        }

        StringBuffer sb = new StringBuffer();
        for (String res : restypes.keySet()) {
            if (res.trim().isEmpty()) {
                continue;
            }

            Map<String, Integer> subrestypes = restypes.get(res);
            sb.append("\n\n");
            sb.append(res);
            sb.append("\n");

            for (String key : subrestypes.keySet()) {
                if (key.trim().isEmpty()) {
                    continue;
                }
                sb.append(key + "\t" + subrestypes.get(key) + "\n");
            }
            sb.append("\n\n");
        }

        FileUtils.saveText(sb.toString(), outdir + "DatasetIndividualEntityIndex");
    }

    /*
     * Counts the number of different entity categories assigned to a specific dataset.
     */
    public static void countEntityCategories(Map<String, String> mappings, String basedir, String outdir) {
        String content = FileUtils.readText(basedir + "EnrichmentCategories");
        String[] lines = content.split("\n");

        Map<String, Map<String, Integer>> restypes = new HashMap<String, Map<String, Integer>>();

        for (String line : lines) {
            String[] tmp = line.split("\t");
            if (tmp.length < 3) {
                continue;
            }

            String categorystr = tmp[2];
            categorystr = categorystr.replaceAll("\\[", "");
            categorystr = categorystr.replaceAll("\\[", "");

            String[] types = categorystr.split(",");
            String id = tmp[0].trim();
            id = getId(mappings, id);

            Map<String, Integer> subrestypes = restypes.get(id);
            if (subrestypes == null) {
                subrestypes = new HashMap<String, Integer>();
                restypes.put(id, subrestypes);
            }

            for (String type : types) {
                Integer value = subrestypes.get(type);
                if (value == null) {
                    value = 0;
                }
                value++;
                subrestypes.put(type, value);
            }
        }

        StringBuffer sb = new StringBuffer();
        for (String res : restypes.keySet()) {
            sb.append(res + "\t" + restypes.get(res).size() + "\n");
        }

        FileUtils.saveText(sb.toString(), outdir + "DatasetCategoryIndex");
    }

    /*
     * Detects correlations based on the different entity categories assigned to a specific dataset with respect to other datasets.
     */
    public static void correlationEntityCategories(Map<String, String> mappings, String basedir, String outdir, int threshold) {
        String content = FileUtils.readText(basedir + "EnrichmentCategories");
        String[] lines = content.split("\n");

        Map<String, Map<String, Integer>> restypes = new HashMap<String, Map<String, Integer>>();

        for (String line : lines) {
            String[] tmp = line.split("\t");
            if (tmp.length < 3) {
                continue;
            }

            String categorystr = tmp[2];
            categorystr = categorystr.replaceAll("\\[", "");
            categorystr = categorystr.replaceAll("\\[", "");

            String[] types = categorystr.split(",");

            String id = tmp[0].trim();
            id = getId(mappings, id);

            Map<String, Integer> subrestypes = restypes.get(id);
            if (subrestypes == null) {
                subrestypes = new HashMap<String, Integer>();
                restypes.put(id, subrestypes);
            }

            for (String type : types) {
                Integer val = subrestypes.get(type);
                if (val == null) {
                    val = 0;
                }
                val++;
                subrestypes.put(type, val);
            }
        }

        StringBuffer sb = new StringBuffer();
        for (String res : restypes.keySet()) {
            Map<String, Integer> typecounts = restypes.get(res);

            sb.append("----------------");
            sb.append("Clusters for Dataset: " + res + "--------------\n");
            nextcomparison:
            for (String rescmp : restypes.keySet()) {
                if (res != rescmp) {
                    Map<String, Integer> typecountscmp = restypes.get(rescmp);
                    Set<String> tmp = new HashSet<String>(typecounts.keySet());
                    tmp.retainAll(typecountscmp.keySet());
                    if (tmp.size() > 0) {
                        for (String key : tmp) {
                            Integer val1 = typecountscmp.get(key);
                            Integer val2 = typecounts.get(key);

                            if (val1 > threshold && val2 > threshold) {
                                sb.append(rescmp);
                                sb.append("\n");
                                continue nextcomparison;
                            }
                        }
                    }
                }
            }
            sb.append("\n");
        }

        FileUtils.saveText(sb.toString(), outdir + "DatasetCategoryClustering");
    }

    /*
     * Counts the number of different entities assigned to a specific dataset.
     */
    public static void countEntityAssociations(Map<String, String> mappings, String basedir, String outdir) {
        String content = FileUtils.readText(basedir + "ResourceEntityAssociations");
        String[] lines = content.split("\n");

        Map<String, Map<String, Integer>> restypes = new HashMap<String, Map<String, Integer>>();

        for (String line : lines) {
            String[] tmp = line.split("\t");
            if (tmp.length < 2) {
                continue;
            }

            String entitystr = tmp[1];
            entitystr = entitystr.replaceAll("\\[", "");
            entitystr = entitystr.replaceAll("\\[", "");

            String[] types = entitystr.split(",");

            String id = tmp[0].trim();
            id = getId(mappings, id);


            Map<String, Integer> subrestypes = restypes.get(id);
            if (subrestypes == null) {
                subrestypes = new HashMap<String, Integer>();
                restypes.put(id, subrestypes);
            }

            for (String type : types) {
                Integer value = subrestypes.get(type);
                if (value == null) {
                    value = 0;
                }
                value++;
                subrestypes.put(type, value);
            }
        }

        StringBuffer sb = new StringBuffer();
        for (String res : restypes.keySet()) {
            sb.append(res + "\t" + restypes.get(res).size() + "\n");
        }

        FileUtils.saveText(sb.toString(), outdir + "DatasetEntityIndex");
    }

    /*
     * Detects correlations based on the different entities assigned to a specific dataset with respect to other datasets.
     */
    public static void correlationEntities(Map<String, String> mappings, String basedir, String outdir, int threshold) {
        String content = FileUtils.readText(basedir + "ResourceEntityAssociations");
        String[] lines = content.split("\n");

        Map<String, Map<String, Integer>> restypes = new HashMap<String, Map<String, Integer>>();

        for (String line : lines) {
            String[] tmp = line.split("\t");
            if (tmp.length < 2) {
                continue;
            }

            String entitystr = tmp[1];
            entitystr = entitystr.replaceAll("\\[", "");
            entitystr = entitystr.replaceAll("\\[", "");

            String[] types = entitystr.split(",");

            String id = tmp[0].trim();
            id = getId(mappings, id);

            Map<String, Integer> subrestypes = restypes.get(id);
            if (subrestypes == null) {
                subrestypes = new HashMap<String, Integer>();
                restypes.put(id, subrestypes);
            }

            for (String type : types) {
                Integer val = subrestypes.get(type);
                if (val == null) {
                    val = 0;
                }
                val++;
                subrestypes.put(type, val);
            }
        }

        StringBuffer sb = new StringBuffer();
        for (String res : restypes.keySet()) {
            Map<String, Integer> typecounts = restypes.get(res);

            sb.append("----------------");
            sb.append("Clusters for Dataset: " + res + "--------------\n");
            nextcomparison:
            for (String rescmp : restypes.keySet()) {
                if (res != rescmp) {
                    Map<String, Integer> typecountscmp = restypes.get(rescmp);
                    Set<String> tmp = new HashSet<String>(typecounts.keySet());
                    tmp.retainAll(typecountscmp.keySet());
                    if (tmp.size() > 0) {
                        for (String key : tmp) {
                            Integer val1 = typecountscmp.get(key);
                            Integer val2 = typecounts.get(key);

                            if (val1 > threshold && val2 > threshold) {
                                sb.append(rescmp);
                                sb.append("\n");
                                continue nextcomparison;
                            }
                        }
                    }
                }
            }
            sb.append("\n");
        }

        FileUtils.saveText(sb.toString(), outdir + "DatasetEntityClustering");
    }

    /**
     * Counts the total number of entities appearing in a set of resources. For
     * each resource we generate an index with the number of entity associations
     * (not distinct) for a resource, later this index will be used to compute
     * TF-IDF for the entities.
     *
     * @param entities
     * @param basedir
     */
    public static Map<String, Map<String, Integer>> countTotalEntityAssociations(String enrichdir, String basedir, String filter, Map<String, String> filemap, Map<String, String> filenamemapping) {
        //the file list of resources
        Set<String> files = new HashSet<String>();
        FileUtils.getFilesList(basedir, files);
        System.out.println(PrintUtils.printMap(filenamemapping));

        Map<String, Map<String, Integer>> entityoccurrences = new TreeMap<String, Map<String, Integer>>();
        for (String file : files) {
            //original resource file
            String fileid = file.substring(file.lastIndexOf("/") + 1).trim();
            String resource = filemap.get(fileid);
            if (!resource.startsWith(filter)) {
                continue;
            }

            if (!(resource.contains("lak2011") || resource.contains("lak2012"))) {
                continue;
            }

            String content = FileUtils.readText(file);
            String title = content.split("\n")[0].trim().toLowerCase();
            title = title.replaceAll("\\'", "");
            title = title.replaceAll(":", "");
            title = title.replaceAll("\\?", "");
            title = title.replaceAll("[\\–]", " ");
            title = title.replaceAll("[\\-]", " ");
            title = title.replaceAll("\\*", " ");
            title = title.replaceAll("\\.", "");
            title = title.replaceAll("\\,", "");
            title = title.replaceAll("\\’", "");
            title = title.replaceAll("\\”", "");
            title = title.replaceAll("\\“", "");
            title = title.replaceAll("\\&", "");
            title = title.replaceAll("reﬂective", "reflective");
            title = title.replaceAll("quantiﬁed", "quantified");
            title = title.replaceAll("open‐ended", "open ended");

            title = title.replaceAll("\\s{1,}", " ").trim();

            resource = filenamemapping.get(title);
            //enrichment resource files			
            if (!FileUtils.fileExists(enrichdir + "/" + fileid, true) || resource == null) {
                continue;
            }

            Set<String> enrichcontent = FileUtils.readIntoSet(enrichdir + "/" + fileid, "\n", false);
            Map<String, String> resourcentity = new TreeMap<String, String>();
            for (String line : enrichcontent) {
                String[] tmp = line.split("\t");
                resourcentity.put(tmp[0].toLowerCase(), tmp[1]);
            }

            //count the appearances of an entity within a resource
            for (String enrichvalue : resourcentity.keySet()) {
                String enrichentity = resourcentity.get(enrichvalue);
                int countentityoccurrence = StringUtils.countMatches(content, enrichvalue);

                Map<String, Integer> subentityoccurrences = entityoccurrences.get(enrichentity);
                if (subentityoccurrences == null) {
                    subentityoccurrences = new TreeMap<String, Integer>();
                    entityoccurrences.put(enrichentity, subentityoccurrences);
                }

                subentityoccurrences.put(resource, countentityoccurrence);
            }
        }

        return entityoccurrences;
    }

    /*
     * Computes the publication statistics.
     */
    public static void publicationStatistcs(String file, String outdir) {
        String content = FileUtils.readText(file);
        String lines[] = content.split("\n");

        Set<String> author = new HashSet<String>();
        Set<String> university = new HashSet<String>();
        Set<String> publication = new HashSet<String>();

        Map<String, Set<String>> authpub = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> authuni = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> unipub = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> pubuni = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> pubauthid = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> pubuniid = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> uniunicoll = new TreeMap<String, Set<String>>();

        for (String line : lines) {
            String[] tmp = line.split("\t");
            if (tmp.length < 4) {
                continue;
            }

            if (!author.contains(tmp[1].trim())) {
                author.add(tmp[1].trim());
            }
            if (!university.contains(tmp[2].trim())) {
                university.add(tmp[2].trim());
            }
            if (!publication.contains(tmp[3].trim())) {
                publication.add(tmp[3].trim());
            }

            //add the publication and its various authors.
            Set<String> subauthpub = authpub.get(tmp[1].trim());
            if (subauthpub == null) {
                subauthpub = new HashSet<String>();
                authpub.put(tmp[1].trim(), subauthpub);
            }
            subauthpub.add(tmp[3].trim());

            //add the publication and its various authors.
            Set<String> subauthuni = authuni.get(tmp[1].trim());
            if (subauthuni == null) {
                subauthuni = new HashSet<String>();
                authuni.put(tmp[2].trim(), subauthuni);
            }
            subauthuni.add(tmp[1].trim());

            //add the publication and its various authors.
            Set<String> subunipub = unipub.get(tmp[2].trim());
            if (subunipub == null) {
                subunipub = new HashSet<String>();
                unipub.put(tmp[2].trim(), subunipub);
            }
            subunipub.add(tmp[3].trim());

            //add the publication and its various authors.
            Set<String> subpubuni = pubuni.get(tmp[3].trim());
            if (subpubuni == null) {
                subpubuni = new HashSet<String>();
                pubuni.put(tmp[3].trim(), subpubuni);
            }
            subpubuni.add(tmp[2].trim());

            //add the publication and its various authors.
            Set<String> subpubauthid = pubauthid.get(tmp[0].trim());
            if (subpubauthid == null) {
                subpubauthid = new HashSet<String>();
                pubauthid.put(tmp[0].trim(), subpubauthid);
            }
            subpubauthid.add(tmp[1].trim());

            //add the publication and its various universities.
            Set<String> subpubuniid = pubuniid.get(tmp[0].trim());
            if (subpubuniid == null) {
                subpubuniid = new HashSet<String>();
                pubuniid.put(tmp[0].trim(), subpubuniid);
            }
            subpubuniid.add(tmp[2].trim());
        }

        //university collaborations 
        for (String unival : unipub.keySet()) {
            Set<String> subunipub = unipub.get(unival);

            Set<String> subuniunicoll = uniunicoll.get(unival);
            if (subuniunicoll == null) {
                subuniunicoll = new HashSet<String>();
                uniunicoll.put(unival, subuniunicoll);
            }

            //check the universities that are participating in this publication
            for (String pubval : subunipub) {
                subuniunicoll.addAll(pubuni.get(pubval));
            }

            subuniunicoll.remove(unival);
        }

        String authorstr = PrintUtils.printSet(author);
        String universitystr = PrintUtils.printSet(university);
        String publicationstr = PrintUtils.printSet(publication);

        String authorpubstr = PrintUtils.printSimpleMapSet(authpub);
        String authorunistr = PrintUtils.printSimpleMapSet(authuni);
        String unipubstr = PrintUtils.printSimpleMapSet(unipub);
        String uniunicollstr = PrintUtils.printSimpleMapSet(uniunicoll);

        String pubauthidstr = PrintUtils.printSimpleMapSet(pubauthid, true);
        String pubuniidstr = PrintUtils.printSimpleMapSet(pubuniid, true);

        FileUtils.saveText(authorstr, outdir + "/authors.txt");
        FileUtils.saveText(universitystr, outdir + "/universities.txt");
        FileUtils.saveText(publicationstr, outdir + "/publications.txt");

        FileUtils.saveText(authorpubstr, outdir + "/authorpublications.txt");
        FileUtils.saveText(authorunistr, outdir + "/authoruniversity.txt");
        FileUtils.saveText(unipubstr, outdir + "/universitypublication.txt");
        FileUtils.saveText(uniunicollstr, outdir + "/universitycollaborations.txt");
        FileUtils.saveText(pubauthidstr, outdir + "/publicationauthorid.txt");
        FileUtils.saveText(pubuniidstr, outdir + "/publicationuniversityid.txt");
    }

    /*
     * Computes the publication statistics.
     */
    public static void publicationStatistcs(String file, String outdir, Map<String, Set<String>> papertopiccoverage) {
        Set<String> lines = FileUtils.readIntoSet(file, "\n", false);

        Set<String> author = new HashSet<String>();
        Set<String> university = new HashSet<String>();
        Set<String> publication = new HashSet<String>();

        Map<String, Set<String>> authpub = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> authuni = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> unipub = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> pubuni = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> pubauthid = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> pubauth = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> pubuniid = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> uniunicoll = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> authtopic = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> authcollaboration = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> uniauth = new TreeMap<String, Set<String>>();

        for (String line : lines) {
            String[] tmp = line.split("\t");
            if (tmp.length < 4) {
                continue;
            }

            if (!author.contains(tmp[1].trim())) {
                author.add(tmp[1].trim());
            }
            if (!university.contains(tmp[2].trim())) {
                university.add(tmp[2].trim());
            }
            if (!publication.contains(tmp[3].trim())) {
                publication.add(tmp[3].trim());
            }

            //add the publication and its various authors.
            Set<String> subauthpub = authpub.get(tmp[1].trim());
            if (subauthpub == null) {
                subauthpub = new HashSet<String>();
                authpub.put(tmp[1].trim(), subauthpub);
            }
            subauthpub.add(tmp[3].trim());

            //add the publication and its various authors.
            Set<String> subauthuni = authuni.get(tmp[1].trim());
            if (subauthuni == null) {
                subauthuni = new HashSet<String>();
                authuni.put(tmp[1].trim(), subauthuni);
            }
            subauthuni.add(tmp[2].trim());

            //add the publication and its various authors.
            Set<String> subuniauth = uniauth.get(tmp[2].trim());
            if (subuniauth == null) {
                subuniauth = new HashSet<String>();
                uniauth.put(tmp[2].trim(), subuniauth);
            }
            subuniauth.add(tmp[1].trim());

            //add the publication and its various authors.
            Set<String> subunipub = unipub.get(tmp[2].trim());
            if (subunipub == null) {
                subunipub = new HashSet<String>();
                unipub.put(tmp[2].trim(), subunipub);
            }
            subunipub.add(tmp[3].trim());

            //add the publication and its various authors.
            Set<String> subpubuni = pubuni.get(tmp[3].trim());
            if (subpubuni == null) {
                subpubuni = new HashSet<String>();
                pubuni.put(tmp[3].trim(), subpubuni);
            }
            subpubuni.add(tmp[2].trim());

            //add the publication and its various authors.
            Set<String> subpubauthid = pubauthid.get(tmp[0].trim());
            if (subpubauthid == null) {
                subpubauthid = new HashSet<String>();
                pubauthid.put(tmp[0].trim(), subpubauthid);
            }
            subpubauthid.add(tmp[1].trim());

            //add the publication and its various authors.
            Set<String> subpubauth = pubauth.get(tmp[3].trim());
            if (subpubauth == null) {
                subpubauth = new HashSet<String>();
                pubauth.put(tmp[3].trim(), subpubauth);
            }
            subpubauth.add(tmp[1].trim());

            //add the publication and its various universities.
            Set<String> subpubuniid = pubuniid.get(tmp[0].trim());
            if (subpubuniid == null) {
                subpubuniid = new HashSet<String>();
                pubuniid.put(tmp[0].trim(), subpubuniid);
            }
            subpubuniid.add(tmp[2].trim());
        }

        //university collaborations 
        for (String unival : unipub.keySet()) {
            Set<String> subunipub = unipub.get(unival);

            Set<String> subuniunicoll = uniunicoll.get(unival);
            if (subuniunicoll == null) {
                subuniunicoll = new HashSet<String>();
                uniunicoll.put(unival, subuniunicoll);
            }

            //check the universities that are participating in this publication
            for (String pubval : subunipub) {
                subuniunicoll.addAll(pubuni.get(pubval));
            }

            subuniunicoll.remove(unival);
        }

        //assign topic coverage by authors
        for (String auth : authpub.keySet()) {
            Set<String> subauthpub = authpub.get(auth);

            Set<String> subauthtopics = authtopic.get(auth);
            if (subauthtopics == null) {
                subauthtopics = new HashSet<String>();
                authtopic.put(auth, subauthtopics);
            }

            for (String pub : subauthpub) {
                String pubtmp = pub.replaceAll("[^\\p{L}\\p{N}]", "");
                pubtmp = pubtmp.replaceAll("\\s{1,}", " ");

                for (String pubcmp : papertopiccoverage.keySet()) {
                    String pubcmptmp = pubcmp.replaceAll("[^\\p{L}\\p{N}]", "");
                    pubcmptmp = pubcmptmp.replaceAll("\\s{1,}", " ");
                    if (pubtmp.trim().toLowerCase().equals(pubcmptmp.trim().toLowerCase())) {
                        subauthtopics.addAll(papertopiccoverage.get(pubcmp));
                        break;
                    }
                }
            }
        }

        //assign topic coverage by authors
        for (String auth : authpub.keySet()) {
            Set<String> subauthpub = authpub.get(auth);

            Set<String> subauthcollaborations = authcollaboration.get(auth);
            if (subauthcollaborations == null) {
                subauthcollaborations = new HashSet<String>();
                authcollaboration.put(auth, subauthcollaborations);
            }

            for (String pub : subauthpub) {
                Set<String> subpubauthors = pubauth.get(pub);
                if (subpubauthors != null) {
                    subauthcollaborations.addAll(subpubauthors);
                }
            }
        }

        Map<String, Set<String>> authorclusters = analyzeClusters(authcollaboration);

        //get publications and authors for a cluster;
        Map<String, Set<String>> authorclusterpubs = new TreeMap<String, Set<String>>();
        Map<String, Set<String>> authorclusteraff = new TreeMap<String, Set<String>>();

        for (String clusterid : authorclusters.keySet()) {
            Set<String> subauthorclusters = authorclusters.get(clusterid);

            Set<String> subauthorclusteraff = authorclusteraff.get(clusterid);
            Set<String> subauthorclusterpubs = authorclusterpubs.get(clusterid);

            subauthorclusteraff = subauthorclusteraff == null ? new HashSet<String>() : subauthorclusteraff;
            subauthorclusterpubs = subauthorclusterpubs == null ? new HashSet<String>() : subauthorclusterpubs;

            authorclusteraff.put(clusterid, subauthorclusteraff);
            authorclusterpubs.put(clusterid, subauthorclusterpubs);

            for (String authorkey : subauthorclusters) {
                if (authpub.get(authorkey) != null) {
                    subauthorclusterpubs.addAll(authpub.get(authorkey));
                }
                if (authuni.get(authorkey) != null) {
                    subauthorclusteraff.addAll(authuni.get(authorkey));
                }
            }
        }

        //set the geo map

        Map<String, String> unilocations = FileUtils.readIntoStringMap("../../Data/lak-dataset-challenge/UniversityLocations", "\t", false);
        Map<String, Integer> unigeolocationimpact = new TreeMap<String, Integer>();

        for (String unikey : uniauth.keySet()) {
            Set<String> subuniauth = uniauth.get(unikey);
            Set<String> subunipub = unipub.get(unikey);

            String location = getUniversityLocation(unikey, unilocations);
            Integer value = unigeolocationimpact.get(location);
            if (value == null) {
                value = 0;
            }

            value += subuniauth == null ? 0 : subuniauth.size();
            value += subunipub == null ? 0 : subunipub.size();

            unigeolocationimpact.put(location, value);
        }
        StringBuffer sb1 = new StringBuffer();
        sb1.append("['University',\t'Impact'],\n");
        for (String unilocation : unigeolocationimpact.keySet()) {
            sb1.append("['" + unilocation + "', " + unigeolocationimpact.get(unilocation) + "],\n");
        }
        FileUtils.saveText(sb1.toString(), outdir + "/unigeolocationimpact.txt");



        StringBuffer sb = new StringBuffer();
        int clusteridno = 1;
        sb.append("['ClusterID',\t'Authors', \t'Affiliation',\t'Publications'],\n");
        for (String clusterid : authorclusters.keySet()) {
            Set<String> subauthorclusters = authorclusters.get(clusterid);
            Set<String> subauthorclusteraff = authorclusteraff.get(clusterid);
            Set<String> subauthorclusterpubs = authorclusterpubs.get(clusterid);
            sb.append("['" + clusteridno + "',\t" + subauthorclusters.size() + ",\t" + subauthorclusteraff.size() + ",\t" + subauthorclusterpubs.size() + "],\n");
            clusteridno++;
        }

        FileUtils.saveText(sb.toString(), outdir + "/clusterdetails.txt");

        String authorclustersstr = PrintUtils.printSimpleMapSet(authorclusters);
        FileUtils.saveText(TextUtils.removeSpecialSymbols(authorclustersstr), outdir + "/authorclusters.txt");

        String authorstr = PrintUtils.printSet(author);
        String universitystr = PrintUtils.printSet(university);
        String publicationstr = PrintUtils.printSet(publication);

        String authorpubstr = PrintUtils.printSimpleMapSet(authpub);
        String authorpubintstr = PrintUtils.printSimpleMapSetInt(authpub);
        String authorcollabstr = PrintUtils.printSimpleMapSetInt(authcollaboration);

        String authorunistr = PrintUtils.printSimpleMapSet(authuni);
        String unipubstr = PrintUtils.printSimpleMapSet(unipub);
        String uniunicollstr = PrintUtils.printSimpleMapSet(uniunicoll);

        String pubauthidstr = PrintUtils.printSimpleMapSet(pubauthid, true);
        String pubuniidstr = PrintUtils.printSimpleMapSet(pubuniid, true);

        String topicpubstr = PrintUtils.printSimpleMapSetInt(papertopiccoverage);
        String topicauthstr = PrintUtils.printSimpleMapSetInt(authtopic);

        FileUtils.saveText(TextUtils.removeSpecialSymbols(authorstr), outdir + "/authors.txt");
        FileUtils.saveText(TextUtils.removeSpecialSymbols(universitystr), outdir + "/universities.txt");
        FileUtils.saveText(TextUtils.removeSpecialSymbols(publicationstr), outdir + "/publications.txt");

        FileUtils.saveText(TextUtils.removeSpecialSymbols(topicpubstr), outdir + "/publicationtopics.txt");
        FileUtils.saveText(TextUtils.removeSpecialSymbols(topicauthstr), outdir + "/authortopics.txt");

        FileUtils.saveText(TextUtils.removeSpecialSymbols(authorpubstr), outdir + "/authorpublications.txt");
        FileUtils.saveText(TextUtils.removeSpecialSymbols(authorpubintstr), outdir + "/authorpublicationssimple.txt");
        FileUtils.saveText(TextUtils.removeSpecialSymbols(authorcollabstr), outdir + "/authorcollaborationsimple.txt");

        FileUtils.saveText(TextUtils.removeSpecialSymbols(authorunistr), outdir + "/authoruniversity.txt");
        FileUtils.saveText(TextUtils.removeSpecialSymbols(unipubstr), outdir + "/universitypublication.txt");
        FileUtils.saveText(TextUtils.removeSpecialSymbols(uniunicollstr), outdir + "/universitycollaborations.txt");
        FileUtils.saveText(TextUtils.removeSpecialSymbols(pubauthidstr), outdir + "/publicationauthorid.txt");
        FileUtils.saveText(TextUtils.removeSpecialSymbols(pubuniidstr), outdir + "/publicationuniversityid.txt");
    }

    private static String getUniversityLocation(String uni, Map<String, String> unilocations) {
        String tmp = uni;
        if (tmp.contains(",")) {
            tmp = tmp.substring(0, tmp.indexOf(","));
        }

        for (String unicmp : unilocations.keySet()) {
            if (unicmp.contains(tmp.trim())) {
                return unilocations.get(unicmp).trim();
            }
        }
        return uni;
    }

    /*
     * Creates author clusters based on their paper collaborations.
     */
    public static Map<String, Set<String>> analyzeClusters(Map<String, Set<String>> authcollaboration) {
        Map<String, Set<String>> authclusters = new TreeMap<String, Set<String>>();

        for (String auth : authcollaboration.keySet()) {
            if (isAuthorClusterMember(auth, authclusters)) {
                continue;
            }

            Set<String> subauthclusters = authclusters.get(auth);
            if (subauthclusters == null) {
                subauthclusters = new HashSet<String>();
                authclusters.put(auth, subauthclusters);
            }

            Set<String> subauthcollaboration = authcollaboration.get(auth);
            for (String authtmp : subauthcollaboration) {
                if (!isAuthorClusterMember(authtmp, authclusters)) {
                    subauthclusters.add(authtmp);
                }
            }

            //check other author's connections to group the clusters.
            for (String authcmp : authcollaboration.keySet()) {
                if (authcmp.equals(auth) || !subauthclusters.contains(authcmp)) {
                    continue;
                }

                Set<String> subauthcollaborationcmp = authcollaboration.get(authcmp);
                for (String authtmp : subauthcollaborationcmp) {
                    if (!isAuthorClusterMember(authtmp, authclusters)) {
                        subauthclusters.add(authtmp);
                    }
                }
            }
        }

        Map<String, Set<String>> filteredauthclusters = new TreeMap<String, Set<String>>();
        nextauth:
        for (String auth : authclusters.keySet()) {
            Set<String> subauthclusters = authclusters.get(auth);
            if (subauthclusters == null || subauthclusters.size() == 0) {
                for (String authcmp : authclusters.keySet()) {
                    Set<String> subauthclusterscmp = authclusters.get(authcmp);
                    if (subauthclusterscmp.contains(auth)) {
                        continue nextauth;
                    }
                }
                subauthclusters.add(auth);
                filteredauthclusters.put(subauthclusters.hashCode() + "", subauthclusters);
            } else {
                subauthclusters.add(auth);
                filteredauthclusters.put(subauthclusters.hashCode() + "", subauthclusters);
            }
        }

        return filteredauthclusters;
    }

    /*
     * Check whether an item in this case an author is part of a cluster.
     */
    private static boolean isAuthorClusterMember(String auth, Map<String, Set<String>> authclusters) {
        if (authclusters.containsKey(auth)) {
            return true;
        }

        int count = 0;

        for (String authcmp : authclusters.keySet()) {
            Set<String> subauthclusters = authclusters.get(authcmp);
            if (subauthclusters.contains(auth)) {
                count++;
            }
        }

        return count != 0;
    }
    /*
     * Merge multiple enrichment source files for the same resource into one.
     */

    public static void mergeEnrichmentFiles(String enrichdir, String outdir) {
        Set<String> files = new HashSet<String>();
        FileUtils.getFilesList(enrichdir, files, "VALID");

        FileUtils.checkDir(outdir);
        for (String file : files) {
            String fileid = file.substring(file.lastIndexOf("/") + 1, file.lastIndexOf("_"));

            Map<String, String> enrichresource = new TreeMap<String, String>();
            if (FileUtils.fileExists(outdir + fileid, false)) {
                Set<String> contenttmp = FileUtils.readIntoSet(outdir + fileid, "\n", false);

                for (String line : contenttmp) {
                    if (line.isEmpty() || line.startsWith("#")) {
                        continue;
                    }

                    String[] tmp = line.split("\t");
                    enrichresource.put(tmp[0].trim(), tmp[1].trim());
                }
            }

            Set<String> content = FileUtils.readIntoSet(file, "\n", false);
            for (String line : content) {
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] tmp = line.split("\t");
                if (enrichresource.containsKey(tmp[0].trim())) {
                    continue;
                }

                enrichresource.put(tmp[0].trim(), tmp[1].trim());
            }

            FileUtils.saveText(PrintUtils.printMap(enrichresource), outdir + fileid);
        }
    }

    /*
     * Merge multiple enrichement source files for the same resource into one.
     */
    public static void mergeResourceFiles(String enrichdir, String outdir) {
        Set<String> files = new HashSet<String>();
        FileUtils.getFilesList(enrichdir, files);

        FileUtils.checkDir(outdir);
        for (String file : files) {
            String fileid = file.substring(file.lastIndexOf("/") + 1);
            FileUtils.saveText(FileUtils.readText(file), outdir + fileid, true);
        }
    }

    /*
     * Merge multiple files into a single file.
     */
    public static void mergeFiles(String enrichdir, String outfile) {
        Set<String> files = new HashSet<String>();
        FileUtils.getFilesList(enrichdir, files);

        for (String file : files) {
            FileUtils.saveText(FileUtils.readText(file), outfile, true);
        }
    }

    /*
     * Pre processes a file of form key value, and merges into a value [key1, key2] file.
     */
    public static void preprocessFileIntoMap(String file, String outfile) {
        Map<String, Set<String>> rst = new TreeMap<String, Set<String>>();

        Set<String> content = FileUtils.readIntoSet(file, "\n", false);
        for (String line : content) {
            String[] data = line.split("\t");

            Set<String> subrst = rst.get(data[1].trim());
            if (subrst == null) {
                subrst = new HashSet<String>();
                rst.put(data[1].trim(), subrst);
            }

            String tmp = data[0];
            if (tmp.contains("@")) {
                tmp = tmp.substring(0, tmp.indexOf("@"));
            }

            if (tmp.equals("null")) {
                tmp = data[1].substring(data[1].lastIndexOf("/") + 1).trim();
                tmp = tmp.replaceAll("%28", "(");
                tmp = tmp.replaceAll("%29", ")");
                tmp = tmp.replaceAll("%27", "'");
                tmp = tmp.replaceAll("_", " ");
            }
            subrst.add(tmp.trim());
        }

        FileUtils.saveText(PrintUtils.printSimpleMapSet(rst), outfile);
    }

    /*
     * Gets the title or the first line of each resources, and stores into a map datastructure with its file id.
     */
    public static Map<String, String> getPaperTitles(String resourcedir, Map<String, String> filemap, String filter) {
        Map<String, String> rst = new TreeMap<String, String>();

        Set<String> files = new HashSet<String>();
        FileUtils.getFilesList(resourcedir, files);

        for (String file : files) {
            String fileid = file.substring(file.lastIndexOf("/") + 1);
            String[] content = FileUtils.readText(file).split("\n");

            String resid = filemap.get(fileid);
            if (!resid.contains(filter)) {
                continue;
            }

            rst.put(filemap.get(fileid), content[0]);
        }

        return rst;
    }

    /*
     * Reads the topics covered by each of the resources in this case papers, and quantifies for each paper the topics covered.
     */
    public static Map<String, Set<String>> quantifyPaperTopicCoverage(Map<String, String> papertitles, String topicspath) {
        Map<String, Set<String>> papertopiccoverage = new TreeMap<String, Set<String>>();
        Set<String> content = FileUtils.readIntoSet(topicspath, "\n", false);

        for (String line : content) {
            if (line.isEmpty()) {
                continue;
            }
            String[] data = line.split("\t");

            if (papertitles.get(data[0].trim()) == null) {
                continue;
            }

            Set<String> subpapertopiccoverage = papertopiccoverage.get(papertitles.get(data[0].trim()));
            if (subpapertopiccoverage == null) {
                subpapertopiccoverage = new HashSet<String>();
                papertopiccoverage.put(papertitles.get(data[0].trim()), subpapertopiccoverage);
            }
            subpapertopiccoverage.add(data[1].trim());
        }
        return papertopiccoverage;
    }

    /*
     * Reads the topics covered by each of the resources in this case papers, and quantifies for each paper the topics covered.
     */
    public static Map<String, List<Entry<String, Integer>>> quantifyPaperTopicCoverageRatio(Map<String, String> papertitles, String topicspath, int threshold) {
        Map<String, List<Entry<String, Integer>>> papertopiccoverage = new TreeMap<String, List<Entry<String, Integer>>>();
        Set<String> content = FileUtils.readIntoSet(topicspath, "\n", false);

        for (String line : content) {
            if (line.isEmpty()) {
                continue;
            }
            String[] data = line.split("\t");

            if (papertitles.get(data[0].trim()) == null) {
                continue;
            }

            List<Entry<String, Integer>> subpapertopiccoverage = papertopiccoverage.get(papertitles.get(data[0].trim()));
            if (subpapertopiccoverage == null) {
                subpapertopiccoverage = new ArrayList<Map.Entry<String, Integer>>();
                papertopiccoverage.put(papertitles.get(data[0].trim()), subpapertopiccoverage);
            }

            String topic = data[1].trim();
            if (topic.contains(":")) {
                topic = topic.substring(topic.lastIndexOf(":") + 1).trim();
            }
            topic = topic.replaceAll("\\_", " ").trim();

            Integer ratio = Integer.parseInt(data[2].trim());

            if (ratio < threshold) {
                continue;
            }

            subpapertopiccoverage.add(new AbstractMap.SimpleEntry<String, Integer>(topic, ratio));
        }
        return papertopiccoverage;
    }

    /**
     * Compute the sum from a set collection of double.
     *
     * @param values
     * @return
     */
    public static double getDoubleSummation(Collection<Double> values) {
        double sum = 0;
        for (double value : values) {
            sum += value;
        }

        return sum;
    }

    /**
     * Compute the sum from a set collection of double.
     *
     * @param values
     * @return
     */
    public static double getDoubleSummation(double[] values) {
        double sum = 0;
        for (int i = 0; i < values.length; i++) {
            sum += values[i];
        }

        return sum;
    }

    /**
     * Gets the mean value from a collection of doubles.
     *
     * @param values
     * @return
     */
    public static double getDoubleMean(Collection<Double> values) {
        if (values == null) {
            return 0;
        }
        double sum = getDoubleSummation(values);
        if (sum == 0 || values.isEmpty()) {
            return 0;
        }
        return sum / values.size();
    }

    /**
     * Gets the mean value from a collection of doubles.
     *
     * @param values
     * @return
     */
    public static double getDoubleMean(double[] values) {
        double sum = getDoubleSummation(values);
        if (sum == 0 || values.length == 0) {
            return 0;
        }
        return sum / values.length;
    }

    /**
     * Compute the sum from a set collection of double.
     *
     * @param values
     * @return
     */
    public static int getIntSummation(Collection<Integer> values) {
        int sum = 0;
        for (int value : values) {
            sum += value;
        }

        return sum;
    }

    /**
     * Gets the mean value from a collection of doubles.
     *
     * @param values
     * @return
     */
    public static double getIntMean(Collection<Integer> values) {
        double sum = getIntSummation(values);
        if (sum == 0 || values.isEmpty()) {
            return 0;
        }
        return sum / values.size();
    }

    /**
     * Computes the standard deviation of a double array.
     *
     * @param values
     * @return
     */
    public static double getDoubleStandardDeviation(Collection<Double> values) {
        double mean = getDoubleMean(values);
        double std_dev = 0;

        for (double value : values) {
            std_dev += Math.pow(value - mean, 2);
        }

        std_dev /= values.size();


        return Math.sqrt(std_dev);
    }

    /**
     * Computes the pairwise difference of weights from the same document in
     * different runs.
     *
     * @param weights
     * @return
     */
    public static double getPairwiseDifference(List<Double> weights) {
        double value = 0;

        for (int i = 0; i < weights.size(); i++) {
            for (int j = i + 1; j < weights.size(); j++) {
                value += Math.abs(weights.get(i) - weights.get(j));
            }
        }

        return value;
    }

    public static boolean getPairedSignificance(String query, String run, double[][] ip_a, double[][] ip_b) {

        double[] partial_ip_values = new double[ip_a.length];
        double[] approximated_ip_values = new double[ip_b.length];

        for (int i = 0; i < partial_ip_values.length; i++) {
            partial_ip_values[i] = ip_a[i][1];
            approximated_ip_values[i] = ip_b[i][1];
        }

        TTest t_test = new TTest();
        double t_test_value = t_test.pairedTTest(partial_ip_values, approximated_ip_values);
        //print the descriptive statistics.
        System.out.println("query=" + query + "\trun=" + run + "\tt-test=" + t_test_value);

        //if the test statistic is greater than the p < 0.005 then we can accept the hypothesis that the metrics are statistically equivalent
        if (!Double.isNaN(t_test_value) && t_test_value > 0.05) {
           return true;
        } return false;
    }
}
