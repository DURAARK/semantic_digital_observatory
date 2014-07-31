package utils;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DBUtils {

    public static int getNYTArticleCount(String host) {
        int rst = 0;
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            String dbURL = "jdbc:mysql://" + host + ":3306/nyt";
            String username = null;
            String password = null;

            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(dbURL, username, password);
            stmt = conn.createStatement();

            if (stmt.execute("SELECT COUNT(ID) AS cntarticles FROM article")) {
                rs = stmt.getResultSet();
            } else {
                System.err.println("select failed");
            }

            if (rs.next()) {
                return rs.getInt("cntarticles");
            }
        } catch (ClassNotFoundException ex) {
            System.err.println("Failed to load mysql driver");
            System.err.println(ex);
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) { /* ignore */ }
                rs = null;
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) { /* ignore */ }
                stmt = null;
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) { /* ignore */ }
                conn = null;
            }
        }

        return rst;
    }

    /**
     * Reads the NYT articles from the MySQL database, which are as well
     * processed.
     *
     * @return
     */
    public static Map<String, Entry<String, Object>> readNYTDocuments(String host, String user, String password) {
        Connection conn = getMySQLConnection(host, "nyt", user, password);
        Map<String, Entry<String, Object>> rst = new TreeMap<String, Map.Entry<String, Object>>();

        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();

            if (stmt.execute("SELECT a.id, a.body, at.article_obj FROM article a, article_annotated at WHERE a.ID = at.ID LIMIT 100")) {
                rs = stmt.getResultSet();
            } else {
                System.err.println("select failed");
            }

            while (rs.next()) {
                String id = rs.getString("id");
                String body = rs.getString("body");

                byte[] blob_bytes = rs.getBytes("article_obj");
                ByteArrayInputStream bais = new ByteArrayInputStream(blob_bytes);
                GZIPInputStream gzipIn = new GZIPInputStream(bais);
                ObjectInputStream objin = new ObjectInputStream(gzipIn);

                Object article_obj = objin.readObject();
                Entry<String, Object> entry = new AbstractMap.SimpleEntry<String, Object>(body, article_obj);
                rst.put(id, entry);
            }
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) { /* ignore */ }
                rs = null;
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) { /* ignore */ }
                stmt = null;
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) { /* ignore */ }
                conn = null;
            }
        }

        return rst;
    }

    public static Connection getMySQLConnection(String host, String database) {
        Connection conn = null;
        String dbURL = "jdbc:mysql://" + host + ":3306/" + database;
        String username = null;
        String password = null;


        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(dbURL, username, password);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return conn;
    }

    public static Connection getMySQLConnection(String host, String database, String username, String password) {
        Connection conn = null;
        String dbURL = "jdbc:mysql://" + host + ":3306/" + database;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(dbURL, username, password);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return conn;
    }

    /**
     * Reads the NYT articles from the database in a data-structure as
     * article_id, article_body.
     *
     * @return
     */
    public static Map<String, String> readNYTArticles(int offset, int limit, String host, String database) {
        Map<String, String> rst = new TreeMap<String, String>();

        Connection conn = getMySQLConnection(host, database);
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();

            if (stmt.execute("SELECT id, body FROM article LIMIT " + limit + " OFFSET " + offset)) {
                rs = stmt.getResultSet();
            } else {
                System.err.println("select failed");
            }

            while (rs.next()) {
                String id = rs.getString("id");
                String body = rs.getString("body");
                rst.put(id, body);
            }
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) { /* ignore */ }
                rs = null;
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) { /* ignore */ }
                stmt = null;
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) { /* ignore */ }
                conn = null;
            }
        }

        return rst;
    }

    /**
     * Reads the NYT articles from the database in a data-structure as
     * article_id, article_body.
     *
     * @return
     */
    public static Map<String, String> readNYTArticlesID(String host, int yearbegin, int yearend, String database) {
        Map<String, String> rst = new TreeMap<String, String>();

        Connection conn = getMySQLConnection(host, database);
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();

            if (stmt.execute("SELECT id, publicationYear FROM article WHERE publicationYear BETWEEN " + yearbegin + " AND " + yearend)) {
                rs = stmt.getResultSet();
            } else {
                System.err.println("select failed");
            }

            while (rs.next()) {
                String id = rs.getString("id");
                String body = rs.getString("publicationYear");
                rst.put(id, body);
            }
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) { /* ignore */ }
                rs = null;
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) { /* ignore */ }
                stmt = null;
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) { /* ignore */ }
                conn = null;
            }
        }

        return rst;
    }

    public static void writeProcessedNYTArticles(Map<String, String> posarticles, Map<String, String> jsonarticles, Map<String, Object> objarticles, String host, String database) {
        Connection conn = getMySQLConnection(host, database);
        PreparedStatement pst = null;

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute("LOCK TABLES article_annotated WRITE");

            pst = conn.prepareStatement("INSERT INTO article_annotated(id, body_pos,article_obj) VALUES(?,?,?)");

            for (String articleid : posarticles.keySet()) {
                String posarticle = posarticles.get(articleid);
                //	String jsonarticle = jsonarticles.get(articleid);
                Object objarticle = objarticles.get(articleid);

                //zip object for space efficiency
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
                BufferedOutputStream bfout = new BufferedOutputStream(gzipOut);

                ObjectOutputStream objectOut = new ObjectOutputStream(bfout);
                objectOut.writeObject(objarticle);
                objectOut.close();

                byte[] objasBytes = baos.toByteArray();

                pst.setString(1, articleid);
                pst.setString(2, posarticle);
                //pst.setString(3, jsonarticle);
                pst.setBytes(3, objasBytes);

                pst.addBatch();
            }

            pst.executeBatch();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                stmt.execute("UNLOCK TABLES");
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    public static void writeEntityNYTArticles(Map<String, Set<Entry<String, String>>> entityarticles, String host, String database) {
        Connection conn = getMySQLConnection(host, database);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("INSERT INTO article_entities(articleid,entity,entity_type) VALUES(?,?,?)");

            for (String articleid : entityarticles.keySet()) {
                Set<Entry<String, String>> entities = entityarticles.get(articleid);

                for (Entry<String, String> entity : entities) {
                    pst.setString(1, articleid);
                    pst.setString(2, entity.getKey());
                    pst.setString(3, entity.getValue());

                    pst.addBatch();
                }
            }

            pst.executeBatch();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    //===========================================================================================================================================================
    //===========================================================================================================================================================
    // Methods for writting into the MySQL database of LOD  Cloud data
    //===========================================================================================================================================================
    //===========================================================================================================================================================

    /**
     * Writes the dataset details into mysql schema
     *
     * @param dataset_name
     * @param dataset_notes
     * @param dataset_uri
     */
    public static int writeDataset(String dataset_name, String dataset_notes, String dataset_uri, String mysql_host, String database) {
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("INSERT INTO dataset(name,description,url) VALUES(?,?,?)");

            pst.setString(1, dataset_name);
            pst.setString(2, dataset_notes);
            pst.setString(3, dataset_uri);

            return pst.executeUpdate();


        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }

        return -1;
    }

    /**
     * Writes the resource types from the individual datasets.
     *
     * @param resource_types
     * @param dataset_id
     */
    public static void writeDatasetResourceTypes(Set<String> resource_types, Map<String, Set<String>> resource_type_properties, int dataset_id, String mysql_host, String database) {
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("INSERT INTO dataset_resource_types(dataset_id,resource_type_id,resource_type_uri) VALUES(?,?,?)");
            int resource_type_id = 1;
            for (String resource_type : resource_types) {
                pst.setInt(1, dataset_id);
                pst.setInt(2, resource_type_id);
                pst.setString(3, resource_type);

                //write the resource properties
                if (resource_type_properties.containsKey(resource_type)) {
                    writeDatasetResourceTypeProperties(resource_type_id, dataset_id, resource_type_properties.get(resource_type), mysql_host, database);
                }

                resource_type_id++;
                pst.addBatch();
            }

            pst.executeBatch();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    /**
     * Writes for each resource type the set of properties associated with it.
     *
     * @param resource_type_id
     * @param dataset_id
     * @param datatype_properties
     */
    private static void writeDatasetResourceTypeProperties(int resource_type_id, int dataset_id, Set<String> datatype_properties, String mysql_host, String database) {
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("INSERT INTO dataset_resource_type_properties(resource_type_id,resource_property_id,property_url,dataset_id) VALUES(?,?,?,?)");

            int datatype_property_id = 1;
            for (String datatype_property : datatype_properties) {
                pst.setInt(1, resource_type_id);
                pst.setInt(2, datatype_property_id);
                pst.setString(3, datatype_property);
                pst.setInt(4, dataset_id);
                datatype_property_id++;

                pst.addBatch();
            }
            pst.executeBatch();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    /**
     * Write for each dataset the set of resources.
     *
     * @param resource_uris
     * @param dataset_id
     */
    public static void writeDatasetResources(Map<String, String> resource_uris, Map<String, List<Entry<String, String>>> resource_values, int dataset_id, String mysql_host, String database) {
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("INSERT INTO dataset_resources(resource_id,dataset_id,resource_type_id,resource_uri) VALUES(?,?,?,?)");

            int resource_id = 1;
            for (String resource_uri : resource_uris.keySet()) {
                pst.setInt(1, resource_id);
                pst.setInt(2, dataset_id);
                pst.setInt(3, getResourceTypeId(resource_uri, dataset_id, mysql_host, database));
                pst.setString(4, resource_uri);

                if (resource_values.get(resource_uri) != null && !resource_values.get(resource_uri).isEmpty()) {
                    writeResourceValues(resource_values.get(resource_uri), resource_id, dataset_id, mysql_host, database);
                }

                resource_id++;
                pst.addBatch();
            }
            pst.executeBatch();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    /**
     * Writes the individual datatype property values for each resource.
     *
     * @param resource_values
     * @param resource_id
     * @param dataset_id
     */
    private static void writeResourceValues(List<Entry<String, String>> resource_values, int resource_id, int dataset_id, String mysql_host, String database) {
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("INSERT INTO dataset_resource_values(resource_id,dataset_id,property_uri,property_value) VALUES(?,?,?,?)");

            for (Entry<String, String> resource_value : resource_values) {
                pst.setInt(1, resource_id);
                pst.setInt(2, dataset_id);
                pst.setString(3, resource_value.getKey());
                pst.setString(4, resource_value.getKey());

                pst.addBatch();
            }
            pst.executeBatch();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    /**
     * Writes the set of entities from the AnnotationIndex.
     *
     * @param entities
     */
    public static void writeEntities(Set<String> entities, String mysql_host, String database) {
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("INSERT INTO entities(entity_uri) VALUES(?)");

            for (String entity : entities) {
                pst.setString(1, entity);
                pst.addBatch();
            }
            pst.executeBatch();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    /**
     * Writes the set of all categories associated with the entities.
     *
     * @param categories
     */
    public static void writeCategories(Set<Entry<String, String>> categories, String mysql_host, String database) {
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("INSERT INTO categories(category_uri,parent_category_uri) VALUES(?,?)");

            for (Entry<String, String> entry : categories) {
                pst.setString(1, entry.getKey());
                pst.setString(2, entry.getValue());
                pst.addBatch();
            }
            pst.executeBatch();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    /**
     * Add the entity category associations.
     *
     * @param entity_categories
     */
    public static void writeCategoryEntities(Map<String, List<Entry<String, Integer>>> entity_categories, String mysql_host, String database) {
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            Map<String, Integer> entity_keys = getEntityKeys(mysql_host, database);
            Map<String, Integer> category_keys = getCategoryKeys(mysql_host, database);

            pst = conn.prepareStatement("INSERT INTO entity_categories(entity_id,category_id,level) VALUES(?,?,?)");

            for (String entity : entity_categories.keySet()) {
                int entity_id = entity_keys.get(entity);

                for (Entry<String, Integer> category : entity_categories.get(entity)) {
                    int category_id = category_keys.get(category.getKey());
                    pst.setInt(1, entity_id);
                    pst.setInt(2, category_id);
                    pst.setInt(3, category.getValue());
                    pst.addBatch();
                }
            }
            pst.executeBatch();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    /**
     * Adds the entities associated with a resource.
     *
     * @param resource_uri
     * @param dataset_id
     * @param entities
     */
    public static void addEntityResource(String resource_uri, int dataset_id, Set<String> entities, String mysql_host, String database) {
        int resource_id = getResourceTypeId(resource_uri, dataset_id, mysql_host, database);
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;
        try {
            pst = conn.prepareStatement("INSERT INTO resource_entities(resource_id,dataset_id,entity_id) VALUES(?,?,?)");

            for (String entity : entities) {
                int entity_id = getEntityID(entity, mysql_host, database);
                pst.setInt(1, resource_id);
                pst.setInt(2, dataset_id);
                pst.setInt(3, entity_id);
                pst.addBatch();
            }

            pst.executeBatch();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    /**
     * Gets the resouce type id for a dataset.
     *
     * @param resource_type_uri
     * @param dataset_id
     * @return
     */
    private static int getResourceTypeId(String resource_type_uri, int dataset_id, String mysql_host, String database) {
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("SELECT resource_type_id FROM dataset_resource_types WHERE resoruce_type_uri = ? AND dataset_id = ?");
            pst.setString(1, resource_type_uri);
            pst.setInt(2, dataset_id);

            ResultSet rst = pst.executeQuery();
            if (rst.next()) {
                return rst.getInt("resource_type_id");
            }

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }

        return -1;
    }

    /**
     * Get the entity id based on its uri
     *
     * @param entity_uri
     * @return
     */
    private static int getEntityID(String entity_uri, String mysql_host, String database) {
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("SELECT entity_id FROM entities WHERE entity_uri = ?");
            pst.setString(1, entity_uri);

            ResultSet rst = pst.executeQuery();
            if (rst.next()) {
                return rst.getInt("entity_id");
            }

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }

        return -1;
    }

    /**
     * get the category id based on its uri\F
     *
     * @param category_uri
     * @return
     */
    private static int getCategoryID(String category_uri, String mysql_host, String database) {
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("SELECT category_id FROM categories WHERE category_uri = ?");
            pst.setString(1, category_uri);

            ResultSet rst = pst.executeQuery();
            if (rst.next()) {
                return rst.getInt("category_id");
            }

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }

        return -1;
    }

    /**
     * Get all keys of the entites
     *
     * @param mysql_host
     * @param database
     * @return
     */
    public static Map<String, Integer> getEntityKeys(String mysql_host, String database) {
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            Map<String, Integer> keys = new TreeMap<String, Integer>();
            pst = conn.prepareStatement("SELECT entity_uri, entity_id FROM entities");
            ResultSet rst = pst.executeQuery();
            while (rst.next()) {
                keys.put(rst.getString("entity_uri"), rst.getInt("entity_id"));
            }
            return keys;

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }

        return null;
    }

    /**
     * Get all keys of the categories
     *
     * @param mysql_host
     * @param database
     * @return
     */
    public static Map<String, Integer> getCategoryKeys(String mysql_host, String database) {
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            Map<String, Integer> keys = new TreeMap<String, Integer>();
            pst = conn.prepareStatement("SELECT category_uri, category_id FROM categories");
            ResultSet rst = pst.executeQuery();
            while (rst.next()) {
                keys.put(rst.getString("category_uri"), rst.getInt("category_id"));
            }
            return keys;

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }

        return null;
    }

    //===========================================================================================================================================================
    //===========================================================================================================================================================
    // Methods for reading the data from the LOD-Profiles resource types.
    //===========================================================================================================================================================
    //===========================================================================================================================================================

    /**
     * Gets the set of datasets part of the topic profiling analysis.
     *
     * @param mysql_host
     * @param database
     * @return
     */
    public static Map<String, String> getDatasets(String mysql_host, String database) {
        Map<String, String> datasets = new TreeMap<String, String>();
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("SELECT id_dataset, dataset FROM dataset_profiles;");
            ResultSet rst = pst.executeQuery();
            while (rst.next()) {
                String dataset = rst.getString("id_dataset").trim().replaceAll("\n", "");
                String dataset_name = rst.getString("dataset").trim().replaceAll("\n", "");
                datasets.put(dataset, dataset_name);
            }

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }

        return datasets;
    }

    /**
     * Returns all resource types from all datasets.
     *
     * @param mysql_host
     * @param database
     * @return
     */
    public static Set<String> getResourceTypes(String mysql_host, String database) {
        Set<String> resource_types = new HashSet<String>();
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("SELECT restype FROM resource_types;");
            ResultSet rst = pst.executeQuery();
            while (rst.next()) {
                String res_type = rst.getString("restype").trim().replaceAll("\n", "");
                resource_types.add(res_type);
            }

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
        return resource_types;
    }

    /**
     * Loads the resource type and categories associated with them for each
     * dataset.
     *
     * @param mysql_host
     * @param database
     * @return
     */
    public static Map<String, Map<String, Map<String, Double>>> getDatasetProfiles(String mysql_host, String database) {
        Map<String, Map<String, Map<String, Double>>> dataset_profiles = new TreeMap<String, Map<String, Map<String, Double>>>();
        Connection conn = getMySQLConnection(mysql_host, database);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("SELECT id_dataset, restype, category, count(id_category) as category_count FROM scored_profiles\n"
                    + "GROUP BY id_dataset, restype, category \n"
                    + "ORDER BY restype, id_dataset, category_count DESC");
            ResultSet rst = pst.executeQuery();
            while (rst.next()) {
                String dataset = rst.getString("id_dataset").trim().replaceAll("\n", "");
                Map<String, Map<String, Double>> sub_dataset_profiles = dataset_profiles.get(dataset);
                sub_dataset_profiles = sub_dataset_profiles == null ? new TreeMap<String, Map<String, Double>>() : sub_dataset_profiles;
                dataset_profiles.put(dataset, sub_dataset_profiles);

                String res_type = rst.getString("restype").trim().replaceAll("\n", "");
                Map<String, Double> res_type_profile = sub_dataset_profiles.get(res_type);
                res_type_profile = res_type_profile == null ? new TreeMap<String, Double>() : res_type_profile;
                sub_dataset_profiles.put(res_type, res_type_profile);

                String category = rst.getString("category").trim().replaceAll("\n", "");
                res_type_profile.put(category, (double) rst.getInt("category_count"));
            }

            //turn into probabilities the scores
            for (String dataset_id : dataset_profiles.keySet()) {
                Map<String, Double> restype_sums = new TreeMap<String, Double>();

                for (String resource_type : dataset_profiles.get(dataset_id).keySet()) {
                    double sum_val = 0;
                    for (String category : dataset_profiles.get(dataset_id).get(resource_type).keySet()) {
                        sum_val += dataset_profiles.get(dataset_id).get(resource_type).get(category);
                    }
                    restype_sums.put(resource_type, sum_val);
                }

                for (String resource_type : dataset_profiles.get(dataset_id).keySet()) {
                    for (String category : dataset_profiles.get(dataset_id).get(resource_type).keySet()) {
                        double prior_probability = dataset_profiles.get(dataset_id).get(resource_type).get(category) / (double) restype_sums.get(resource_type);
                        dataset_profiles.get(dataset_id).get(resource_type).put(category, prior_probability);
                    }
                }
            }

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
        return dataset_profiles;
    }

    /**
     * Loads the specific resource type profiles for each dataset.
     *
     * @param mysql_host
     * @param database
     * @param has_enrichments
     * @return
     */
    public static Map<String, Map<String, Set<Entry<String, Integer>>>> getDatasetTypeProfiles(String mysql_host, String database, boolean has_enrichments, String mysql_user, String mysql_pwd) {
        Map<String, Map<String, Set<Entry<String, Integer>>>> dataset_type_profiles = new TreeMap<String, Map<String, Set<Entry<String, Integer>>>>();

        Connection conn = getMySQLConnection(mysql_host, database, mysql_user, mysql_pwd);
        PreparedStatement pst = null;

        try {
            if (has_enrichments) {
                pst = conn.prepareStatement("SELECT d.dataset_name AS Dataset, t.type AS Resource_Type, t.subject AS Enrich_Value, t.count AS Count FROM type_profiles as t, datasets as d "
                        + " WHERE d.id = t.dataset_id and t.subject like 'http://dbpedia%' "
                        + " GROUP BY d.dataset_name, t.type, t.subject ORDER BY type, count DESC;");
            } else {
                pst = conn.prepareStatement("SELECT d.dataset_name AS Dataset, t.type AS Resource_Type, tv.enrich_value AS Enrich_Value, t.count AS Count FROM type_profiles as t, datasets as d, type_profile_values as tv"
                        + " WHERE t.subject NOT LIKE 'http://dbpedia%' AND tv.enrich_value IS NOT NULL AND "
                        + " t.subject = tv.property AND d.id = t.dataset_id "
                        + " GROUP BY  d.dataset_name, t.type, tv.enrich_value ORDER BY type, count DESC");
            }
            ResultSet rst = pst.executeQuery();
            while (rst.next()) {
                String dataset_name = rst.getString("Dataset").trim().replaceAll("\n", "");
                String resource_type = rst.getString("Resource_Type").trim().replaceAll("\n", "");
                String resource_type_subject = rst.getString("Enrich_Value").trim().replaceAll("\n", "");
                int count = rst.getInt("Count");

                //process first resource type subject in case there are more than one resource
                resource_type_subject = resource_type_subject.replaceAll(",http", ";http");
                resource_type_subject = resource_type_subject.replaceAll(", http", ";http");
                resource_type_subject = resource_type_subject.replaceAll(", null", "");
                resource_type_subject = resource_type_subject.replaceAll("null", "");
                if (resource_type_subject.trim().isEmpty()) {
                    continue;
                }

                Map<String, Set<Entry<String, Integer>>> sub_dataset_type_profiles = dataset_type_profiles.get(dataset_name);
                sub_dataset_type_profiles = sub_dataset_type_profiles == null ? new TreeMap<String, Set<Entry<String, Integer>>>() : sub_dataset_type_profiles;
                dataset_type_profiles.put(dataset_name, sub_dataset_type_profiles);

                Set<Entry<String, Integer>> type_subjects = sub_dataset_type_profiles.get(resource_type);
                type_subjects = type_subjects == null ? new HashSet<Entry<String, Integer>>() : type_subjects;
                sub_dataset_type_profiles.put(resource_type, type_subjects);

                String[] sub_res_type_subjects = resource_type_subject.split(";");
                for (String sub_res_type_subject : sub_res_type_subjects) {
                    Entry<String, Integer> entry = new AbstractMap.SimpleEntry<String, Integer>(sub_res_type_subject, count);
                    type_subjects.add(entry);
                }
            }

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());

        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
        return dataset_type_profiles;
    }

    /**
     * Writes the adjacency matrix for a specific resource type and its corresponding profile.
     * @param type_uri
     * @param category_distances
     * @param mysql_host
     * @param database
     * @param mysql_user
     * @param mysql_pwd
     */
    public static void writeTypeProfileCategoryAdjacencyMatrix(String dataset_id, String type_uri, Map<String, Map<String, Double>> category_distances, String mysql_host, String database, String mysql_user, String mysql_pwd) {
        Connection conn = getMySQLConnection(mysql_host, database, mysql_user, mysql_pwd);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("INSERT INTO type_profile_graph_adjacency(dataset_id, type_uri, category_uri_a, category_uri_b, path_length) VALUES(?,?,?,?,?)");
            for(String category_uri_a:category_distances.keySet()){
                for(String category_uri_b:category_distances.get(category_uri_a).keySet()){
                    double path_length = category_distances.get(category_uri_a).get(category_uri_b);

                    pst.setString(1, dataset_id);
                    pst.setString(2, type_uri);
                    pst.setString(3, category_uri_a);
                    pst.setString(4, category_uri_b);
                    pst.setDouble(5, path_length);

                    pst.addBatch();
                }
            }

            pst.executeBatch();

        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    /**
     * Loads the adjacency matrix for the profiles of a resource type.
     * @param type_uri
     * @param mysql_host
     * @param database
     * @param mysql_user
     * @param mysql_pwd
     * @return
     */
    public static Map<String, Map<String, Double>> getTypeProfileAdjacencyMatrix(String dataset_id, String type_uri, String mysql_host, String database, String mysql_user, String mysql_pwd){
        Map<String, Map<String, Double>> adjacency_matrix = new TreeMap<String, Map<String, Double>>();
        Connection conn = getMySQLConnection(mysql_host, database, mysql_user, mysql_pwd);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("SELECT category_uri_a, category_uri_b, path_length FROM type_profile_graph_adjacency WHERE dataset_id=? AND type_uri=?");
            pst.setString(1, dataset_id);
            pst.setString(2, type_uri);

            ResultSet rs = pst.executeQuery();
            while(rs.next()){
                String category_uri_a = rs.getString("category_uri_a");
                String category_uri_b = rs.getString("category_uri_b");
                double path_length = rs.getDouble("path_length");

                Map<String, Double> sub_adjacency_matrix = adjacency_matrix.get(category_uri_a);
                sub_adjacency_matrix = sub_adjacency_matrix == null ? new TreeMap<String, Double>() : sub_adjacency_matrix;
                adjacency_matrix.put(category_uri_a, sub_adjacency_matrix);

                sub_adjacency_matrix.put(category_uri_b, path_length);
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }

        return adjacency_matrix;
    }

    /**
     * Checks whether the adjacency matrix for the profiles of a resource type is written in the database.
     * @param type_uri
     * @param mysql_host
     * @param database
     * @param mysql_user
     * @param mysql_pwd
     * @return
     */
    public static boolean hasTypeProfileAdjacencyMatrix(String dataset_id, String type_uri, String mysql_host, String database, String mysql_user, String mysql_pwd){
        Connection conn = getMySQLConnection(mysql_host, database, mysql_user, mysql_pwd);
        PreparedStatement pst = null;

        try {
            pst = conn.prepareStatement("SELECT COUNT(*) FROM type_profile_graph_adjacency WHERE dataset_id=? AND type_uri=?");
            pst.setString(1, dataset_id);
            pst.setString(2, type_uri);

            ResultSet rs = pst.executeQuery();
            if(rs.next()){
               return rs.getInt(1) != 0;
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        } finally {
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }

        return false;
    }
}
