import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.sql.*;

public class Loader1Awful {
    private static Connection con = null;
    private static Statement stmt = null;

    private static void openDB(Properties prop) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (Exception e) {
            System.err.println("Cannot find the Postgres driver. Check CLASSPATH.");
            System.exit(1);
        }
        String url = "jdbc:postgresql://" + prop.getProperty("host") + "/" + prop.getProperty("database");
        try {
            con = DriverManager.getConnection(url, prop);
//            if (con != null) {
//                System.out.println("Successfully connected to the database "
//                        + prop.getProperty("database") + " as " + prop.getProperty("user"));
//            }
        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private static void closeDB() {
        if (con != null) {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                con.close();
                con = null;
            } catch (Exception ignored) {
            }
        }
    }

    private static Properties loadDBUser() {
        Properties properties = new Properties();
        try {
            properties.load(new InputStreamReader(new FileInputStream("resources/dbUser.properties")));
            return properties;
        } catch (IOException e) {
            System.err.println("can not find db user file");
            throw new RuntimeException(e);
        }
    }

    private static List<String> loadSQLFile(String name) {
        try {
            return Files.readAllLines(Path.of("../sql/"+name+".sql"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void loadData(String line) {
        try {
            if (con != null) {
                stmt = con.createStatement();
                stmt.execute(line);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

//    public static void clearDataInTable() {
//        Statement stmt0;
//        if (con != null) {
//            try {
//                stmt0 = con.createStatement();
//                stmt0.executeUpdate("drop table movies;");
//                stmt0.executeUpdate("create table if not exists movies(\n" +
//                        "movieid serial not null\n" +
//                        "constraint movies_pkey\n" +
//                        "primary key,\n" +
//                        "title varchar(200) not null,\n" +
//                        "country char(2) not null\n" +
//                        "constraint movies_country_fkey\n" +
//                        "references countries,\n" +
//                        "year_released integer not null,\n" +
//                        "runtime integer,\n" +
//                        "constraint movies_title_country_year_released_key\n" +
//                        "unique (title, country, year_released)\n" +
//                        ");");
//                stmt0.close();
//            } catch (SQLException ex) {
//                throw new RuntimeException(ex);
//            }
//        }
//    }

    public static void main(String[] args) {

        String[] sqls = new String[]{
                "company", "supply_center","contract","salesman", "product",  "product_model","orders"
        };

        int cnt = 0;
        long start = System.currentTimeMillis();
        for (String sql : sqls) {

            Properties prop = loadDBUser();
            List<String> lines = loadSQLFile(sql);

            // Empty target table
//        openDB(prop);
//        clearDataInTable();
//        closeDB();


            for (String line : lines) {
                openDB(prop);
                loadData(line);//do insert command
                closeDB();

                cnt++;
                if (cnt % 1000 == 0) {
                    System.out.println("insert " + 1000 + " data successfully!");
                }
            }
        }

        long end = System.currentTimeMillis();
        System.out.println(cnt + " records successfully loaded");
        System.out.println("Spending time : "+(end - start)+"ms");
        System.out.println("Loading speed : " + (cnt * 1000L) / (end - start) + " records/s");
    }
}

