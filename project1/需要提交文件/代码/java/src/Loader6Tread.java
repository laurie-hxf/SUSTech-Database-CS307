import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.CountDownLatch;

public class Loader6Tread {
    private static final int BATCH_SIZE = 3000;
    private static Connection con = null;
    private static PreparedStatement stmt = null;
    private static final Object lock = new Object();

    private static Connection openDB(Properties prop) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (Exception e) {
            throw new RuntimeException("Cannot find the Postgres driver. Check CLASSPATH.");
        }

        String url = "jdbc:postgresql://" + prop.getProperty("host") + "/" + prop.getProperty("database");
        try {
            Connection con = DriverManager.getConnection(url, prop);
            con.setAutoCommit(false);
            return con;
        } catch (SQLException e) {
            throw new RuntimeException("Database connection failed", e);
        }
    }

    public static PreparedStatement setPrepareStatement(String table, Connection con) throws SQLException {
        switch (table) {
            case "company":
                return con.prepareStatement("INSERT INTO public.company (client_enterprise, country, city, industry) VALUES (?,?,?,?)");
            case "contract":
                return con.prepareStatement("INSERT INTO public.contract (contract_number, order_date, supply_center, client_enterprise) VALUES (?,?,?,?)");
            case "orders":
                return con.prepareStatement("INSERT INTO public.orders (order_id, quantity, estimated_delivery_date, lodgement_date, contract_number, salesman_id, product_code) VALUES (?,?,?,?,?,?,?)");
            case "product_model":
                return con.prepareStatement("INSERT INTO public.product_model (product_code, product_model, unit_price) VALUES (?,?,?)");
            case "product":
                return con.prepareStatement("INSERT INTO public.product (product_code, product_name) VALUES (?,?)");
            case "salesman":
                return con.prepareStatement("INSERT INTO public.salesman (salesman_id, salesman_number, salesman, gender, age, mobile_number) VALUES (?,?,?,?,?,?)");
            case "supply_center":
                return con.prepareStatement("INSERT INTO public.supply_center (supply_center, director) VALUES (?,?)");
            default:
                throw new SQLException("Unknown table: " + table);
        }
    }


    public static void setPrepareStatement(String table) {
        try {
            switch (table) {
                case "company":
                    stmt = con.prepareStatement("INSERT INTO public.company (client_enterprise, country, city, industry) " +
                            "VALUES (?,?,?,?);");
                    break;
                case "contract":
                    stmt = con.prepareStatement("INSERT INTO public.contract (contract_number, order_date, supply_center, client_enterprise) " +
                            "VALUES (?,?,?,?);");
                    break;
                case "orders":
                    stmt = con.prepareStatement("INSERT INTO public.orders (order_id, quantity, estimated_delivery_date, lodgement_date, contract_number, salesman_id, product_code) " +
                            "VALUES (?,?,?,?,?,?,?);");
                    break;
                case "product_model":
                    stmt = con.prepareStatement("INSERT INTO public.product_model (product_code, product_model, unit_price) " +
                            "VALUES (?,?,?);");
                    break;
                case "product":
                    stmt = con.prepareStatement("INSERT INTO public.product (product_code, product_name) " +
                            "VALUES (?,?);");
                    break;
                case "salesman":
                    stmt = con.prepareStatement("INSERT INTO public.salesman (salesman_id, salesman_number, salesman, gender, age, mobile_number) " +
                            "VALUES (?,?,?,?,?,?);");
                    break;
                case "supply_center":
                    stmt = con.prepareStatement("INSERT INTO public.supply_center (supply_center, director)" +
                            "VALUES (?,?);");
            }
        } catch (SQLException e) {
            System.err.println("Insert statement failed");
            System.err.println(e.getMessage());
            closeDB();
            System.exit(1);
        }
    }

    private static void closeDB() {
//        if (con != null) {
        try {
            if (stmt != null) {
                stmt.close();
            }
            con.close();
            con = null;
        } catch (Exception ignored) {
        }
//        }
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
            return Files.readAllLines(Path.of("../sql/" + name + ".sql"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void loadData(String line, String table, PreparedStatement stmt) {

//        Matcher matcher = pattern.matcher(line);
//        if (con != null) {
        try {
            String[] temp;
            List<String> lineData = new ArrayList<>();
            if (table.equals("orders")) {
                temp = line.split("[(]");
                String[] temp2 = temp[2].split("[',)]");
                if (temp2[7].equals(" null")) {
                    temp2[8] = "null";
                    temp2[15] = temp2[13];
                    temp2[13] = temp2[11];
                    temp2[11] = temp2[9];
                }
                lineData.add(" ");
                lineData.add(temp2[1].trim());
                lineData.add(temp2[3].trim());
                lineData.add(temp2[5].trim());
                lineData.add(temp2[8].trim());
                lineData.add(temp2[11].trim());
                lineData.add(temp2[13].trim());
                lineData.add(temp2[15].trim());
            } else if (table.equals("product_model")) {
                temp = line.split("[(]");
                String[] temp2 = temp[2].split("[',)]");
                if (temp2[6].equals("SBatteryT2")) {
                    temp2[4] = temp2[4] + "'SBatteryT2";
                    temp2[6] = temp2[8];
                }
                lineData.add(" ");
                lineData.add(temp2[1].trim());
                lineData.add(temp2[4].trim());
                lineData.add(temp2[6].trim());
            } else if (table.equals("salesman")) {
                temp = line.split("[(]");
                String[] temp2 = temp[2].split("[',)]");
                lineData.add(" ");
                lineData.add(temp2[0].trim());
                lineData.add(temp2[1].trim());
                lineData.add(temp2[3].trim());
                lineData.add(temp2[6].trim());
                lineData.add(temp2[8].trim());
                lineData.add(temp2[9].trim());
            } else {
                temp = line.split("'");
                for (int i = 0; i < temp.length - 1; i++) {
                    if (!Objects.equals(temp[i], ", ")) {
                        if (Objects.equals(temp[i], ", null, ")) {
                            lineData.add("null");
                        } else {
                            lineData.add(temp[i]);
                        }
                    }
                }
            }

            switch (table) {
                case "company":
                    stmt.setString(1, lineData.get(1));
                    stmt.setString(2, lineData.get(2));
                    stmt.setString(3, lineData.get(3));
                    stmt.setString(4, lineData.get(4));
                    break;

                case "contract":
                    stmt.setString(1, lineData.get(1));
                    stmt.setDate(2, Date.valueOf(lineData.get(2)));
                    stmt.setString(3, lineData.get(3));
                    stmt.setString(4, lineData.get(4));
                    break;

                case "orders":
                    stmt.setString(1, lineData.get(1));
                    stmt.setInt(2, Integer.parseInt(lineData.get(2)));
                    stmt.setDate(3, Date.valueOf(lineData.get(3)));
                    if (lineData.get(4) == null || lineData.get(4).equalsIgnoreCase("null") || lineData.get(4).isBlank()) {
                        stmt.setNull(4, java.sql.Types.DATE);
                    } else {
                        stmt.setDate(4, Date.valueOf(lineData.get(4)));
                    }
//                        stmt.setDate(4, Date.valueOf(lineData.get(4)));
                    stmt.setString(5, lineData.get(5));
                    stmt.setInt(6, Integer.parseInt(lineData.get(6)));
                    stmt.setString(7, lineData.get(7));
                    break;

                case "product_model":
                    stmt.setString(1, lineData.get(1));
                    stmt.setString(2, lineData.get(2));
                    stmt.setInt(3, Integer.parseInt(lineData.get(3)));
                    break;
                case "product":
                    stmt.setString(1, lineData.get(1));
                    stmt.setString(2, lineData.get(2));
                    break;
                case "salesman":
                    stmt.setInt(1, Integer.parseInt(lineData.get(1)));
                    stmt.setInt(2, Integer.parseInt(lineData.get(2)));
                    stmt.setString(3, lineData.get(3));
                    stmt.setString(4, lineData.get(4));
                    stmt.setInt(5, Integer.parseInt(lineData.get(5)));
                    stmt.setLong(6, Long.parseLong(lineData.get(6)));
                    break;
                case "supply_center":
                    stmt.setString(1, lineData.get(1));
                    stmt.setString(2, lineData.get(2));
            }
            stmt.addBatch();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
//        }
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

    public static void main(String[] args) throws SQLException, InterruptedException {
        String[] sqls1 = {"company", "supply_center", "salesman", "product", "contract"};
        String[] sqls2 = {"orders", "product_model"};
        int[] totalRecords = new int[1];
        long start = System.currentTimeMillis();


        for (String[] sqls : new String[][]{sqls1, sqls2}) {
            CountDownLatch latch = new CountDownLatch(sqls.length);

            for (String tableName : sqls) {
                Thread thread = new Thread(() -> {
                    Connection localCon = null;
                    PreparedStatement localStmt = null;

                    try {
                        Properties prop = loadDBUser();
                        List<String> lines = loadSQLFile(tableName);
                        int cnt = 0;

                        localCon = openDB(prop);
                        localStmt = setPrepareStatement(tableName, localCon);

                        for (String line : lines) {
                            loadData(line, tableName, localStmt);
                            cnt++;
                            if (cnt % BATCH_SIZE == 0) {
                                localStmt.executeBatch();
                                System.out.println("Thread " + Thread.currentThread().getName() +
                                        " inserted " + BATCH_SIZE + " records into " + tableName);
                            }
                        }

                        if (cnt % BATCH_SIZE != 0) {
                            localStmt.executeBatch();
                            System.out.println("Thread " + Thread.currentThread().getName() +
                                    " inserted " + (cnt % BATCH_SIZE) + " records into " + tableName);
                        }

                        localCon.commit();

                        synchronized (totalRecords) {
                            totalRecords[0] += cnt;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (localStmt != null) localStmt.close();
                            if (localCon != null) localCon.close();
                        } catch (SQLException ignored) {
                        }
                    }

                    latch.countDown();
                });

                thread.setName("Loader-" + tableName);
                thread.start();
            }

            latch.await(); // 等待本组表全部加载完毕
        }

        long end = System.currentTimeMillis();
        System.out.println(totalRecords[0] + " records successfully loaded");
        System.out.println("Spending time : " + (end - start) + "ms");
        System.out.println("Loading speed : " + (totalRecords[0] * 1000L) / (end - start) + " records/s");
    }
}

