import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.*;

/**
 * Cloud Computing and Big Data Ecosystems Design: HBase assignment
 * Universidad Politecnica de Madrid
 *
 * @author Moritz Meister (moritz.meister@alumnos.upm.es, meister.mo@gmail.com)
 * @author Gioele Bigini (gioele.bigini@gmail.com)
 * @date 16/01/19.
 */

public class HBaseScrabble {
    private Configuration config;
    private HBaseAdmin hBaseAdmin;

    /**
     * The Constructor. Establishes the connection with HBase.
     *
     * @param zkHost
     * @throws IOException
     */
    public HBaseScrabble(String zkHost) throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkHost.split(":")[0]);
        config.set("hbase.zookeeper.property.clientPort", zkHost.split(":")[1]);
        HBaseConfiguration.addHbaseResources(config);
        this.hBaseAdmin = new HBaseAdmin(config);
    }

    /**
     * Initializes and creates the HTable with name 'ScrabbleGames" with its schema.
     *
     * If the table exists already, it is being disabled and deleted to be reinitialized.
     *
     * Columns are grouped logically into column families, so these column families
     * are rather general. Since we only have three queries, only optimizing for these
     * queries would be bad practice if the queries were ever to be extended.
     *
     * Column families:
     * 1. Game: contains all game related information
     * 2. Winner: contains all information related to the winning player of the game
     * 3. Loser: contains all information related to the losing player of the game
     *
     * @throws IOException
     */
    public void createTable() throws IOException {
        byte[] TABLE = Bytes.toBytes("ScrabbleGames");
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE));

        byte[] gInfo = Bytes.toBytes("Game");
        byte[] wInfo = Bytes.toBytes("Winner");
        byte[] lInfo = Bytes.toBytes("Loser");

        try {
            this.hBaseAdmin.disableTable("ScrabbleGames"); // disable the table
            this.hBaseAdmin.deleteTable("ScrabbleGames");  // Delete the table
            System.out.println("Deleted previous ScrabbledGames table version");
        } catch (Exception e) {
        }

        HColumnDescriptor gFamily = new HColumnDescriptor(gInfo);
        gFamily.setMaxVersions(10); // Default is 3.
        HColumnDescriptor wFamily = new HColumnDescriptor(wInfo);
        wFamily.setMaxVersions(10); // Default is 3.
        HColumnDescriptor lFamily = new HColumnDescriptor(lInfo);
        lFamily.setMaxVersions(10); // Default is 3.

        table.addFamily(gFamily);
        table.addFamily(wFamily);
        table.addFamily(lFamily);

        this.hBaseAdmin.createTable(table);
    }

    /**
     * Loads data from the specified folder into the previously defined table "ScrabbleGames".
     *
     * The row key being used is a 20 character string, where TourneyID and GameID are being padded with leading zeros
     * to extend them to 20 character length. For example, 00000421530000000123 would be Game 123 in Tourney 42153.
     *
     * Rows are read one by one from the file but the puts are being loaded into the HBase table in batches of 100.000.
     *
     * @param folder containing the input data with the name "scrabble_games.csv". It requires the data folder to be in
     *               in the root folder of the project at the same level as the src folder.
     * @throws IOException
     * @throws InterruptedException
     */
    public void loadTable(String folder) throws IOException, InterruptedException {

        byte[] gInfo = Bytes.toBytes("Game");
        byte[] wInfo = Bytes.toBytes("Winner");
        byte[] lInfo = Bytes.toBytes("Loser");

        String filePath = folder + "/" + "scrabble_games.csv";

        HTable table = new HTable(config, "ScrabbleGames");

        int[] keyTable = {1, 0};

        String line;
        BufferedReader br = new BufferedReader(new FileReader(filePath));

        // read header
        String header = br.readLine();
        List<String> columns = Arrays.asList(header.split(","));

        List<Put> putList = new ArrayList<Put>();

        int rowId = 1;
        while ((line = br.readLine()) != null) {

            String[] values = line.split(",");

            Put p = new Put(getKey(values, keyTable));
            List<String> cells = Arrays.asList(line.split(","));

            //Game info
            p.add(gInfo, Bytes.toBytes("gameid"), Bytes.toBytes(cells.get(0)));
            p.add(gInfo, Bytes.toBytes("tourneyid"), Bytes.toBytes(cells.get(1)));
            p.add(gInfo, Bytes.toBytes("tie"), Bytes.toBytes(cells.get(2)));
            p.add(gInfo, Bytes.toBytes("round"), Bytes.toBytes(cells.get(15)));
            p.add(gInfo, Bytes.toBytes("division"), Bytes.toBytes(cells.get(16)));
            p.add(gInfo, Bytes.toBytes("date"), Bytes.toBytes(cells.get(17)));
            p.add(gInfo, Bytes.toBytes("lexicon"), Bytes.toBytes(cells.get(18)));

            //Winner info
            p.add(wInfo, Bytes.toBytes("id"), Bytes.toBytes(cells.get(3)));
            p.add(wInfo, Bytes.toBytes("name"), Bytes.toBytes(cells.get(4)));
            p.add(wInfo, Bytes.toBytes("score"), Bytes.toBytes(cells.get(5)));
            p.add(wInfo, Bytes.toBytes("oldrating"), Bytes.toBytes(cells.get(6)));
            p.add(wInfo, Bytes.toBytes("newrating"), Bytes.toBytes(cells.get(7)));
            p.add(wInfo, Bytes.toBytes("pos"), Bytes.toBytes(cells.get(8)));

            //Loser info
            p.add(lInfo, Bytes.toBytes("id"), Bytes.toBytes(cells.get(9)));
            p.add(lInfo, Bytes.toBytes("name"), Bytes.toBytes(cells.get(10)));
            p.add(lInfo, Bytes.toBytes("score"), Bytes.toBytes(cells.get(11)));
            p.add(lInfo, Bytes.toBytes("oldrating"), Bytes.toBytes(cells.get(12)));
            p.add(lInfo, Bytes.toBytes("newrating"), Bytes.toBytes(cells.get(13)));
            p.add(lInfo, Bytes.toBytes("pos"), Bytes.toBytes(cells.get(14)));

            putList.add(p);

            if (rowId % 100000.0 == 0) {
                // write in batch to HBase makes it a bit faster, however not sure how big batches can be
                // taking all data at once leads to heap size exception
                table.put(putList);
                putList.clear();
                System.out.println("Loaded 100000 Records");
            }
            rowId++;
        }

        System.out.println("Last Line: " + rowId);
        table.put(putList);
        System.out.println("Put rest: " + putList.size());

    }

    /**
     * This method generates the key.
     *
     * @param values   The value of each column
     * @param keyTable The position of each value that is required to create the key in the array of values.
     * @return The encoded key to be inserted in HBase
     */
    private byte[] getKey(String[] values, int[] keyTable) {
        String keyString = "";
        for (int keyId : keyTable) {
            keyString += String.format("%010d", Integer.parseInt(values[keyId]));
        }
        byte[] key = Bytes.toBytes(keyString);

        return key;
    }


    /**
     * Counts total number of records loaded into the "ScrabbleGames" table.
     */
    public void countRecords() throws IOException {
        HTable table = new HTable(config, "ScrabbleGames");
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        Result res = rs.next();

        int nRows = 0;

        while (res != null && !res.isEmpty()) {
            nRows++;
            res = rs.next();
        }

        System.out.println("Total rows in table: " + nRows);
    }

    public List<String> query1(String tourneyid, String winnername) throws IOException {
        HTable table = new HTable(config, "ScrabbleGames");

        byte[] startKey = Bytes.toBytes(String.format("%010d", Integer.parseInt(tourneyid)) + "0000000000");
        byte[] endKey = Bytes.toBytes(String.format("%010d", Integer.parseInt(tourneyid)) + "9999999999");
        Scan scan = new Scan(startKey, endKey);

        SingleColumnValueFilter f = new SingleColumnValueFilter(Bytes.toBytes("Winner"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(winnername));

        ArrayList<String> queryResult = new ArrayList<>();

        scan.setFilter(f);
        ResultScanner rs = table.getScanner(scan);

        Result result = rs.next();
        while (result != null && !result.isEmpty()) {
            // String key = Bytes.toString(result.getRow());
            queryResult.add(Bytes.toString(result.getValue(Bytes.toBytes("Loser"), Bytes.toBytes("id"))));
            result = rs.next();
        }

        return queryResult;
    }

    public List<String> query2(String firsttourneyid, String lasttourneyid) throws IOException {
        HTable table = new HTable(config, "ScrabbleGames");

        byte[] startKey = Bytes.toBytes(String.format("%010d", Integer.parseInt(firsttourneyid)) + "0000000000");
        byte[] endKey = Bytes.toBytes(String.format("%010d", Integer.parseInt(lasttourneyid)) + "0000000000");
        Scan scan = new Scan(startKey, endKey);

        Set<String> finalResult = new HashSet<>();
        Set<String> queryResult = new HashSet<>();
        Set<String> appearedOnce = new HashSet<>();

        ResultScanner rs = table.getScanner(scan);

        String currentTourney = "-1";

        Result result = rs.next();
         while (result != null && !result.isEmpty()) {
            //String key = Bytes.toString(result.getRow());

            String temp = Bytes.toString(result.getValue(Bytes.toBytes("Game"), Bytes.toBytes("tourneyid")));

            if (!currentTourney.equals(temp)) {

                if (!firsttourneyid.equals(currentTourney)) {
                    // The retain takes the interception of the lists
                    finalResult.retainAll(queryResult);
                }

                finalResult.addAll(queryResult);
                queryResult.clear();
                appearedOnce.clear();
                currentTourney = temp;
            }

            // check if WINNER appeared once or already more
            String playerId = Bytes.toString(result.getValue(Bytes.toBytes("Winner"), Bytes.toBytes("id")));

            if (appearedOnce.contains(playerId)) {
                queryResult.add(playerId);
                appearedOnce.remove(playerId);
            }
            else {
                if (!queryResult.contains(playerId))
                    appearedOnce.add(playerId);
            }

            // Do the same for LOSER
            playerId = Bytes.toString(result.getValue(Bytes.toBytes("Loser"), Bytes.toBytes("id")));

            if (appearedOnce.contains(playerId)) {
                queryResult.add(playerId);
                appearedOnce.remove(playerId);
            }
            else {
                if (!queryResult.contains(playerId))
                    appearedOnce.add(playerId);
            }

            result = rs.next();
        }

        appearedOnce.clear();
         // here the problem is that when you take the intersection of the lists in the interval 1 to 2
        // the while cycle exit before updating the finalresult lists (It does only one cycle). Since it is empty this list, the
        // intersection will give an empty set. In the interval 1 to 2 so It is en error to do it (the intersection), we should just
        // add the results to finalresults. It is not an error for an interval bigger than 1 to 2 because the finalresults will
        // be updated in the while loop (at least it do two cycles) and in the case of an empty finalresults list
        // means that immediately in the first tourney no-one played more than two games, so It is right to have
        // an empty result at the end.
        if (Integer.parseInt(lasttourneyid) - Integer.parseInt(firsttourneyid) < 2)
            finalResult.addAll(queryResult);
        else
            finalResult.retainAll(queryResult);

        return new ArrayList<>(finalResult);
    }

    public List<String> query3(String tourneyid) throws IOException {
        HTable table = new HTable(config, "ScrabbleGames");

        byte[] startKey = Bytes.toBytes(String.format("%010d", Integer.parseInt(tourneyid)) + "0000000000");
        byte[] endKey = Bytes.toBytes(String.format("%010d", Integer.parseInt(tourneyid)) + "9999999999");
        Scan scan = new Scan(startKey, endKey);

        SingleColumnValueFilter f = new SingleColumnValueFilter(Bytes.toBytes("Game"),
                Bytes.toBytes("tie"),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("True"));

        ArrayList<String> queryResult = new ArrayList<>();

        scan.setFilter(f);
        ResultScanner rs = table.getScanner(scan);

        Result result = rs.next();
        while (result != null && !result.isEmpty()) {
            //String key = Bytes.toString(result.getRow());
            queryResult.add(Bytes.toString(result.getValue(Bytes.toBytes("Game"), Bytes.toBytes("gameid"))));
            result = rs.next();
        }

        return queryResult;
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 2) {
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }
        //args = new String[4];
        //args[0] = "localhost:2181";
        //args[1] = "QUERY2B";
        //args[2] = "1";
        //args[3] = "3";
        HBaseScrabble hBaseScrabble = new HBaseScrabble(args[0]);
        if (args[1].toUpperCase().equals("CREATETABLE")) {
            long startTime = System.nanoTime();
            hBaseScrabble.createTable();
            double estimatedTime = (System.nanoTime() - startTime) / 1000000000.0;
            System.out.println("Query took " + estimatedTime + " seconds.");
        } else if (args[1].toUpperCase().equals("LOADTABLE")) {
            if (args.length != 3) {
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables], 3)csvsFolder");
                System.exit(-1);
            } else if (!(new File(args[2])).isDirectory()) {
                System.out.println("Error: Folder " + args[2] + " does not exist.");
                System.exit(-2);
            }
            long startTime = System.nanoTime();
            hBaseScrabble.loadTable(args[2]);
            double estimatedTime = (System.nanoTime() - startTime) / 1000000000.0;
            System.out.println("Query took " + estimatedTime + " seconds.");
            //hBaseScrabble.merge();
            //System.out.println("Merged automatically split regions.");
        } else if (args[1].toUpperCase().equals("QUERY1")) {
            if (args.length != 4) {
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query1, " +
                        "3) tourneyid 4) winnername");
                System.exit(-1);
            }
            long startTime = System.nanoTime();
            List<String> opponentsName = hBaseScrabble.query1(args[2], args[3]);
            double estimatedTime = (System.nanoTime() - startTime) / 1000000000.0;
            System.out.println("There are " + opponentsName.size() + " opponents of winner " + args[3] + " that play in tourney " + args[2] + ".");
            System.out.println("The list of opponents is: " + Arrays.toString(opponentsName.toArray(new String[opponentsName.size()])));
            System.out.println("Query took " + estimatedTime + " seconds.");
        } else if (args[1].toUpperCase().equals("QUERY2")) {
            if (args.length != 4) {
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query2, " +
                        "3) firsttourneyid 4) lasttourneyid");
                System.exit(-1);
            }
            long startTime = System.nanoTime();
            List<String> playerNames = hBaseScrabble.query2(args[2], args[3]);
            double estimatedTime = (System.nanoTime() - startTime) / 1000000000.0;
            System.out.println("There are " + playerNames.size() + " players that participates in more than one tourney between tourneyid " + args[2] + " and tourneyid " + args[3] + " .");
            System.out.println("The list of players is: " + Arrays.toString(playerNames.toArray(new String[playerNames.size()])));
            System.out.println("Query took " + estimatedTime + " seconds.");
        } else if (args[1].toUpperCase().equals("QUERY3")) {
            if (args.length != 3) {
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) tourneyid");
                System.exit(-1);
            }
            long startTime = System.nanoTime();
            List<String> games = hBaseScrabble.query3(args[2]);
            double estimatedTime = (System.nanoTime() - startTime) / 1000000000.0;
            System.out.println("There are " + games.size() + " that ends in tie in tourneyid " + args[2] + " .");
            System.out.println("The list of games is: " + Arrays.toString(games.toArray(new String[games.size()])));
            System.out.println("Query took " + estimatedTime + " seconds.");
        } else if (args[1].toUpperCase().equals("COUNTRECORDS")) {
            // print total number of records
            hBaseScrabble.countRecords();
        } else {
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }

    }

}
