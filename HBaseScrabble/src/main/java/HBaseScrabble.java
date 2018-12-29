import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;


public class HBaseScrabble {
    private Configuration config;
    private HBaseAdmin hBaseAdmin;

    /**
     * The Constructor. Establishes the connection with HBase.
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

    // TODO: The column families are NOT that right. This should be changed
    public void createTable() throws IOException {
        byte[] TABLE = Bytes.toBytes("ScrabbleGames");
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE));

        try {
            this.hBaseAdmin.disableTable("ScrabbleGames"); // disable the table
            this.hBaseAdmin.deleteTable("ScrabbleGames");  // Delete the table
            System.out.println("Deleted previous ScrabbledGames table version");
        } catch (Exception e) {
        }

        File file = new File("data/scrabble_games.csv");
        Scanner fileReader = new Scanner(file);

        String header = fileReader.nextLine();
        List<String> columns = Arrays.asList(header.split(","));

        for (String s: columns) {
            table.addFamily(new HColumnDescriptor(Bytes.toBytes(s.toUpperCase())));
        }

        this.hBaseAdmin.createTable(table);
    }

    // TODO: LOADING THE TABLE ROW-BY-ROW IS INEFFICIENT. CAN WE DO SOMETHING DIFFERENT?
    // It takes 15 minutes
    public void loadTable()throws IOException {
        HTable table = new HTable(config, "ScrabbleGames");

        File file = new File("data/scrabble_games.csv");
        Scanner fileReader = new Scanner(file);
        String header = fileReader.nextLine();
        List<String> columns = Arrays.asList(header.split(","));

        int rowId = 1;
        while (fileReader.hasNext()) {
            Put p = new Put(Bytes.toBytes("row" + rowId));

            String line = fileReader.nextLine();
            List<String> cells = Arrays.asList(line.split(","));
            for (int i = 0; i < columns.size(); i++) {
                p.add(Bytes.toBytes(columns.get(i).toUpperCase()), Bytes.toBytes(columns.get(i).toUpperCase()), Bytes.toBytes(cells.get(i)));
            }

            table.put(p);
            rowId++;

            if (rowId%100000.0 == 0) {
                System.out.println("Loaded 100000 Records");
            }
        }
    }

    /**
     * This method generates the key
     * @param values The value of each column
     * @param keyTable The position of each value that is required to create the key in the array of values.
     * @return The encoded key to be inserted in HBase
     */
    private byte[] getKey(String[] values, int[] keyTable) {
        String keyString = "";
        for (int keyId : keyTable){
            keyString += values[keyId];
        }
        byte[] key = Bytes.toBytes(keyString);

        return key;
    }

    public void query0() throws IOException {
        HTable table = new HTable(config, "ScrabbleGames");

        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        Result res = rs.next();

        // print only the first 3 rows
        for (int i = 0; i < 3; i++) {
            System.out.println(Bytes.toString(res.getValue(Bytes.toBytes("GAMEID"), Bytes.toBytes("GAMEID"))));
            res = rs.next();
        }
    }

    public List<String> query1(String tourneyid, String winnername) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return null;

    }

    public List<String> query2(String firsttourneyid, String lasttourneyid) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return null;
    }

    public List<String> query3(String tourneyid) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return null;
    }


    public static void main(String[] args) throws IOException {
        if(args.length<2){
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }
        HBaseScrabble hBaseScrabble = new HBaseScrabble(args[0]);
        if(args[1].toUpperCase().equals("CREATETABLE")){
            hBaseScrabble.createTable();
        }
        else if(args[1].toUpperCase().equals("LOADTABLE")){

            //if(args.length!=3){
            //    System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables], 3)csvsFolder");
            //    System.exit(-1);
            //}
            //else if(!(new File(args[2])).isDirectory()){
            //    System.out.println("Error: Folder "+args[2]+" does not exist.");
            //    System.exit(-2);
            //}
            //hBaseScrabble.loadTable(args[2]);
            hBaseScrabble.loadTable();
        }
        else if(args[1].toUpperCase().equals("QUERY0")){
            hBaseScrabble.query0();
        }
        else if(args[1].toUpperCase().equals("QUERY1")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query1, " +
                        "3) tourneyid 4) winnername");
                System.exit(-1);
            }

            List<String> opponentsName = hBaseScrabble.query1(args[2], args[3]);
            System.out.println("There are "+opponentsName.size()+" opponents of winner "+args[3]+" that play in tourney "+args[2]+".");
            System.out.println("The list of opponents is: "+Arrays.toString(opponentsName.toArray(new String[opponentsName.size()])));
        }
        else if(args[1].toUpperCase().equals("QUERY2")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query2, " +
                        "3) firsttourneyid 4) lasttourneyid");
                System.exit(-1);
            }
            List<String> playerNames =hBaseScrabble.query2(args[2], args[3]);
            System.out.println("There are "+playerNames.size()+" players that participates in more than one tourney between tourneyid "+args[2]+" and tourneyid "+args[3]+" .");
            System.out.println("The list of players is: "+Arrays.toString(playerNames.toArray(new String[playerNames.size()])));
        }
        else if(args[1].toUpperCase().equals("QUERY3")){
            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) tourneyid");
                System.exit(-1);
            }
            List<String> games = hBaseScrabble.query3(args[2]);
            System.out.println("There are "+games.size()+" that ends in tie in tourneyid "+args[2]+" .");
            System.out.println("The list of games is: "+Arrays.toString(games.toArray(new String[games.size()])));
        }
        else{
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }

    }



}
