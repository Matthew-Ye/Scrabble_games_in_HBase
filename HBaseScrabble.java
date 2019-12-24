import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.lang.StringUtils;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;


public class HBaseScrabble {
    private Configuration config;
    private HBaseAdmin hBaseAdmin;
    private byte[] table = Bytes.toBytes("ScrabbleGames");
    private HBaseSetUp setUp = new HBaseSetUp();

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

    public void createTable() throws IOException {
        try{
            HTableDescriptor hTable = new HTableDescriptor(TableName.valueOf(table));
            for (byte[] cf: setUp.listCF) {
                hTable.addFamily(new HColumnDescriptor(cf).setMaxVersions(10));
            }
            this.hBaseAdmin.createTable(hTable);

        } catch(TableExistsException te){
            System.err.println("Table ScrabbleGames already exists" );
        }


    }

    public void loadTable(String folder)throws IOException{
        HTable hTable = new HTable(config,table);
        String csvName = "scrabble_games.csv";
        String line = "";
        String cvsSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(folder+"/"+csvName))) {
            int lines=1;
            br.readLine();
            while ((line = br.readLine()) != null) {

                lines++;

                String[] columns = line.split(cvsSplitBy);

                String tournamentId = StringUtils.leftPad(columns[1], 4, "0");  // 0000 -4 character - 8 bytes needed from bytes 0 to 7
                String gameId = StringUtils.leftPad(columns[0], 7, "0"); // 0000000 -7 character  - 14 bytes needed
                String winnerId = StringUtils.leftPad(columns[3], 5, "0"); // 0000 -5 character   - 10 bytes needed
                String loserId = StringUtils.leftPad(columns[9], 5, "0");// 0000 -5 character  - 10 bytes needed
                // String[] values  = TournamentId , GameId , WinnerId , LoserId
                String[] values ={tournamentId,gameId,winnerId,loserId};
                int[] keyTable = {0,1,2,3};

                System.out.println(tournamentId+" - "+gameId+" - "+winnerId+" - "+loserId);
                System.out.println("Line "+line);
                System.out.println("KEY - "+new String(getKey(values,keyTable)));

                //1 character in java needs 2 bytes
                Put p = new Put(getKey(values,keyTable));

                int i=0;
                for (byte[] columnFamily: setUp.listCF) {
                    Map<String, Integer> currentMap = new HashMap<>();
                    switch (i){
                    //Tournament Game Winnes Loser
                        case 0:
                            currentMap = setUp.tournament;
                            break;
                        case 1:
                            currentMap = setUp.game;
                            break;
                        case 2:
                            currentMap = setUp.winner;
                            break;
                        case 3:
                            currentMap = setUp.loser;
                            break;
                    }

                    for (Map.Entry<String, Integer> entry : currentMap.entrySet())
                    {

                        p.add(columnFamily, Bytes.toBytes(entry.getKey()), Bytes.toBytes(columns[entry.getValue()]));
//                        System.out.println(entry.getKey() + "/" + entry.getValue());
                    }
                    i++;
                }

                hTable.put(p);

                System.out.println("\n***************** ");


            }
            System.out.println("Lines "+lines);

        } catch (IOException e) {
            e.printStackTrace();
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

    byte[] getTournamentStartKey(String tournamentId) {
        byte[] key = new byte[42];
        System.arraycopy(Bytes.toBytes(tournamentId),0,key,
                0,tournamentId.length());
        for (int i = 7; i < 42; i++){
            key[i] = (byte) - 255;
            // System.out.println("i :"+key[i]);
        }
        return key;
    }

    byte[] getTournamentEndKey(String tournamentId) {
        byte[] key = new byte[42];
        System.arraycopy(Bytes.toBytes(tournamentId),0,key,
                0,tournamentId.length());
        for (int i = 7; i < 42; i++){
            key[i] = (byte)255;
        }
        return key;
    }
    public List<String> query1(String tourneyid, String winnername) throws IOException {
        HTable hTable = new HTable(config,table);
        String tmpTourneyId = StringUtils.leftPad(tourneyid, 4, "0");
        String stopKey = StringUtils.leftPad(String.valueOf(Integer.parseInt(tourneyid) + 1), 4, "0");
        System.out.println("StartKey : "+tmpTourneyId + " StopKey : " +stopKey) ;
        Scan scan = new Scan(getTournamentStartKey(tmpTourneyId),getTournamentStartKey(stopKey));

        Filter filterByWinnerName = new SingleColumnValueFilter(Bytes.toBytes("Winner"),Bytes.toBytes("winnername"),
                CompareFilter.CompareOp.EQUAL,Bytes.toBytes(winnername));
        scan.setFilter(filterByWinnerName);
        List<String> query1 = new ArrayList<String>();
        ResultScanner rs = hTable.getScanner(scan);
        for(Result result:rs) {
            byte [] value = result.getValue(Bytes.toBytes("Loser"), Bytes.toBytes("loserid"));
            String loserId = new String(value);
            query1.add(loserId);
        }

        return query1;
    }

    public List<String> query2(String firsttourneyid, String lasttourneyid) throws IOException {
        HTable hTable = new HTable(config,table);
        List<String> query2 = new ArrayList<String>();
        Map<String, Set<Integer>> playerTourneys = new HashMap();
        // we have to scan each tourney
        for (int i = Integer.parseInt(firsttourneyid); i < Integer.parseInt(lasttourneyid); i++) {
            String tmpTourneyId = StringUtils.leftPad(String.valueOf(i), 4, "0");
            String stopKey = StringUtils.leftPad(String.valueOf(i+1), 4, "0");
            Scan scan = new Scan(getTournamentStartKey(tmpTourneyId),getTournamentStartKey(stopKey));
            ResultScanner scanner = hTable.getScanner(scan);
            Set<String> ids = new HashSet();
            for (Result res:scanner){
                String winnerid = new String(res.getValue(Bytes.toBytes("Winner"), Bytes.toBytes("winnerid")));
                String loserid = new String(res.getValue(Bytes.toBytes("Loser"), Bytes.toBytes("loserid")));
                ids.add(winnerid);
                ids.add(loserid);                                 
            }
            for(String p:ids) {
                if (playerTourneys.containsKey(p)) {
                    playerTourneys.get(p).add(i);
                } else {
                    playerTourneys.put(p, new HashSet());
                    playerTourneys.get(p).add(i);
                }
            }
        }
        for (String id : playerTourneys.keySet()) {
            if (playerTourneys.get(id).size()>1) {
                query2.add(id);
            }
        }
        return query2;
    }

    public List<String> query3(String tourneyid) throws IOException {
        HTable hTable = new HTable(config,table);
        String tmpTourneyId = StringUtils.leftPad(tourneyid, 4, "0");
        String stopKey = StringUtils.leftPad(String.valueOf(Integer.parseInt(tourneyid) + 1), 4, "0");
        Scan scan = new Scan(getTournamentStartKey(tmpTourneyId),getTournamentStartKey(stopKey));
        Filter filterTie = new SingleColumnValueFilter(Bytes.toBytes("Game"),Bytes.toBytes("tie"),
                CompareFilter.CompareOp.EQUAL,Bytes.toBytes("True"));
        scan.setFilter(filterTie);
        List<String> query3 = new ArrayList<String>();
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result res : scanner) {
            String gameId = new String(res.getValue(Bytes.toBytes("Game"), Bytes.toBytes("gameid")));
            query3.add(gameId);
        }
        return query3;
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

            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables], 3)csvsFolder");
                System.exit(-1);
            }
            else if(!(new File(args[2])).isDirectory()){
                System.out.println("Error: Folder "+args[2]+" does not exist.");
                System.exit(-2);
            }
            hBaseScrabble.loadTable(args[2]);
        }
        else if(args[1].toUpperCase().equals("QUERY1")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query1, " +
                        "3) tourneyid 4) winnername");
                System.out.println(args[0]+" "+args[1]+" "+args[2]+" "+args[3]);
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
