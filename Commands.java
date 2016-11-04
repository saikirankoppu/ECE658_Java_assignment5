import java.util.*;
import java.io.*;
import java.net.*;

public class Commands implements Runnable {
  public static ArrayList < String > fingerTable;
  public static ArrayList < String > splitArrayBS;
  public static ArrayList < String > keyTable;
  private ServerSocket commSocket;
  private ArrayList < String > newarr;
  private int noOfNodes;
  private InetAddress IPbootstrap;
  private int Bootstrap_Port_number;
  private String hexkey;
  public static Socket socket;
  private long myID;
  public static int msgRcvdNum;
  public static int msgFwdNum;
  public static int msgAnsNum;
  public static long startTime;
  public static long refTime;
  public static long keytable_inisize;

  //Constructor for the main class
  public Commands(ArrayList < String > fingerTable, ArrayList < String > keyTable, ArrayList < String > newarr, int noOfNodes, InetAddress IPbootstrap, int Bootstrap_Port_number, ServerSocket commSocket, String hexkey, ArrayList < String > splitArrayBS, Socket socket, long myID, int msgRcvdNum, int msgFwdNum, int msgAnsNum, long startTime, long refTime, long keytable_inisize) throws IOException {
    this.fingerTable = fingerTable;
    this.keyTable = keyTable;
    this.commSocket = commSocket;
    this.noOfNodes = noOfNodes;
    this.IPbootstrap = IPbootstrap;
    this.Bootstrap_Port_number = Bootstrap_Port_number;
    this.newarr = newarr;
    this.hexkey = hexkey;
    this.splitArrayBS = splitArrayBS;
    this.socket = socket;
    this.myID = myID;
    this.msgRcvdNum = msgRcvdNum;
    this.msgFwdNum = msgFwdNum;
    this.msgAnsNum = msgAnsNum;
    this.startTime = startTime;
    this.refTime = refTime;
    this.keytable_inisize = keytable_inisize;
  }

  public void run() {

    try {
      //Socket socket = new Socket(IPbootstrap, Bootstrap_Port_number);
      //System.out.println("BS thred"+IPbootstrap+" "+Bootstrap_Port_number);
      PrintWriter sendBS = new PrintWriter(socket.getOutputStream(), true);

      System.out.println("Enter a command :");
      Scanner scan = new Scanner(System.in);

      int hopCount = 0;
      //Queries for sending

      File Queryfile = new File("queries.txt");
      FileReader queryReader = new FileReader(Queryfile);

      BufferedReader queryBuffer = new BufferedReader(queryReader);

      String queryLine;
      //String file = new String("The");
      //StringBuffer query = new StringBuffer("Life ");

      String[] queryArray = null;

      ArrayList < String > queryList = new ArrayList < String > ();
      //ArrayList<String> found = new ArrayList<String>();

      while ((queryLine = queryBuffer.readLine()) != null) {

        queryList.add(queryLine);
      }
      queryReader.close();
      //Queries

      while (true) {

        InetAddress myIP = InetAddress.getLocalHost();
        String IP = InetAddress.getLocalHost().getHostAddress();
        //System.out.println("IP  IP"+IP);
        //Scanner scan = new Scanner(System.in);
        String command = scan.nextLine();

        //5 Random filenames

        switch (command) {
        case "details":

          System.out.println("My IP address " + myIP.getLocalHost().getHostAddress() + " My port " + commSocket.getLocalPort());
          break;

        case "fingertable":
          lab5.printfingertable();
          System.out.println("Size of the fingertable is :" + fingerTable.size() / 5);
          break;

        case "keytable":
          lab5.printkeytable();
          System.out.println("Size of the keytable is :" + keyTable.size() / 4);
          break;

        case "files":
          System.out.println("The filenames present at this node are :\n" + newarr);
          break;

        case "search":

          //refTime=startTime;
          for (int q = 0; q < 60; q++) //2->noOfNodes
          {
            startTime = System.currentTimeMillis();
            lab5.startTime = startTime;

            System.out.println("The current query element is :" + queryList.get(q));
            //System.out.println("Some");
            //String some="Some";//queryList.get(q).
            StringBuffer queryHash = lab5.find_md5(queryList.get(q).toLowerCase().replace(" ", "*"));
            //System.out.println("some "+some);
            //String queryHashfinal=queryHash.substring(26);
            String queryHashfinal = queryHash.substring(26);
            String serForward = "00** SER " + InetAddress.getLocalHost().getHostAddress() + " " + commSocket.getLocalPort() + " " + queryHashfinal + " " + hopCount;
            int lenSER = serForward.length();
            serForward = serForward.replace("**", Integer.toString(lenSER));

            //long queryHashdec=Long.parseLong(queryHashfinal,16);

            ArrayList < Long > preNodes = new ArrayList < Long > ();

            ArrayList < Long > SucNodes = new ArrayList < Long > ();

            for (int j = 0; j < fingerTable.size(); j += 5) {
              long sucessor2 = Long.parseLong(fingerTable.get(j + 2));
              SucNodes.add(sucessor2);

            }

            //SucNodes_Ind=SucNodes;
            //preNodes=SucNodes;

            int check = SucNodes.indexOf(myID);
            if (check == -1) {
              SucNodes.add(myID);
            }

            long queryfileDec = Long.parseLong(queryHashfinal, 16);

            System.out.println("The current query key is " + queryfileDec);

            int in = SucNodes.indexOf(queryfileDec);
            //System.out.println("in-direct "+in);

            long sucessor;
            if ( in >= 0) {
              sucessor = queryfileDec;
              /*if(in+1==SucNodes.size())
              {sucessor=SucNodes.get(0);
              	
              }
              else
              {
              	sucessor=SucNodes.get(in+1);
              }*/
            } else { //add queryfileDec

              SucNodes.add(queryfileDec);

              Collections.sort(SucNodes);

              //System.out.println("SucNodes "+SucNodes);

              in = SucNodes.lastIndexOf(queryfileDec);
              if ( in +1 == SucNodes.size()) {
                sucessor = SucNodes.get(0);
              } else {
                sucessor = SucNodes.get( in +1);
              }
            }

            //lab5.printfingertable();
            //System.out.println("Successor "+sucessor+" myID "+myID);
            for (int j = 0; j < fingerTable.size(); j += 5) {
              long sucessor1 = Long.parseLong(fingerTable.get(j + 2));

              preNodes.add(sucessor1);

            }

            //System.out.println("PreNodes "+preNodes);

            if (sucessor == myID) { //search locally
              serLocal(queryfileDec);
            } else {
              int index;
              //System.out.println("preSuc "+preNodes);
              index = preNodes.indexOf(sucessor);
              //index--;
              //System.out.println("index for FT "+index+" suc "+sucessor);
              //forward
              int IPSend = (index * 5) + 3;

              //String IP1=fingerTable.get(index*5+3);
              String IP1 = fingerTable.get(IPSend);

              int portSend = (index * 5) + 4;
              int port1 = Integer.parseInt(fingerTable.get(portSend));
              //System.out.println("IP1 port"+IP1+" "+port1);
              //int port1=Integer.parseInt(fingerTable.get(index*5+4));
              Socket soc = new Socket(IP1, port1);
              PrintWriter serFor = new PrintWriter(soc.getOutputStream(), true);

              //System.out.println("Forwarding string "+serForward);

              serFor.println(serForward);
              Thread.sleep(1000);
              //BufferedReader serReply=new BufferedReader(new InputStreamReader(soc.getInputStream(),5000));
              //BufferedReader serReply=new BufferedReader(new InputStreamReader(soc.getInputStream()),60000);
              //System.out.println(serReply.readLine());
            }

          } //query ends

          break;

        case "exit":
          {
            //givekey	& update FT /others	
            String givky = new String("00** GIVEKY ");
            int noOfkeys = 0;
            //give key'
            ArrayList < Long > sucNodes = new ArrayList < Long > ();
            for (int i = 0; i < fingerTable.size(); i += 5) {
              sucNodes.add(Long.parseLong(fingerTable.get(i + 2)));
            }
            int check = sucNodes.indexOf(myID);
            if (check == -1) {
              sucNodes.add(myID);
            }
            //ArrayList<Long> Suc_sucNodes=sucNodes;
            check = sucNodes.indexOf(myID);
            ArrayList < Long > preSuc = sucNodes;
            Collections.sort(sucNodes);

            /*long predec;//for getting keys
            if(check==0)
            {predec=sucNodes.get(sucNodes.size()-1);
            }
            else
            {
            	predec=sucNodes.get(check-1);
            }*/

            long sucessor; //for sending

            check = sucNodes.lastIndexOf(myID);
            if (check + 1 == sucNodes.size()) {
              sucessor = sucNodes.get(0);

            } else {
              sucessor = sucNodes.get(check + 1);

            }
            int index = preSuc.indexOf(sucessor);

            String IP1 = fingerTable.get(index * 5 + 3);
            int port1 = Integer.parseInt(fingerTable.get(index * 5 + 4));

            //select keys from keytable
            StringBuffer buf = new StringBuffer();
            int l = (int) keytable_inisize;
            for (; l < keyTable.size(); l += 4) {
              {
                noOfkeys++;
                buf.append(keyTable.get(l + 2)); //IP
                buf.append(" ");
                buf.append(keyTable.get(l + 3)); //port
                buf.append(" ");
                buf.append(keyTable.get(l + 1)); //key
                buf.append(" ");
                buf.append(keyTable.get(l).replace(" ", "*")); //file
                buf.append(" ");
              }
            }

            //myID
            //for(int i=0;i<keyTable.size();i+=4)
            //{
            //long key=Long.parseLong(keyTable.get(i+1),16);
            /*if(key==myID)
            {
            	noOfkeys++;
            	buf.append(keyTable.get(i+2));//IP
            	buf.append(" ");
            	buf.append(keyTable.get(i+3));//port
            	buf.append(" ");
            	buf.append(keyTable.get(i+1));//key
            	buf.append(" ");
            	buf.append(keyTable.get(i).replace(" ", "*"));//file
            	buf.append(" ");
            }*/

            givky = givky + noOfkeys + " " + buf;
            int len = givky.length();
            givky = givky.replace("**", Integer.toString(len));
            Socket soc = new Socket(IP1, port1);
            PrintWriter p2 = new PrintWriter(soc.getOutputStream(), true);
            p2.println(givky);
            Thread.sleep(1000);
            BufferedReader repllv = new BufferedReader(new InputStreamReader(soc.getInputStream()), 5000);
            System.out.println(repllv.readLine());
            break;
          }
          //informing nodes
        case "upfin1":
          {
            String leave = "00** UPFIN 1 ";
            leave = leave + myIP + " " + commSocket.getLocalPort() + " " + hexkey;
            int length = leave.length();

            leave = leave.replace("**", Integer.toString(length));
            //inform peers 
            System.out.println("leave " + leave);
            int len2 = splitArrayBS.size() / 3;

            for (int i = 0; i < len2; i++) {
              String ipUreg = splitArrayBS.get(i * 3 + 0);
              int portUreg = Integer.parseInt(splitArrayBS.get(i * 3 + 1));
              //String key=splitArrayBS.get(i*3+2);
              Socket socPeerUnr = new Socket(ipUreg, portUreg);
              PrintWriter p1 = new PrintWriter(socPeerUnr.getOutputStream(), true);
              p1.println(leave);
              p1.close();
              socPeerUnr.close();
              Socket n1 = commSocket.accept();
              BufferedReader replUng = new BufferedReader(new InputStreamReader(n1.getInputStream()), 5000);
              System.out.println(replUng.readLine());
              replUng.close();
              n1.close();
            }

            break;
          }
        case "UNREG":
          //changed k

          String unreg = "00** UNREG";
          unreg = unreg + " " + hexkey;
          int len = unreg.length();
          unreg = unreg.replace("**", Integer.toString(len));
          System.out.println("unreg " + unreg);
          Socket soc = new Socket(IPbootstrap, Bootstrap_Port_number);
          PrintWriter bsUnreg = new PrintWriter(soc.getOutputStream(), true);
          bsUnreg.println(unreg + "\n");
          //bsUnreg.close();
          Thread.sleep(1000);
          BufferedReader replyBs = new BufferedReader(new InputStreamReader(soc.getInputStream()), 5000);
          System.out.println(replyBs.readLine());
          //soc.close();
          break;

          /*case "exitall":
          	String printMsg=new String("PRINT adarsh_kulkarni");
          	byte[] byteprint=new byte[500];
          	byteprint=printMsg.getBytes();
          	
          	break;*/

        case "messages":

          System.out.println("Messages Received " + msgRcvdNum);
          System.out.println("Messages Forwarded " + msgFwdNum);
          System.out.println("Messages Answered " + msgAnsNum);

          break;
          //Write a Case for a node leaving the node 
        default:
          System.out.println("Incorrect command entered");

          break;
        }
      }
      //function

    } catch (Exception Ex) {
      Ex.printStackTrace();
    }

  }

  private void serLocal(long fileDec) { //System.out.println("local");
    String serreply = "00** SEROK ";
    StringBuilder serpart2 = new StringBuilder();
    ArrayList < Long > keyOfKeytable = new ArrayList < Long > ();

    for (int i = 0; i < keyTable.size(); i += 4) {
      keyOfKeytable.add(Long.parseLong(keyTable.get(i + 1), 16));
    }

    int findex = keyOfKeytable.indexOf(fileDec);
    //System.out.println("findex "+findex);

    int fileno = 0;

    //System.out.println("keyOfKeytable "+keyOfKeytable);

    //int lastindex=keyOfKeytable.lastIndexOf(mykey);
    if (findex >= 0) {
      for (int i = 0; i < keyTable.size(); i += 4) {
        //int indx=findex*4;
        long key = Long.parseLong(keyTable.get(i + 1), 16);
        //System.out.println("index key "+i+" "+key);
        //lab5.printkeytable();

        //System.out.println("key "+key+"fileDec "+fileDec);

        if (key == fileDec) {
          serpart2.append(keyTable.get(i + 0) + " "); //filename
          serpart2.append(keyTable.get(i + 2) + " "); //IP
          serpart2.append(keyTable.get(i + 3) + " "); //Port
          fileno++;
        }

        //System.out.println("ds "+serpart2);

      }
    }
    serreply = serreply + fileno + " " + serpart2 + " 0";
    int l = serreply.length();
    serreply = serreply.replace("**", Integer.toString(l));
    System.out.println(serreply);
  }

}