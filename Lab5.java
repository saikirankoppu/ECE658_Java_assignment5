import java.io.*;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class Lab5 {

  public static BufferedWriter bw = null;
  static ArrayList < String > fingertable = new ArrayList < String > ();
  static ArrayList < String > keyTable = new ArrayList < String > ();

  static int myPort;
  static int msgRcvdNum = 0;
  static int msgFwdNum = 0;
  static int msgAnsNum = 0;
  static long startTime = 0;
  static long refTime = 0;
  static long keytable_inisize = 0;

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    myPort = Integer.parseInt(args[0]);
    ServerSocket sock = new ServerSocket(myPort);
    String BSName = args[1];
    int BSport = Integer.parseInt(args[2]);
    String result = "result20_" + myPort;
    FileWriter resultname = new FileWriter(result + ".txt");
    bw = new BufferedWriter(resultname);

    try {
      InetAddress BShost = InetAddress.getByName(BSName);

      String myIp = InetAddress.getLocalHost().getHostAddress();

      Socket socket = new Socket(BShost, BSport);

      //Object for writing to the socket
      PrintWriter toServer = new PrintWriter(socket.getOutputStream(), true);

      //Object for reading socket data from the server
      BufferedReader fromServer = new BufferedReader(new InputStreamReader(socket.getInputStream()), 60000);

      int noOfNodes = 20;

      File fileName = new File("filenames.txt");
      FileReader fileReader = new FileReader(fileName);
      BufferedReader bufferedReader = new BufferedReader(fileReader);

      String line;

      ArrayList < String > arr = new ArrayList < String > ();
      //ArrayList<String> found = new ArrayList<String>();
      while ((line = bufferedReader.readLine()) != null) {

        arr.add(line);
      }

      fileReader.close();
      ArrayList < String > newarr = new ArrayList < String > ();
      ArrayList < String > checknum = new ArrayList < String > ();
      int p = 0;
      for (int i = 0; i < 7; p++) {

        Random rand = new Random();
        int min = 0;
        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((noOfNodes - min) - 1) + min;
        boolean found = checknum.contains(Integer.toString(randomNum));
        if (found == false) {
          checknum.add(Integer.toString(randomNum));
          newarr.add(arr.get(randomNum));
          i++;
        }

      }
      //adding into keytable -initializing
      for (int i = 0, j = 0; j < 7; j++) { //
        //i++;
        keyTable.add(newarr.get(j)); //file

        String hashfile = find_md5(newarr.get(j).replace(" ", "*").toLowerCase()).toString();
        hashfile = hashfile.substring(26);
        //hashfile=hashfile.substring(31);
        keyTable.add(hashfile); //key

        keyTable.add(myIp); //IP
        keyTable.add(Integer.toString(myPort)); //port
        String a[] = newarr.get(j).split(" ");
        if (a.length > 1) {
          for (int z = 0; z < a.length; z++) {
            keyTable.add(newarr.get(j)); //file
            hashfile = find_md5(a[z].toLowerCase()).toString();
            //hashfile=hashfile.substring(26);
            hashfile = hashfile.substring(26);
            keyTable.add(hashfile); //key

            keyTable.add(myIp); //ip
            keyTable.add(Integer.toString(myPort)); //port
          }

        }
      }
      Commands.keytable_inisize = keyTable.size();
      StringBuilder sendString = new StringBuilder();
      sendString.append("00** ");
      sendString.append("REG ");

      sendString.append(myIp + " ");

      sendString.append(myPort + " ");

      String md5String = myIp + myPort;

      // CALL MD5
      String hashOfIP = find_md5(md5String).toString();
      //Socket n1=null;
      hashOfIP = hashOfIP.substring(26); //network size
      //hashOfIP = hashOfIP.substring(26);//orginal
      ArrayList < String > splitArrayBS = new ArrayList < String > ();

      long decHash = Long.parseLong(hashOfIP, 16);
      long mykey = decHash;
      new Thread(new Commands(fingertable, keyTable, newarr, noOfNodes, BShost, BSport, sock, hashOfIP, splitArrayBS, socket, mykey, msgRcvdNum, msgFwdNum, msgAnsNum, startTime, refTime, keytable_inisize)).start();
      //System.out.println("dec of "+hashOfIP+"is "+decHash);
      sendString.append(hashOfIP.trim());
      //System.out.println("sendString "+sendString);
      int networkSize = 24; //++++++++

      for (int i = 0, j = 0; i < 24 * 5; i++) { // adding zeros -intializing
        //decHash=0;

        if (i % 5 == 2) {
          j++;

        }
        if (i % 5 == 0) //initial
        {
          Double x = ((decHash + Math.pow(2, j)) % Math.pow(2, networkSize));
          //System.out.println("x double "+x);

          Long y = x.longValue();
          //System.out.println("y long "+y);
          //Thread.sleep(5000);
          fingertable.add(y.toString());
        } else if (i % 5 == 1) //final
        {
          Double x = (decHash + Math.pow(2, j) + Math.pow(2, j)) % Math.pow(2, networkSize);
          Long y = x.longValue();
          fingertable.add(y.toString());

        } else if (i % 5 == 2) //sucessor
        {
          Long x = decHash;
          fingertable.add(x.toString());
        } else if (i % 5 == 3) //IP
        {
          fingertable.add(myIp);
        } else //port
          fingertable.add(args[0]);
      }
      Commands.fingerTable = fingertable;

      String sendToBS = new String();

      sendToBS = sendString.toString();

      int length = sendToBS.length();
      sendToBS = sendToBS.replace("**", Integer.toString(length));

      System.out.println("Command to BS \n" + sendToBS);
      //Sending the data to server
      toServer.println(sendToBS + "\n");

      //System.out.println("message to BS \n "+sendToBS);
      String cmdResult = null;
      cmdResult = fromServer.readLine();
      String preSplit[] = cmdResult.split(" ");

      for (int i = 0; i < preSplit.length; i++) {
        splitArrayBS.add(preSplit[i]);
      }

      int noOfpeersBS = Integer.parseInt(splitArrayBS.get(2));
      splitArrayBS.remove(0);
      splitArrayBS.remove(0);
      splitArrayBS.remove(0);
      System.out.println("Received from BS \n" + cmdResult);
      //updating fingertable
      updatefingerBS(splitArrayBS, mykey);
      //System.out.println("no of peers "+noOfNodes);
      //Calling a function to handle new node(me)
      sendInitial(splitArrayBS, noOfpeersBS, myIp, myPort, hashOfIP, noOfpeersBS, sock);

      //asking key from other nodes

      getkeyfunction(splitArrayBS, mykey);

      if (noOfpeersBS > 0) {
        addkeys(mykey, splitArrayBS, sock);

      }

      while (true) {

        System.out.println("Server begins.. ");
        Socket n1 = sock.accept();
        //Commands.socket=n1;
        BufferedReader fromClient = new BufferedReader(new InputStreamReader(n1.getInputStream()));
        PrintWriter toNode = new PrintWriter(n1.getOutputStream(), true);

        String clientMsg = fromClient.readLine();

        System.out.println("From others peers " + clientMsg);

        //take respective actions
        //Splitting the data to get the different Commands
        String[] strArray = clientMsg.split(" ");

        switch (strArray[1]) {
        case "UPFIN":
          {
            switch (strArray[2]) {
            case "0":
              { // when we receive join(UPFIN 0)node sends join request to server

                splitArrayBS.add(strArray[3]);
                splitArrayBS.add(strArray[4]);
                splitArrayBS.add(strArray[5]);

                //System.out.println("BS modified UPFIN"+splitArrayBS);

                updatefingerBS(splitArrayBS, mykey);

                String joinConfm = "00** UPFINOK 0";
                InetAddress IPjoin = InetAddress.getByName(strArray[3]);

                int Jport = Integer.parseInt(strArray[4]);
                int len = joinConfm.length();
                joinConfm = joinConfm.replace("**", Integer.toString(len));
                Socket f1 = new Socket(IPjoin, Jport);
                PrintWriter newUp = new PrintWriter(f1.getOutputStream(), true);
                newUp.println(joinConfm);
                f1.close();

                Commands.fingerTable = fingertable;
                Commands.splitArrayBS = splitArrayBS;
                //sendInitial
                n1.close();

              }

              break;
            case "1":
              { // when we receive join(UPFIN 1)node leaves ,delete that send UPFIN OK to it
                int ind = splitArrayBS.indexOf(strArray[5]);
                System.out.println("Before deleting " + splitArrayBS);
                splitArrayBS.remove(ind); //key
                String IP = splitArrayBS.get(ind - 2);
                int port = Integer.parseInt(splitArrayBS.get(ind - 1));
                splitArrayBS.remove(ind - 1); //port
                splitArrayBS.remove(ind - 2); //ip
                System.out.println("After deleting " + splitArrayBS);

                Socket soc = new Socket(IP, port);
                String upfin1 = "0014 UPFINOK 0";
                //System.out.println("Removed "+strArray[3]+" "+strArray[4]+" "+strArray[5]);
                PrintWriter p1 = new PrintWriter(soc.getOutputStream(), true);
                p1.println(upfin1);
                updatefingerBS(splitArrayBS, mykey);
                //calll a function so that the keytable deletes the leaving node
                Commands.splitArrayBS = splitArrayBS;
              }

              break;

            default:
              break;
            }

            break;
          }
        case "UPFINOK":
          {
            System.out.println(clientMsg);

            break;
          }

        case "GETKY":
          {
            System.out.println(clientMsg);
            //compute the keys and files to be sent
            getKeyReply(splitArrayBS, mykey, n1, clientMsg);

            n1.close();

            break;
          }

        case "GIVEKY":
          {
            String leavemsg[] = clientMsg.split(" ");
            int len1 = Integer.parseInt(leavemsg[2]);
            for (int i = 5, j = 0; j < len1; i += 4, j++) {
              keyTable.add(leavemsg[i + 1]); //file
              keyTable.add(leavemsg[i]); //key
              keyTable.add(leavemsg[i - 1]); //IP
              keyTable.add(leavemsg[i - 2]); //port
            }
            String reply = "0015 GIVEKYOK 0";
            toNode.println(reply);
            Commands.keyTable = keyTable;
            break;
          }
        case "GIVEKYOK":
          {

            break;
          }
        case "ADD":
          {
            System.out.println(clientMsg);
            long keyFile = Long.parseLong(strArray[4], 16);
            ArrayList < Long > SucNodes = new ArrayList < Long > ();
            long sucessor;

            int checkmyId = SucNodes.indexOf(mykey);
            if (checkmyId == -1) {
              SucNodes.add(mykey);

            }
            int indexDirect = SucNodes.indexOf(keyFile);
            //System.out.println("1");
            if (indexDirect >= 0) {
              sucessor = keyFile;
            } else //add keyfile and sort get sucessor
            { //System.out.println("2");
              //refSucNodes=SucNodes;
              SucNodes.add(keyFile);
              Collections.sort(SucNodes);
              int index = SucNodes.lastIndexOf(keyFile);
              if (index + 1 == SucNodes.size()) { //System.out.println("2.1");
                sucessor = SucNodes.get(0);
              } else { //System.out.println("2.2");
                sucessor = SucNodes.get(index + 1);
              }
            }

            //System.out.println("sucessor key"+sucessor+" "+mykey);

            if (sucessor == mykey) {
              //System.out.println("Adding");
              keyTable.add(strArray[5].replace("*", " ")); //filename
              keyTable.add(strArray[4]); //key
              keyTable.add(strArray[2]); //ip
              keyTable.add(strArray[3]); //port
              String addOk = "0012 ADDOK 0";
              String IP = strArray[2];
              int port = Integer.parseInt(strArray[3]);
              //System.out.println("IP port"+IP+" "+port);

              Socket soc = new Socket(IP, port);
              PrintWriter print = new PrintWriter(soc.getOutputStream(), true);
              print.println(addOk);

              Commands.keyTable = keyTable;
              soc.close();
            } else {
              ArrayList < Long > refSucNodes = new ArrayList < Long > ();

              for (int i = 0; i < fingertable.size(); i += 5) {
                long temp = Long.parseLong(fingertable.get(i + 2));
                refSucNodes.add(temp);
              }

              int ind = refSucNodes.lastIndexOf(sucessor);
              String IP = fingertable.get(ind * 5 + 3);
              int port = Integer.parseInt(fingertable.get(ind * 5 + 4));
              //System.out.println("forwarding IP port"+IP+" "+port);
              Socket soc = new Socket(IP, port);
              PrintWriter print = new PrintWriter(soc.getOutputStream(), true);
              print.println(clientMsg);
              //Commands.keyTable=keyTable;
              soc.close();
            }
            break;
          }
        case "ADDOK":
          {

            break;
          }
        case "UNROK":
          {
            System.out.println("From BS " + strArray);
            break;

          }

        case "SER":
          {
            msgRcvdNum++;
            Commands.msgRcvdNum = msgRcvdNum;

            String fileKeyHex = strArray[4];
            long filekeydec = Long.parseLong(fileKeyHex, 16);
            int hopCount = Integer.parseInt(strArray[5]);
            hopCount++;

            String newFwdQuery = strArray[0] + " " + strArray[1] + " " + strArray[2] + " " + strArray[3] + " " + strArray[5] + " " + hopCount;

            long fileKey = Long.parseLong(fileKeyHex, 16);
            ArrayList < Long > refsucNodes = new ArrayList < Long > ();
            ArrayList < Long > sucNodes = new ArrayList < Long > ();

            for (int r = 0; r < fingertable.size(); r += 5) {
              sucNodes.add(Long.parseLong(fingertable.get(r + 2)));
            }

            //refsucNodes=sucNodes;

            //System.out.println("refsucNodes "+refsucNodes);
            //System.out.println("sucNodes "+sucNodes);

            int check = sucNodes.indexOf(mykey);
            //int flag=0;

            if (check == -1) { //flag=1;
              sucNodes.add(mykey);
            }
            //System.out.println("suc nodes "+sucNodes);
            int dindex = sucNodes.lastIndexOf(fileKey);
            //System.out.println("dindex "+dindex);

            if (dindex >= 0) //found sucessor directly
            {
              long sucessor = fileKey;
              //System.out.println("Sucesor if dindex>=0 "+sucessor);
              //long sucessor=sucNodes.get(dindex);
              //check if sucessor is myID
              if (sucessor == mykey) //call a function to check my key table and send the rply
              {
                localsearch(n1, fileKeyHex, mykey, clientMsg, hopCount);
              } else //if(sucessor!=mykey)//call a function to forward
              {
                //nt indSuc=

                serForward(dindex, newFwdQuery, n1, fileKeyHex, mykey, hopCount);
              }

            } else if (dindex == -1) //not found directly so add key and find sucessor
            {
              for (int r = 0; r < fingertable.size(); r += 5) {
                refsucNodes.add(Long.parseLong(fingertable.get(r + 2)));
              }
              int i = refsucNodes.indexOf(mykey);

              sucNodes.add(fileKey);
              Collections.sort(sucNodes);
              int indPreSuc = sucNodes.lastIndexOf(fileKey);
              //System.out.println("unsorted "+refsucNodes);
              //System.out.println("sorted "+sucNodes);
              //S//ystem.out.println("key "+fileKey);

              long sucessor;
              //System.out.println("indPreSuc "+indPreSuc);

              if (indPreSuc + 1 == sucNodes.size()) //check if last element
              {
                sucessor = sucNodes.get(0);

                if (sucessor == mykey) {
                  localsearch(n1, fileKeyHex, mykey, clientMsg, hopCount);
                } else {

                  //System.out.println("refSucNOdes inside dindex -1 "+refsucNodes);
                  int index = refsucNodes.lastIndexOf(sucessor);
                  //System.out.println("index dindex -1 "+index);
                  //System.out.println("successor "+sucessor);
                  serForward(index, newFwdQuery, n1, fileKeyHex, mykey, hopCount);
                }
              } else if (indPreSuc + 1 != sucNodes.size()) {
                sucessor = sucNodes.get(indPreSuc + 1);
                if (sucessor == mykey) {
                  localsearch(n1, fileKeyHex, mykey, clientMsg, hopCount);
                } else { //System.out.println("successor "+sucessor);

                  int index = refsucNodes.indexOf(sucessor);
                  serForward(index, newFwdQuery, n1, fileKeyHex, mykey, hopCount);
                }
              }
            }

            break;

          } //end of SER
        case "SEROK":
          System.out.println(clientMsg);

          long latency = (System.currentTimeMillis() - Commands.startTime) / 1000;
          System.out.println("Latency :" + latency);

          bw.append(clientMsg + "\n");
          //Append the hop count
          bw.flush();

          break;
        }
      }

    } catch (Exception Ex) {
      Ex.printStackTrace();
    }

  }
  public static StringBuffer find_md5(String name) throws Exception {
    //String yourString= "hii";
    byte[] bytesOfMessage = name.getBytes("UTF-8");

    MessageDigest md = MessageDigest.getInstance("MD5");
    byte[] thedigest = md.digest(bytesOfMessage);
    //System.out.println("MD5sds "+thedigest);
    StringBuffer key = new StringBuffer();
    for (int i = 0; i < thedigest.length; i++) {
      key.append(Integer.toString((thedigest[i] & 0xff) + 0x100, 16)
        .substring(1));
    }
    return key;
  }
  public static void printfingertable() {
    System.out.println("finger table");
    System.out.println("Initial Final  Sucessor   Ip    Port");
    for (int i = 0; i < fingertable.size(); i += 5) {
      //System.out.println("finger size "+fingertable.size());
      System.out.println(fingertable.get(i) + "        " + fingertable.get(i + 1) + "        " + fingertable.get(i + 2) + "        " + fingertable.get(i + 3) + "       " + fingertable.get(i + 4));
    }

  }

  public static void printkeytable() {
    System.out.println("key table");
    System.out.println("File 			  Key 					  Ip  	  Port");
    for (int i = 0; i < keyTable.size(); i += 4) {
      //System.out.println("finger size "+fingertable.size());
      System.out.println(keyTable.get(i) + "    " + keyTable.get(i + 1) + "        " + keyTable.get(i + 2) + "        " + keyTable.get(i + 3));
    }

  }

  public static void updatefingerBS(ArrayList < String > fromBS, long mykey) throws Exception {

    //System.out.println("string bs after 3"+fromBS+"  nopeers"+fromBS.size()/3);
    // fromBS contains only IP port key..
    if (fromBS.size() / 3 == 0) {
      String IP = InetAddress.getLocalHost().getHostAddress().toString();
      for (int i = 0; i < 24 * 5; i++) { // adding zeros -intializing
        //decHash=0;

        if (i % 5 == 3) //IP
        {
          fingertable.set(i, IP); //IP
        }
        if (i % 5 == 4) //port
          fingertable.set(i, Integer.toString(myPort));
      }

    }

    if (fromBS.size() / 3 != 0) {
      // multiple keys given by BS
      //ArrayList<Integer> copykeysBS=new ArrayList<Integer>();
      ArrayList < Long > keysBS = new ArrayList < Long > ();
      for (int i = 0; i < fromBS.size(); i += 3) {
        Long temKey = Long.parseLong(fromBS.get(i + 2), 16);
        Long tempkey1 = temKey;
        keysBS.add(tempkey1);
      }
      //add our own key
      keysBS.add(mykey);
      //String myKeyhex=Long.toHexString(mykey);
      //copykeysBS=keysBS;
      //sort this keyBS

      Collections.sort(keysBS);

      //access each start,check if that is already present in sortkeys,if it present modify
      for (int i = 0; i < fingertable.size(); i += 5) {
        long start = Long.parseLong(fingertable.get(i + 0));
        int find = keysBS.indexOf(start);
        if (find >= 0 && start != mykey) //added&&
        {
          String findkey = Long.toHexString(start);
          int index = fromBS.indexOf(findkey);
          //String key=fromBS.get(index);

          String port = fromBS.get(index - 1);
          String IP = fromBS.get(index - 2);
          fingertable.set(i + 2, fingertable.get(i)); //key to sucessor
          fingertable.set(i + 3, IP); //IP 
          fingertable.set(i + 4, port); //IP 

        }
        if (find == -1) { //add start to the sorting array and sort again
          keysBS.add(start);
          Collections.sort(keysBS);

          String findkey = Long.toHexString(start);

          int index = keysBS.lastIndexOf(start);
          int length = keysBS.size();
          //System.out.println("index and length"+index+"  "+length);
          //System.out.println("sorted "+keysBS);

          if (index + 1 == length) // start is at last(MAX)
          {
            String findkey1 = Long.toHexString(keysBS.get(0));
            int indexBS = fromBS.indexOf(findkey1);
            if (indexBS >= 0) {
              String key = Long.toString(keysBS.get(0));
              fingertable.set(i + 2, key); //key to sucessor

              String port = fromBS.get(indexBS - 1);
              String IP = fromBS.get(indexBS - 2);
              fingertable.set(i + 3, IP); //IP 
              fingertable.set(i + 4, port); //IP 

              int rem = keysBS.indexOf(start);
              if (rem >= 0) {
                keysBS.remove(rem);
              }
              //keysBS.remove(start);
            } else {}
          } else // start in middle
          {
            //System.out.println("sorted key"+keysBS);
            int index1 = keysBS.indexOf(start);
            //System.out.println(start+"index "+index1);
            long key = keysBS.get(index1 + 1);

            if (key != mykey) {
              fingertable.set(i + 2, Long.toString(key)); //key to sucessor
              String hexKey = Long.toHexString(key);

              //System.out.println("HexKEy "+hexKey);
              //System.out.println("length og BS"+fromBS.size());
              int BSindex = fromBS.indexOf(hexKey);
              if (BSindex >= 0) {
                String port = fromBS.get(BSindex - 1);
                String IP = fromBS.get(BSindex - 2);
                fingertable.set(i + 3, IP); //IP 
                fingertable.set(i + 4, port); //IP
              }
              //keysBS.remove(start);
            }
          }

        }
        int rem = keysBS.indexOf(start);
        if (rem >= 0) {
          keysBS.remove(rem);
        }
      }

      //printfingertable();
      //access each start element and modify
      Commands.fingerTable = fingertable;

    }
  }

  public static void sendInitial(ArrayList < String > splitArrayBS, int noOfpeersBS, String myIP, int myPort, String key, int length, ServerSocket sock) throws Exception {
    if (length == 9999) {
      System.out.println("Failed, there is some error in the command");
      System.exit(0);
    } else if (length == 9998) {
      System.out.println("Failed, already in the register, unregister first");
      System.exit(0);
    } else if (length == 9997) {
      System.out.println("Failed, cant register. BS full");
      System.exit(0);
    } else {
      length = splitArrayBS.size() / 3;
      if (noOfpeersBS > 0) {
        String updateComm = new String();
        updateComm = "00** " + "UPFIN 0 " + myIP + " " + myPort + " " + key;

        int replaceLen = updateComm.length();
        updateComm = updateComm.replace("**", Integer.toString(replaceLen));
        System.out.println("UPFIN Command \n " + updateComm);

        for (int i = 0; i < length * 3; i = i + 3) {
          String Iptosend = splitArrayBS.get(i);
          int porttoSend = Integer.parseInt(splitArrayBS.get(i + 1));

          Socket toUpdate = new Socket(Iptosend, porttoSend);
          PrintWriter toClient = new PrintWriter(toUpdate.getOutputStream(), true);
          toClient.println(updateComm);
          toUpdate.close();

          Socket n2 = sock.accept();
          BufferedReader fromClient = new BufferedReader(new InputStreamReader(n2.getInputStream()));

          String client = fromClient.readLine();
          System.out.println(client);
          n2.close();
          fromClient.close();
        }
        // handle all intial things which are required while joining
        //getting sucessor,asking for keys and all..
      }
    }

  }

  public static void getkeyfunction(ArrayList < String > fromBS, long myKey) throws Exception { //ArrayList<Long> keysBS=new ArrayList<Long>();

    if (fromBS.size() / 3 != 0) {
      // multiple keys given by BS
      ArrayList < Long > keysBS = new ArrayList < Long > ();
      for (int i = 0; i < fromBS.size(); i += 3) {
        Long temKey = Long.parseLong(fromBS.get(i + 2), 16);
        Long tempkey1 = temKey;
        keysBS.add(tempkey1);
      }
      //add our own key
      keysBS.add(myKey);
      //copykeysBS=keysBS;
      //sort this keyBS

      Collections.sort(keysBS);

      int sIndex = keysBS.lastIndexOf(myKey);
      long sucessor;
      int len = keysBS.size();
      if (sIndex + 1 == len) // if its last element then get the 1st 
      {
        sucessor = keysBS.get(0);
      } else {
        sucessor = keysBS.get(sIndex + 1);
      }
      //send getkey to sucessor NOW.

      int index = fromBS.indexOf(Long.toHexString(sucessor));
      //System.out.println("FROMBS BS "+fromBS);
      //System.out.println("FROMBS GETKY "+index);

      InetAddress IP = InetAddress.getByName(fromBS.get(index - 2));

      //System.out.println("GETKY IP "+IP);

      int port = Integer.parseInt(fromBS.get(index - 1));
      // all keys should be LONG
      //System.out.println("GETKY PORT "+port);
      Socket socket = new Socket(IP, port);

      String getkey = "00** GETKY" + " " + Long.toHexString(myKey);
      int len1 = getkey.length();
      getkey = getkey.replace("**", Integer.toString(len1));
      //System.out.println("Get key to IP:port"+IP+":"+port);
      System.out.println("GETKY Command \n" + getkey);

      PrintWriter sucString = new PrintWriter(socket.getOutputStream(), true);
      sucString.println(getkey);

      // handle key reply...
      Thread.sleep(1000);
      BufferedReader sucKeys = new BufferedReader(new InputStreamReader(socket.getInputStream()), 60000);
      String getkeys = sucKeys.readLine();
      System.out.println("Reply to GETKY \n " + getkeys);
      String[] updatekeys = getkeys.split(" ");
      int noOfKeys = Integer.parseInt(updatekeys[2]);
      // now add the keys and IP port to keytable
      for (int i = 0; i < (noOfKeys * 4); i += 4) {
        keyTable.add(updatekeys[i + 6].replace("*", " ")); // add filename
        keyTable.add(updatekeys[i + 5]); // add key
        keyTable.add(updatekeys[i + 3]); // add IP
        keyTable.add(updatekeys[i + 4]); // add port

      }
      Commands.keyTable = keyTable;
      socket.close();
      sucKeys.close();
    }
  }

  public static void addkeys(long myID, ArrayList < String > fromBS, ServerSocket sock) throws Exception {
    PrintWriter sendAddKey = null;
    BufferedReader addok = null;

    //add our own key
    ArrayList < Long > refSucNodes = new ArrayList < Long > ();
    for (int i1 = 0; i1 < fingertable.size(); i1 += 5) {
      Long temKey = Long.parseLong(fingertable.get(i1 + 2));

      refSucNodes.add(temKey);
    }
    //System.out.println("1");
    for (int i = 0; i < keyTable.size(); i += 4) { //get each key'
      ArrayList < Long > SucNodes = new ArrayList < Long > ();
      for (int i1 = 0; i1 < fingertable.size(); i1 += 5) {
        Long temKey = Long.parseLong(fingertable.get(i1 + 2));
        //Long tempkey1=temKey.longValue();
        SucNodes.add(temKey);
      }
      //System.out.println("2");
      int checkmyId = SucNodes.indexOf(myID);
      if (checkmyId == -1)
        SucNodes.add(myID);

      String addkey = "00** ADD " + keyTable.get(i + 2) + " " + keyTable.get(i + 3) + " " + keyTable.get(i + 1) + " " + keyTable.get(i).replace(" ", "*");
      int len = addkey.length();
      addkey = addkey.replace("**", Integer.toString(len));

      long KeyfileDec = Long.parseLong(keyTable.get(i + 1), 16);

      int in = SucNodes.lastIndexOf(KeyfileDec); //last.......
      //System.out.println("key dec "+KeyfileDec);
      long sucessor;
      if ( in >= 0) {
        sucessor = KeyfileDec;
        if (sucessor != myID) //send it to sucessor
        {
          System.out.println("Adding key to the network \n" + addkey);
          String IP = fingertable.get( in * 5 + 3);
          int port = Integer.parseInt(fingertable.get( in * 5 + 4));
          //System.out.println("sending to sucessor"+sucessor+"  "+IP+":"+port+"\n addkey"+addkey);
          Socket soc = new Socket(IP, port);
          sendAddKey = new PrintWriter(soc.getOutputStream(), true);
          sendAddKey.println(addkey);
          //System.out.println("1");
          //receive add OK and close

          //addok=new BufferedReader(new InputStreamReader(soc.getInputStream(),5000));
          //addok=new BufferedReader(new InputStreamReader(soc.getInputStream()),6000);
          //System.out.println(addok.readLine());
          soc.close();

          Socket n2 = sock.accept();
          BufferedReader fromClient = new BufferedReader(new InputStreamReader(n2.getInputStream()));

          String client = fromClient.readLine();
          System.out.println(client);
          n2.close();
          fromClient.close();
        }

      } else //not found directly
      { //System.out.println("indirect");

        SucNodes.add(KeyfileDec); //adding key
        //System.out.println("ref nodes "+refSucNodes);
        Collections.sort(SucNodes);
        int sucIndex = SucNodes.lastIndexOf(KeyfileDec); //last.......
        //System.out.println("sucNodes "+SucNodes);
        //System.out.println("index of Keyfile "+sucIndex);
        if (sucIndex + 1 == SucNodes.size()) //last element
        {
          sucessor = SucNodes.get(0);
        } else {
          sucessor = SucNodes.get(sucIndex + 1);
        }

        if (sucessor != myID) { //System.out.println("Adding key to the network \n"+addkey);	
          int ind = refSucNodes.lastIndexOf(sucessor);
          //System.out.println("123.. ind"+ind);
          String IP = fingertable.get(ind * 5 + 3);
          //System.out.println("IP");
          int port = Integer.parseInt(fingertable.get(ind * 5 + 4));
          //System.out.println("port");
          //System.out.println("sending to sucessor"+sucessor+"  "+IP+":"+port+"\n addkey"+addkey);
          Socket soc = new Socket(IP, port);
          sendAddKey = new PrintWriter(soc.getOutputStream(), true);
          sendAddKey.println(addkey);
          System.out.println("Adding keys to the network \n" + addkey);
          //System.out.println("3");
          //receive add OK and close

          //addok=new BufferedReader(new InputStreamReader(soc.getInputStream(),5000));
          //addok=new BufferedReader(new InputStreamReader(soc.getInputStream()),6000);
          //System.out.println(addok.readLine());
          soc.close();

          Socket n2 = sock.accept();
          BufferedReader fromClient = new BufferedReader(new InputStreamReader(n2.getInputStream()));

          String client = fromClient.readLine();
          System.out.println(client);
          n2.close();
          fromClient.close();

        }
      }
      Thread.sleep(1000);
    }
  }

  public static void getKeyReply(ArrayList < String > fromBS, long myKey, Socket send, String fromClient) throws Exception {
    ArrayList < Long > nodesBS = new ArrayList < Long > ();
    ArrayList < Long > preSortkeysBS = new ArrayList < Long > ();

    /*for(int i=0;i<fromBS.size();i+=3)
    {
    	Long temKey=Long.parseLong(fromBS.get(i+2),16);
    	Long tempkey1=temKey.longValue();
    	nodesBS.add(tempkey1);
    }*/
    for (int i = 0; i < fingertable.size(); i += 5) {
      Long temKey = Long.parseLong(fingertable.get(i + 2));
      Long tempkey1 = temKey;
      nodesBS.add(tempkey1);
    }

    nodesBS.add(myKey); // got all keys in nodesBS
    long sucessor;
    int len = 0;
    StringBuilder Keytosend = new StringBuilder();
    String preb = "00** GETKYOK ";
    for (int i = 0, j = 0; i < keyTable.size(); i += 4, j++) {

      //long checkKey=newKey;

      long abc = Long.parseLong(keyTable.get(j * 4 + 1), 16);
      int index = nodesBS.indexOf(abc);
      if (index >= 0) {
        if (index + 1 == nodesBS.size()) {
          sucessor = nodesBS.get(0);

        } else {
          sucessor = nodesBS.get(index + 1);
        }
      } else { //add filekey
        nodesBS.add(abc);
        Collections.sort(nodesBS);
        int ind = nodesBS.lastIndexOf(abc);
        if (ind + 1 == nodesBS.size()) {
          sucessor = nodesBS.get(0);
        } else {
          sucessor = nodesBS.get(ind + 1);
        }
      }
      String[] getkyClient = fromClient.split(" ");
      long newKey_ask = Long.parseLong(getkyClient[2], 16);
      if (sucessor == newKey_ask) {
        len += 1;
        Keytosend.append(keyTable.get(j * 4 + 2) + " "); //ip
        Keytosend.append(keyTable.get(j * 4 + 3) + " "); //port
        Keytosend.append(keyTable.get(j * 4 + 1) + " "); //key
        String filename = keyTable.get(j * 4);
        filename = filename.replace(" ", "*");
        Keytosend.append(filename + " ");
      }

      //send.close();

    }
    preb = preb + len + " " + Keytosend;
    len = preb.length();
    preb = preb.replace("**", Integer.toString(len));
    System.out.println("GETKY reply string: " + preb);
    PrintWriter getkyok = new PrintWriter(send.getOutputStream(), true);
    getkyok.println(preb);
    send.close();
    getkyok.close();

  }

  public static void serForward(int indSuc, String clientMsg, Socket n1, String hexfile, long mykey, int hop) throws Exception {
    msgFwdNum++;
    Commands.msgFwdNum = msgFwdNum;

    String IP = fingertable.get(indSuc * 5 + 3); //IP
    int port = Integer.parseInt(fingertable.get(indSuc * 5 + 4)); //port
    Socket soc = new Socket(IP, port);
    /*if(port==myPort)
    {
    	localsearch(n1,hexfile,mykey);	
    }*/
    //else
    //{
    //System.out.println("IP port sending"+IP+"  "+port);
    PrintWriter serokReply = new PrintWriter(soc.getOutputStream(), true);
    serokReply.println(clientMsg); //forwarding to sucessorclientMsg
    soc.close();
    //}
  }

  public static void localsearch(Socket n1, String hexfile, long mykey, String clientMsg, int hop) throws Exception {
      String client[] = clientMsg.split(" ");
      String serreply = "00** SEROK";
      StringBuilder serpart2 = new StringBuilder();
      ArrayList < Long > keyOfKeytable = new ArrayList < Long > ();

      long fileDec = Long.parseLong(hexfile, 16);

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

          if (key == fileDec) { //System.out.println("match");
            serpart2.append(keyTable.get(i + 0) + " "); //filename
            serpart2.append(keyTable.get(i + 2) + " "); //IP
            serpart2.append(keyTable.get(i + 3) + " "); //Port
            fileno++;
          }

          //System.out.println("ds "+serpart2);

        }
      }

      msgAnsNum++;
      Commands.msgAnsNum = msgAnsNum;

      serreply = serreply + " " + fileno + " " + serpart2 + " " + hop;
      int l = serreply.length();
      serreply = serreply.replace("**", Integer.toString(l));
      System.out.println("serok reply " + serreply);

      Socket serRepl = new Socket(client[2], Integer.parseInt(client[3]));
      PrintWriter toNode = new PrintWriter(serRepl.getOutputStream(), true);
      toNode.println(serreply); //sending the result back (found not found is taken care)

      toNode.close();

      serRepl.close();

    }
    //add functions here

}