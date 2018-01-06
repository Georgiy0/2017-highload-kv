package ru.mail.polis.Kubrin;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Executors;

public class KeyValueStorageEnlightenNode implements KVService {
    private final int timeout = 500;
    private Set<String> deleted;
    private HttpServer server;
    private Dao serverDao;
    private File valuesDirectory;
    private Set<String> clusterTopology;
    private int defaultAck, defaultFrom;
    private int thisNodeNumInTopology;
    private int thisNode;
    private class GETresponse {
        public GETresponse(int code, byte[] data) {
            this.code = code;
            this.data = data;
        }
        public int code;
        public byte[] data;
    };

    public KeyValueStorageEnlightenNode(final int port,
                           @NotNull final File data,
                           @NotNull final String nodeAddressInTopology,
                           @NotNull final Set<String> topology) throws IOException {
        System.out.println(topology);
        // a hash set that keeps the information about the deleted entities.
        this.deleted = new HashSet<String>();
        this.valuesDirectory = data;
        this.clusterTopology = topology;
        // determine the number of current node in the topology.
        int temp = 0;
        for(Iterator<String> it = clusterTopology.iterator(); it.hasNext();) {
            String address = it.next();
            if(address.equals(nodeAddressInTopology)) {
                thisNodeNumInTopology = temp;
                break;
            }
            temp++;
        }
        // precomputed default values of ack and from for the given cluster topology
        // (dynamic changes to cluster topology are not implemented).
        defaultFrom = clusterTopology.size();
        defaultAck = (defaultFrom / 2)+1;
        serverDao = new DaoFiles(data);
        server = HttpServer.create();
        server.setExecutor(Executors.newCachedThreadPool());
        server.bind(new InetSocketAddress(port), 0);
        // implementation of storage REST API.
        server.createContext("/v0/entity", httpExchange -> {
            String requestMethod = httpExchange.getRequestMethod();
            String requestParameter = httpExchange.getRequestURI().getQuery();
            String parameterID = requestParameter;
            String parameterReplicas;
            int ack = defaultAck;
            int from = defaultFrom;
            // boolean variable local is set to true if the request URL contains parameter local=true.
            // This request parameter is used in order to differ inter-cluster request from external requests to the cluster.
            boolean local = false;
            // parse replicas and local request parameters.
            if(requestParameter.contains("&")) {
                String[] parameterReplicasSplit = requestParameter.split("&");
                parameterID = parameterReplicasSplit[0];
                parameterReplicas = parameterReplicasSplit[1];
                parameterReplicasSplit = parameterReplicas.split("=");
                if(parameterReplicasSplit.length != 1 && parameterReplicasSplit[0].equals("replicas")) { // check replicas
                    parameterReplicas = parameterReplicasSplit[1];
                    if(parameterReplicas.contains("/")){
                        parameterReplicasSplit = parameterReplicas.split("/");
                        ack = Integer.parseInt(parameterReplicasSplit[0]);
                        from = Integer.parseInt(parameterReplicasSplit[1]);
                        if(ack < 1 || ack > from) // validate ack and from values.
                            sendHttpRespond(httpExchange, 400, "Invalid ack value.");
                    }
                }
                else if(parameterReplicas.split("=")[0].equals("local")) { // check local
                    local = Boolean.parseBoolean(parameterReplicas.split("=")[1]);
                }
            }

            // validate id parameter.
            String[] parameterIDSplit = parameterID.split("=");
            if(parameterIDSplit.length == 1 || !parameterIDSplit[0].equals("id")) {
                sendHttpRespond(httpExchange, 400, "No ID specified.");
                return;
            }

            String requestID = parameterIDSplit[1]; // get ID for the request
            if(requestMethod.equals("GET")) {
                if(local) {
                    localGET(httpExchange, requestID);
                }
                else {
                    int foundNum = 0;
                    int deletedNum = 0;
                    int notFoundNum = 0;
                    int node = hash(requestID);
                    int currentAck = 0;
                    byte[] response = null;
                    GETresponse curResponse;
                    for (int i = 0; i < from; i++) {
                        if(node == thisNodeNumInTopology) {
                            if(deleted.contains(requestID)) {
                                // response code 405 is used to indicate deleted entity in inter-cluster requests.
                                deletedNum++;
                                currentAck++;
                            }
                            else {
                                try {
                                    byte[] dataGET = serverDao.getData(requestID);
                                    if (response == null)
                                        response = dataGET;
                                    foundNum++;
                                    currentAck++;
                                } catch (NoSuchElementException e) {
                                    notFoundNum++;
                                    currentAck++;
                                }
                            }
                        }
                        else {
                            curResponse = sendHttpRequestGet(node, requestID);
                            if (response == null)
                                response = curResponse.data;
                            if (curResponse.code == 404) {
                                notFoundNum++;
                                currentAck++;
                            } else if (curResponse.code == 200) {
                                foundNum++;
                                currentAck++;
                            } else if (curResponse.code == 405) {
                                deletedNum++;
                                currentAck++;
                            }
                        }
                        node = (node + 1) % clusterTopology.size();
                    }
                    if (currentAck >= ack) {
                        if ((notFoundNum >= ack)) // if all ack nodes responded with 404 than the entity was not found on the cluster.
                            sendHttpRespond(httpExchange, 404, "Not found");
                        else if (deletedNum > (from - ack)) // if the entity was deleted even on one node than we responde that it was deleted on the cluster.
                            sendHttpRespond(httpExchange, 404, "Deleted");
                        else if ((foundNum + notFoundNum) >= ack) // if we found the entity that we respond with its value.
                            sendHttpRespond(httpExchange, 200, response);

                    } else
                        sendHttpRespond(httpExchange, 504, "Not enough replicas");
                }
            }
            else if(requestMethod.equals("PUT")) {
                if(local) {
                    localPUT(httpExchange, requestID);
                }
                else {
                    int dataSize = Integer.valueOf(httpExchange.getRequestHeaders().getFirst("Content-Length"));
                    byte[] dataPUT = new byte[dataSize];
                    int read = httpExchange.getRequestBody().read(dataPUT);
                    if (read != dataPUT.length && read != -1) {
                        throw new IOException("Can't read the file!");
                    }
                    int node = hash(requestID);
                    int currentAck = 0;
                    for(int i = 0; i<from; i++) {
                        if(node == thisNodeNumInTopology) {
                            serverDao.upsertData(requestID, dataPUT);
                            deleted.remove(requestID); // the entity was upsert and we need to remove its id from the set of deleted.
                            currentAck++;
                        }
                        else if(sendHttpRequestPut(node, requestID, dataPUT) == 201)
                            currentAck++;
                        node = (node + 1) % clusterTopology.size();
                    }
                    if(currentAck >= ack)
                        sendHttpRespond(httpExchange, 201, "Success");
                    else
                        sendHttpRespond(httpExchange, 504, "Not enough replicas");
                }
            }
            else if(requestMethod.equals("DELETE")){
                if(local) {
                    localDELETE(httpExchange, requestID);
                }
                else {
                    int node = hash(requestID);
                    int currentAck = 0;
                    for(int i = 0; i<from; i++) {
                        if(node == thisNodeNumInTopology) {
                            serverDao.deleteData(requestID);
                            deleted.add(requestID); // add id to the set of deleted entities.
                            currentAck++;
                        }
                        else if(sendHttpRequestDelete(node, requestID) == 202)
                            currentAck++;
                        node = (node + 1) % clusterTopology.size();
                    }
                    if(currentAck >= ack)
                        sendHttpRespond(httpExchange, 202, "Success");
                    else
                        sendHttpRespond(httpExchange, 504, "Not enough replicas");
                }
            }
        });

        // status request handler.
        server.createContext("/v0/status", httpExchange -> {
            sendHttpRespond(httpExchange, 200, "ServerEnlighten1 running");
        });
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(1);
    }

    private void sendHttpRespond(HttpExchange httpExchange, int code, String respond) throws IOException {
        httpExchange.sendResponseHeaders(code, respond.getBytes().length);
        httpExchange.getResponseBody().write(respond.getBytes());
        httpExchange.getResponseBody().close();
    }

    private void sendHttpRespond(HttpExchange httpExchange, int code, byte[] data) throws IOException {
        httpExchange.sendResponseHeaders(code, data.length);
        httpExchange.getResponseBody().write(data);
        httpExchange.getResponseBody().close();
    }

    // sends GET request to a node with given id and returns the response code.
    private GETresponse sendHttpRequestGet(int node, String id) {
        HttpURLConnection connection = null;
        try {
            int i = 0;
            String URLString = getNodeAddress(node);
            URLString += "/v0/entity?id="+id+"&local=true";
            URL url = new URL(URLString);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setRequestProperty("charset", "utf-8");
            connection.setDoOutput( true );
            connection.setUseCaches( false );
            connection.setConnectTimeout(timeout);
            connection.connect();
            int resCode = connection.getResponseCode();
            ByteArrayOutputStream buffer = null;
            if (connection.getResponseCode() == 200) {
                InputStream is = connection.getInputStream();
                buffer = new ByteArrayOutputStream();
                int nRead;
                byte[] data = new byte[2048];
                while ((nRead = is.read(data, 0, data.length)) != -1) {
                    buffer.write(data, 0, nRead);
                }
                buffer.flush();
            }
            if(buffer == null)
                return new GETresponse(resCode, null);
            return new GETresponse(resCode, buffer.toByteArray());
        }
        catch (Exception e) {
            return new GETresponse(-1, null);
        }
        finally {
            if(connection != null)
                connection.disconnect();
        }
    }

    // sends PUT request to a node with given id and returns the response code.
    private int sendHttpRequestPut(int node, String id, byte[] value) {
        HttpURLConnection connection = null;
        try {
            int i = 0;
            String URLString = getNodeAddress(node);
            URLString += "/v0/entity?id="+id+"&local=true";
            URL url = new URL(URLString);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("PUT");
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setRequestProperty("Content-Length", Integer.toString(value.length));
            connection.setRequestProperty("charset", "utf-8");
            connection.setDoOutput( true );
            connection.setUseCaches( false );
            connection.setConnectTimeout(timeout);
            connection.getOutputStream().write(value);
            connection.getOutputStream().flush();
            connection.connect();
            return connection.getResponseCode();
        }
        catch (Exception e) {
            return -1;
        }
        finally {
            if(connection != null)
                connection.disconnect();
        }
    }

    // sends DELETE request to a node with given id and returns the response code.
    private int sendHttpRequestDelete(int node, String id) {
        HttpURLConnection connection = null;
        try {
            int i = 0;
            String URLString = getNodeAddress(node);
            URLString += "/v0/entity?id="+id+"&local=true";
            URL url = new URL(URLString);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("DELETE");
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setRequestProperty("charset", "utf-8");
            connection.setDoOutput( true );
            connection.setUseCaches( false );
            connection.setConnectTimeout(timeout);
            connection.connect();
            return connection.getResponseCode();
        }
        catch (Exception e) {
            return -1;
        }
        finally {
            if(connection != null)
                connection.disconnect();
        }
    }

    // hash function is used to determine which node will process which IDs.
    private int hash(String id) {
        int temp = id.hashCode();
        if(temp < 0)
            temp *= -1;
        return temp % clusterTopology.size();
    }

    // returns address of the given node from the cluster topology.
    private String getNodeAddress(int nodeNum) {
        String address = null;
        int i = 0;
        for(Iterator<String> it = clusterTopology.iterator(); it.hasNext();) {
            address = it.next();
            if(i == nodeNum)
                break;
            i++;
        }
        return address;
    }

    private void localGET(HttpExchange httpExchange, String requestID) throws IOException {
        if(deleted.contains(requestID))
            // response code 405 is used to indicate deleted entity in inter-cluster requests.
            sendHttpRespond(httpExchange, 405, "Value with key " + requestID + " was deleted!");
        else {
            try {
                byte[] dataGET = serverDao.getData(requestID);
                sendHttpRespond(httpExchange, 200, dataGET);
            } catch (NoSuchElementException e) {
                sendHttpRespond(httpExchange, 404, "Value with key " + requestID + " was not found!");
            }
        }
    }

    private void localPUT(HttpExchange httpExchange, String requestID) throws IOException {
        int dataSize = Integer.valueOf(httpExchange.getRequestHeaders().getFirst("Content-Length"));
        byte[] dataPUT = new byte[dataSize];
        int read = httpExchange.getRequestBody().read(dataPUT);
        if (read != dataPUT.length && read != -1) {
            throw new IOException("Can't read the file!");
        }
        serverDao.upsertData(requestID, dataPUT);
        deleted.remove(requestID); // the entity was upsert and we need to remove its id from the set of deleted.
        sendHttpRespond(httpExchange, 201, "Value was written with the key " + requestID + ".");
    }

    private void localDELETE(HttpExchange httpExchange, String requestID) throws IOException {
        serverDao.deleteData(requestID);
        deleted.add(requestID); // add id to the set of deleted entities.
        sendHttpRespond(httpExchange, 202, "Value with the key " + requestID + " was deleted.");
    }
}
