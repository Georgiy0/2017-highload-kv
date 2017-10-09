package ru.mail.polis.Kubrin;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVService;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.NoSuchElementException;

/**
 * Created by Egor on 09.10.2017.
 */
public class KeyValueStorage implements KVService {
    private HttpServer server;
    private Dao serverDao;
    private File valuesDirectory;

    public KeyValueStorage(final int port,
                           @NotNull final File data) throws IOException {
        this.valuesDirectory = data;
        serverDao = new DaoFiles(data);
        server = HttpServer.create();
        server.bind(new InetSocketAddress(port), 0);
        server.createContext("/v0/entity", httpExchange -> {
            String requestMethod = httpExchange.getRequestMethod();
            String requestParameter = httpExchange.getRequestURI().getQuery();
            if(requestParameter.split("=").length == 1 || !requestParameter.split("=")[0].equals("id")) {
                sendHttpRespond(httpExchange, 400, "No ID specified.");
                return;
            }
            requestParameter = requestParameter.split("=")[1];
            if(requestMethod.equals("GET")) {
                try {
                    byte[] dataGET = serverDao.getData(requestParameter);
                    sendHttpRespond(httpExchange, 200, dataGET);
                } catch (NoSuchElementException e) {
                    sendHttpRespond(httpExchange, 404, "Value with key "+requestParameter+" was not found!");
                }
            }
            else if(requestMethod.equals("PUT")) {
                int dataSize = Integer.valueOf(httpExchange.getRequestHeaders().getFirst("Content-Length"));
                byte[] dataPUT = new byte[dataSize];
                int read = httpExchange.getRequestBody().read(dataPUT);
                if (read != dataPUT.length && read != -1) {
                    throw new IOException("Can't read the file!");
                }
                serverDao.upsertData(requestParameter, dataPUT);
                sendHttpRespond(httpExchange, 201, "Value was written with the key " + requestParameter + ".");
            }
            else if(requestMethod.equals("DELETE")){
                serverDao.deleteData(requestParameter);
                sendHttpRespond(httpExchange, 202, "Value with the key " + requestParameter + " was deleted.");
            }
        });
        server.createContext("/v0/status", httpExchange -> {
            sendHttpRespond(httpExchange, 200, "Server running");
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
}


