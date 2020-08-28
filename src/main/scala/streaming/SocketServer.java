package streaming;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class SocketServer {

	public static void main(String[] args) {

		ServerSocket socServer = null;
		String line;
		DataInputStream is;
		PrintStream os;
		Socket clientSocket = null;

		Path path = FileSystems.getDefault().getPath("data-files\\", "streamingtweets.txt");
		List<String> lines = null;
		try {
			// Read all the line from the file.
			lines = Files.readAllLines(path, StandardCharsets.UTF_8);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			// Server socket, bound to the specified port.
			socServer = new ServerSocket(9999);
			System.out.println("Socket opened");

			System.out.println("Total records read :" + lines.size());
		} catch (IOException e) {
			System.out.println(e);
		}

		try {
			// Listens for a connection to be made to this socket and accepts it.
			// The method blocks until a connection is made.
			clientSocket = socServer.accept();

			System.out.println("Accepted client request from : " + clientSocket.getInetAddress());
			is = new DataInputStream(clientSocket.getInputStream());
			os = new PrintStream(clientSocket.getOutputStream());

			while (true) {

				// Pick a random line
				int randomline = (int) (Math.random() * lines.size());

				System.out.println("Publishing " + lines.get(randomline));
				os.println(lines.get(randomline));
				os.flush();
				// Randomly sleep 1 - 3 seconds
				Thread.sleep((long) (Math.random() * 3000));
			}
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}