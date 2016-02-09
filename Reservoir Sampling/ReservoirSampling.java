package rs.pkg1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;

public class ReservoirSampling {

	public static void main(String[] args) throws IOException {

		try {

			String fileName = createFile(args[0]);

			int k = Integer.parseInt(args[1]);

			int[] reservoir = new int[k];
			int[] innitialRes = new int[k];
			Random rand = new Random();

			try (BufferedReader br = new BufferedReader(
					new FileReader(fileName))) {
				String line;
				int popI = 0;
				try {
					while ((line = br.readLine()) != null) {
						if (popI < k) {
							innitialRes[popI] = Integer.parseInt(line);
							reservoir[popI] = Integer.parseInt(line);
						} else if (popI >= k) {

							int j = rand.nextInt(popI - 0 + 2) + 1;
							if (j >= 0 && j < k) {
								reservoir[j] = Integer.parseInt(line);
							}
						}

						popI++;
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			for (int a = 0; a < reservoir.length; a++) {
				System.out.println("old:" + innitialRes[a] + " new:"
						+ reservoir[a]);
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static String createFile(String limitStr)
			throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter("data.txt", "UTF-8");

		int i = 0;
		int limit = Integer.parseInt(limitStr);
		while (i < limit) {
			Random rand = new Random();
			int m = rand.nextInt(limit - 1 + 2) + 1;
			if (i < limit - 1)
				writer.println(m);
			else {
				writer.print(m);
			}
			i++;
		}
		writer.close();

		return "data.txt";

	}

}
