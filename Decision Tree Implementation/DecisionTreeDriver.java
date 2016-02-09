package dtree.pkg1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DecisionTreeDriver {

	public static Map<String, String[]> attrValueMap;
	public static String[] attributeArray;
	public static ArrayList<String> oldBestList = new ArrayList<String>();

	private static String findBestSplit(double rootInfoT,
			ArrayList<String> strResultArray) {
		String bestSplit = null;
		double gainFinal = 0.0;

		for (int i = 0; i < attributeArray.length; i++) {
			double infoOfAttr = 0.0;
			String attr = attributeArray[i];
			String[] values = attrValueMap.get(attr);
			int countRecords = 0;
			for (int j = 0; j < values.length; j++) {
				ArrayList<String> strValuesSubArray = new ArrayList<String>();
				int countValues = 0;
				countRecords = 0;
				for (int s = 0; s < strResultArray.size(); s++) {
					String line = strResultArray.get(s);
					if (line.contains(values[j])) {
						strValuesSubArray.add(line);
						countValues++;
					}
					countRecords++;
				}
				double infoV = calculateInfoT(strValuesSubArray);
				infoOfAttr += ((double) countValues / countRecords) * infoV;
			}
			double gainlocal = 0.0;
			gainlocal = rootInfoT - infoOfAttr;
			if (gainlocal > gainFinal) {
				gainFinal = gainlocal;
				bestSplit = attributeArray[i];
			}
		}

		return (bestSplit);
	}

	private static void buildSubTree(ArrayList<String> strResultArray) {
		String bestSplit;
		double rootInfoT = calculateInfoT(strResultArray);
		bestSplit = findBestSplit(rootInfoT, strResultArray);
		String[] values = attrValueMap.get(bestSplit);
		int countRecords = 0;
		for (int j = 0; j < values.length; j++) {
			ArrayList<String> strValuesSubArray = new ArrayList<String>();
			int yesCount = 0;
			countRecords = 0;
			String predict = "no";
			for (int s = 0; s < strResultArray.size(); s++) {
				String line = strResultArray.get(s);
				if (line.contains(values[j])) {
					strValuesSubArray.add(line);
					String[] lineArr = line.split(",");
					if (lineArr[4].equalsIgnoreCase("yes")) {
						yesCount++;
						predict = "yes";
					}
					countRecords++;

				}

			}
			if (yesCount == 0 || yesCount == countRecords) {
				if (!oldBestList.contains(bestSplit)) {
					System.out.println("|\t" + bestSplit + "=" + values[j]
							+ ":" + predict);
				} else {
					System.out.println(bestSplit + "=" + values[j] + ":"
							+ predict);
				}

			} else {
				System.out.println(bestSplit + "=" + values[j]);
				oldBestList.add(bestSplit);
				buildSubTree(strValuesSubArray);
			}
		}

	}

	private static double calculateInfoT(ArrayList<String> strResultArray) {
		int totalRecords = 0, yesCount = 0, noCount;
		for (String line : strResultArray) {
			String[] lineArr = line.split(",");
			if (lineArr[4].equalsIgnoreCase("yes")) {
				yesCount++;
			}
			totalRecords++;
		}

		noCount = totalRecords - yesCount;
		double yesDiv = (double) yesCount / totalRecords;
		double noDiv = (double) noCount / totalRecords;
		double infoT = -yesDiv * (Math.log(yesDiv) / Math.log(2)) - noDiv
				* (Math.log(noDiv) / Math.log(2));
		if (yesCount == 0 || noCount == 0) {
			infoT = 0;
		}

		return infoT;
	}

	private static Map<String, String[]> populateMap(
			ArrayList<String> strResultArray) {
		Map<String, String[]> attValueMap = new HashMap<String, String[]>();

		for (int i = 0; i < attributeArray.length; i++) {
			ArrayList<String> vList = new ArrayList<String>();
			for (int j = 0; j < strResultArray.size(); j++) {
				String[] lineArr = strResultArray.get(j).split(",");
				if (j == 0) {
					vList.add(lineArr[i]);
				} else if (!vList.toString().contains(lineArr[i])) {
					vList.add(lineArr[i]);
				}
			}

			String[] vArr = new String[vList.size()];
			vArr = vList.toArray(vArr);

			attValueMap.put(attributeArray[i], vArr);
		}

		return attValueMap;
	}

	private static ArrayList<String> FileReader() throws FileNotFoundException,
			IOException {
		String fileName = "input/data.csv";

		ArrayList<String> result = new ArrayList<String>();

		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			String line;
			try {
				while ((line = br.readLine()) != null) {

					result.add(line);

				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return result;
	}

	public static void main(String[] args) {

		try {
			ArrayList<String> strResultArray = FileReader();

			attributeArray = strResultArray.get(0).split(",");

			strResultArray.remove(0);

			attrValueMap = populateMap(strResultArray);

			buildSubTree(strResultArray);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}



















/*String[] oArr = { "sunny", "overcast", "rain" };
String[] tArr = { "hot", "mild", "cool" };
String[] hArr = { "high", "normal" };
String[] wArr = { "weak", "strong" };
String[] pArr = { "yes", "no" };
String[] rArr = { "outlook", "temperature", "humidity", "wind",
		"playtennis" };

attValueMap.put("root", rArr);
attValueMap.put("outlook", oArr);
attValueMap.put("temperature", tArr);
attValueMap.put("humidity", hArr);
attValueMap.put("wind", wArr);
attValueMap.put("playtennis", pArr);*/
