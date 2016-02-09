package hadoop.assign1;


import java.util.*;

import org.apache.hadoop.io.Text;

public class Node {


	public static enum NodeColor {
		WHITE, GRAY, BLACK
	};

	private String nodeId; 
	private int distance; 
	private List<String> adjacentNodes = new ArrayList<String>(); 
	private NodeColor nColor = NodeColor.WHITE;
	private String parentNode;

	public Node() {
		
		
		distance = Integer.MAX_VALUE;
		nColor = NodeColor.WHITE;
		parentNode = null;
	}

	
	public Node(String nodeInfo) {

		String[] inputLine = nodeInfo.split("\t"); 
		String key = "", value = ""; 

		try {
			key = inputLine[0]; 
			value = inputLine[1]; 

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);

		}

		String[] tokens = value.split("\\|"); 

		this.nodeId = key; 

		for (String s : tokens[0].split(",")) {
			if (s.length() > 0) {
				adjacentNodes.add(s);
			}
		}

		if (tokens[1].equals("Integer.MAX_VALUE")) {
			this.distance = Integer.MAX_VALUE;
		} else {
			this.distance = Integer.parseInt(tokens[1]);
		}

		this.nColor = NodeColor.valueOf(tokens[2]);

		this.parentNode = tokens[3];

	}

	public Text getNodeInfo() {
		StringBuffer s = new StringBuffer();

		try {
			for (String v : adjacentNodes) {
				s.append(v).append(",");
			}
		} catch (NullPointerException e) {
			e.printStackTrace();
			System.exit(1);
		}

		s.append("|");

		if (this.distance < Integer.MAX_VALUE) {
			s.append(this.distance).append("|");
		} else {
			s.append("Integer.MAX_VALUE").append("|");
		}

		s.append(nColor.toString()).append("|");

		s.append(getParentNode());

		return new Text(s.toString());
	}


	public String getNodeId() {
		return nodeId;
	}


	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}


	public int getDistance() {
		return distance;
	}


	public void setDistance(int distance) {
		this.distance = distance;
	}


	public List<String> getAdjacentNodes() {
		return adjacentNodes;
	}


	public void setAdjacentNodes(List<String> adjacentNodes) {
		this.adjacentNodes = adjacentNodes;
	}


	public NodeColor getnColor() {
		return nColor;
	}


	public void setnColor(NodeColor nColor) {
		this.nColor = nColor;
	}


	public String getParentNode() {
		return parentNode;
	}


	public void setParentNode(String parentNode) {
		this.parentNode = parentNode;
	}




}