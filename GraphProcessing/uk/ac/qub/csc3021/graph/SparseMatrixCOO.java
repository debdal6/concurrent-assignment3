package uk.ac.qub.csc3021.graph;

import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;

// This class represents the adjacency matrix of a graph as a sparse matrix
// in coordinate format (COO)
public class SparseMatrixCOO extends SparseMatrix {
	// variable declarations
	int[] source;
	int[] destination;
	int num_vertices; // Number of vertices in the graph
	int num_edges; // Number of edges in the graph

	public SparseMatrixCOO(String file) {
		try {
			InputStreamReader is = new InputStreamReader(new FileInputStream(file), "UTF-8");
			BufferedReader rd = new BufferedReader(is);
			readFile(rd);
		} catch (FileNotFoundException e) {
			System.err.println("File not found: " + e);
			return;
		} catch (UnsupportedEncodingException e) {
			System.err.println("Unsupported encoding exception: " + e);
			return;
		} catch (Exception e) {
			System.err.println("Exception: " + e);
			return;
		}
	}

	int getNext(BufferedReader rd) throws Exception {
		String line = rd.readLine();
		if (line == null)
			throw new Exception("premature end of file");
		return Integer.parseInt(line);
	}

	void getNextPair(BufferedReader rd, int pair[]) throws Exception {
		String line = rd.readLine();
		if (line == null)
			throw new Exception("premature end of file");
		StringTokenizer st = new StringTokenizer(line);
		pair[0] = Integer.parseInt(st.nextToken());
		pair[1] = Integer.parseInt(st.nextToken());
	}

	void readFile(BufferedReader rd) throws Exception {
		String line = rd.readLine();
		if (line == null)
			throw new Exception("premature end of file");
		if (!line.equalsIgnoreCase("COO"))
			throw new Exception("file format error -- header");

		num_vertices = getNext(rd);
		num_edges = getNext(rd);

		// Allocate memory for the COO representation
		source = new int[num_edges];
		destination = new int[num_edges];

		int edge[] = new int[2];
		for (int i = 0; i < num_edges; ++i) {
			getNextPair(rd, edge);
			// Insert edge with source edge[0] and destination edge[1]
			source[i] = edge[0];
			destination[i] = edge[1];
		}
	}

	// Return number of vertices in the graph
	public int getNumVertices() {
		return num_vertices;
	}

	// Return number of edges in the graph
	public int getNumEdges() {
		return num_edges;
	}

	// Auxiliary function for PageRank calculation
	public void calculateOutDegree(int outdeg[]) {
		// Calculate the out-degree for every vertex, i.e., the
		// number of edges where a vertex appears as a source vertex.
		for (int i = 0; i < num_edges; i++) {
			outdeg[source[i]] += 1;
		}
	}

	public void edgemap(Relax relax) {
		// Iterate over all edges in the sparse matrix and calculate
		// the contribution to the new PageRank value of a destination
		// vertex made by the corresponding source vertex
		for (int i = 0; i < num_edges; i++) {
			int src = source[i];
			int dest = destination[i];

			relax.relax(src, dest);
		}
	}

	public void ranged_edgemap(Relax relax, int from, int to) {
		// Only implement for parallel/concurrent processing
		// if you find it useful. Not relevant for the first assignment.
	}
}
