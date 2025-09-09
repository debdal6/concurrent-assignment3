package uk.ac.qub.csc3021.graph;

public class ParallelContextThread extends ParallelContext{
    public ParallelContextThread(int num_threads) {
	// We only use one thread in this case
	super( 1 );
    }

    // Terminate all threads (easy if we create no threads!)
    public void terminate() { }

    // Call into the iterate method and visit all edges
    public void edgemap( SparseMatrix matrix, Relax relax ) {
	((SparseMatrixPipelined) matrix).edgemap(relax);
    }
}
