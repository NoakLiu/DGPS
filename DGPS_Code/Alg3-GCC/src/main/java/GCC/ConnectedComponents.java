
package GCC;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import GCC.InitializationDriver.InputType;
import GCC.StarDriver.StarDriverType;

public class ConnectedComponents {
    private static final int MAX_ITERATIONS = 30;
    private final Path input, output;
    private final FileSystem fs;
    private InputType type;
    private long numCliques, numInitialNodes, numNodes, numClusters;
    private boolean testOk;

    public ConnectedComponents(Path input, Path output) throws IOException {
        this.input = input;
        this.output = output;
        this.fs = FileSystem.get(new Configuration());
    }

    public boolean run() throws Exception {
        InitializationDriver init = new InitializationDriver(this.input, this.input.suffix("_0"), false);
        if (init.run(null) != 0) {
            this.fs.delete(this.input.suffix("_0"), true);
            return false;
        }

        StarDriver largeStar, smallStar;
        int i = 0;
        do {
            largeStar = new StarDriver(StarDriverType.LARGE, this.input.suffix("_" + i), this.input.suffix("_" + (i + 1)), i, false);
            if (largeStar.run(null) != 0) {
                this.fs.delete(this.input.suffix("_" + i), true);
                this.fs.delete(this.input.suffix("_" + (i + 1)), true);
                return false;
            }

            this.fs.delete(this.input.suffix("_" + i), true);
            i++;

            smallStar = new StarDriver(StarDriverType.SMALL, this.input.suffix("_" + i), this.input.suffix("_" + (i + 1)), i, false);
            if (smallStar.run(null) != 0) {
                this.fs.delete(this.input.suffix("_" + i), true);
                this.fs.delete(this.input.suffix("_" + (i + 1)), true);
                return false;
            }

            this.fs.delete(this.input.suffix("_" + i), true);
            i++;
        }
        while ((largeStar.getNumChanges() + smallStar.getNumChanges() != 0) && (i < 2 * MAX_ITERATIONS));

        TerminationDriver term = new TerminationDriver(this.input.suffix("_" + i), this.output, false);
        if (term.run(null) != 0) {
            this.fs.delete(this.input.suffix("_" + i), true);
            this.fs.delete(this.output, true);
            return false;
        }

        this.fs.delete(this.input.suffix("_" + i), true);

        CheckDriver check = new CheckDriver(this.output, false);
        if (check.run(null) != 0)
            return false;

        this.type = init.getInputType();
        this.numCliques = init.getNumCliques();
        this.numInitialNodes = init.getNumInitialNodes();
        this.numClusters = term.getNumClusters();
        this.numNodes = term.getNumNodes();
        this.testOk = check.isTestOk();

        return true;
    }

    public InputType getInputType() {
        return this.type;
    }

    public long getNumCliques() {
        return this.numCliques;
    }

    public long getNumInitialNodes() {
        return this.numInitialNodes;
    }

    public long getNumNodes() {
        return this.numNodes;
    }

    public long getNumClusters() {
        return this.numClusters;
    }

    public boolean isTestOk() {
        return this.testOk;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage: ConnectedComponents <input> <output>");
            System.exit(1);
        }

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        System.out.println("Start ConnectedComponents.");
        ConnectedComponents cc = new ConnectedComponents(input, output);
        if (!cc.run())
            System.exit(1);
        System.out.println("End ConnectedComponents.");

        System.out.println("Input file format: \033[1;94m" + cc.getInputType().toString() + "\033[0m.");
        System.out.println("Number of initial nodes: \033[1;94m" + cc.getNumInitialNodes() + "\033[0m.");
        System.out.println("Number of Cliques: \033[1;94m" + cc.getNumCliques() + "\033[0m.");
        System.out.println("Number of final nodes: \033[1;94m" + cc.getNumNodes() + "\033[0m.");
        System.out.println("Number of Clusters: \033[1;94m" + cc.getNumClusters() + "\033[0m.");
        System.out.println("TestOK: \033[1;94m" + String.valueOf(cc.isTestOk()) + "\033[0m.");

        System.exit(0);
    }
}
