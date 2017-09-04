/**
 * Created by da on 9/3/17.
 */
public class Driver {
    public static void main(String[] args) throws Exception {

        DataPreprocessor dataPreprocessor= new DataPreprocessor();
        CoOccurrenceMatrix coOccurrenceMatrix = new CoOccurrenceMatrix();
        NormalizeMatrix normalizeMatrix = new NormalizeMatrix();
        MatrixMultiplier matrixMultiplier = new MatrixMultiplier();
        FinalSumUp finalSumUp = new FinalSumUp();

        String rawDataDir = args[0];
        String preprocessorOutputDir = args[1];
        String coOccurrenceMatrixOutputDir = args[2];
        String normalizeMatrixOutputDir = args[3];
        String multiplierOutputDir = args[4];
        String finalOutputDir = args[5];

        String[] path1 = {rawDataDir,preprocessorOutputDir};
        String[] path2 = {preprocessorOutputDir,coOccurrenceMatrixOutputDir};
        String[] path3 = {coOccurrenceMatrixOutputDir,normalizeMatrixOutputDir};
        String[] path4 = {normalizeMatrixOutputDir,rawDataDir,multiplierOutputDir};
        String[] path5 = {multiplierOutputDir,finalOutputDir};

        dataPreprocessor.main(path1);
        coOccurrenceMatrix.main(path2);
        normalizeMatrix.main(path3);
        matrixMultiplier.main(path4);
        finalSumUp.main(path5);

    }
}
