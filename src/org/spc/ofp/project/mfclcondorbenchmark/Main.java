/***********************************************************************
 *  Copyright - Secretariat of the Pacific Community                   *
 *  Droit de copie - Secrétariat Général de la Communauté du Pacifique *
 *  http://www.spc.int/                                                *
 ***********************************************************************/
package org.spc.ofp.project.mfclcondorbenchmark;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

/**
 *
 * @author Fabrice Bouyé (fabriceb@spc.int)
 */
public final class Main {

    private final List<Exception> raisedExceptions = new ArrayList<>();
    private final Properties settings = new Properties();
    private final String wordDir;
    private final Path workPath;
    private final String templateSource;
    private final Path templateSourcePath;
    private final List<Path> workDirPathList = new ArrayList<>();

    /**
     * Creates a new instance.
     * @throws IOException In case of IO error.
     */
    public Main() throws IOException {
        ////////////////////////////////////////////////////////////////////////
        // Read settings.
        readSettings();
        ////////////////////////////////////////////////////////////////////////
        // Check if template dir exists.
        templateSource = settings.getProperty("template.source"); // NOI18N.
        templateSourcePath = new File(templateSource).toPath();
        if (!Files.exists(templateSourcePath)) {
            throw new IOException();
        }
        ////////////////////////////////////////////////////////////////////////
        // Initialize work space.
        wordDir = settings.getProperty("work.dir"); // NOI18N.
        workPath = new File(wordDir).toPath();
        initializeWorkspace();
        ////////////////////////////////////////////////////////////////////////
        // Prepare host work spaces in parallel.
        final int testNumber = Integer.parseInt(settings.getProperty("test.number")); // NOI18N.
        final String hostsValue = settings.getProperty("hosts"); // NOI18N.
        final String[] hosts = hostsValue.split(",\\s*"); // NOI18N.
        Arrays.stream(hosts)
                .parallel()
                .forEach(host -> {
                    try {
                        message(String.format("Setting up workspace for host \"%s\".", host)); // NOI18N.
                        setupWorkDirsForHost(host, testNumber);
                    } catch (Exception ex) {
                        raisedExceptions.add(ex);
                    }
                });
        ////////////////////////////////////////////////////////////////////////
        // Launch in parallel.
        if (workDirPathList.isEmpty()) {
            final IOException ex = new IOException();
            raisedExceptions.stream()
                    .forEach(suppressed -> ex.addSuppressed(suppressed));
            throw ex;
        }
        workDirPathList.stream()
                .parallel()
                .forEach(workDirPath -> {
                    try {
                        message(String.format("Launching job \"%s\".", workDirPath.toString())); // NOI18N.
                        launchJob(workDirPath);
                        message(String.format("Job \"%s\" finished.", workDirPath.toString())); // NOI18N.
                    } catch (Exception ex) {
                        raisedExceptions.add(ex);
                    }
                });
        ////////////////////////////////////////////////////////////////////////
        // Extract results sequentially.
        message("Getting results."); // NOI18N.
        workDirPathList.stream()
                .forEach(workDirPath -> {
                    try {
                        processResults(workDirPath);
                    } catch (Exception ex) {
                        raisedExceptions.add(ex);
                    }
                });
        ////////////////////////////////////////////////////////////////////////
        if (!raisedExceptions.isEmpty()) {
            final IOException ex = new IOException();
            raisedExceptions.stream()
                    .forEach(suppressed -> ex.addSuppressed(suppressed));
            throw ex;
        }
    }

    /**
     * Read the settings file.
     * @throws IOException In case of IO error.
     */
    private void readSettings() throws IOException {
        message("Reading settings."); // NOI18N.
        final Path settingsPath = new File("settings.properties").toPath(); // NOI18N.
        if (!Files.exists(settingsPath)) {
            throw new IOException();
        }
        try (final InputStream input = Files.newInputStream(settingsPath)) {
            settings.load(input);
        }
    }

    /**
     * Initialize workspace.
     * <br/>Previous workspace will be cleared if it exists.
     * @throws IOException In case of IO error.
     */
    private void initializeWorkspace() throws IOException {
        // Delete workdir if exists.
        if (Files.exists(workPath)) {
            message("Cleaning previous workspace."); // NOI18N.
            clearDirectory(workPath);
        }
        // Create workdir.
        message("Initializing workspace.");
        Files.createDirectory(workPath);
    }

    /**
     * Setup the work directories for given host.
     * @param host The host.
     * @param testNumber The number of tests.
     * @throws IOException In case of IO error.
     */
    private void setupWorkDirsForHost(final String host, final int testNumber) throws IOException {
        if (!Files.exists(templateSourcePath)) {
            throw new IOException();
        }
        // Create host dir.
        final Path hostWorkPath = new File(workPath.toFile(), host).toPath();
        Files.createDirectory(hostWorkPath);
        final List<Exception> hostSetupException = new ArrayList<>();
        IntStream.range(0, testNumber)
                .parallel()
                .forEach(runIndex -> {
                    try {
                        setupWorkDirForHost(host, runIndex, hostWorkPath);
                    } catch (IOException ex) {
                        hostSetupException.add(ex);
                    }
                });
        if (!hostSetupException.isEmpty()) {
            final IOException ex = new IOException();
            hostSetupException.stream()
                    .forEach(suppressed -> ex.addSuppressed(ex));
            throw ex;
        }
    }

    /**
     * Setup a work directories for given host.
     * @param host The host.
     * @param runIndex The test number.
     * @param hostWorkPath The host work dir.
     * @throws IOException In case of IO error.
     */
    private void setupWorkDirForHost(final String host, final int runIndex, final Path hostWorkPath) throws IOException {
        // Create run dir.
        final Path runWorkPath = new File(hostWorkPath.toFile(), String.valueOf(runIndex)).toPath();
        Files.createDirectory(runWorkPath);
        final String subFile = settings.getProperty("sub.file"); // NOI18N.
        // Create sub file.
        final Path subFilePath = new File(runWorkPath.toFile(), subFile).toPath();
        Files.createFile(subFilePath);
        try (final PrintWriter writer = new PrintWriter(Files.newOutputStream(subFilePath))) {
            final String universe = settings.getProperty("universe"); // NOI18N.
            writer.printf("universe = %s", universe).println(); // NOI18N.
            final String execFile = settings.getProperty("exec.file"); // NOI18N.
            writer.printf("executable = %s", execFile).println(); // NOI18N.
            final String getEnv = settings.getProperty("get.env"); // NOI18N.
            writer.printf("getenv = %s", getEnv).println(); // NOI18N.
            final String errFile = settings.getProperty("err.file"); // NOI18N.
            writer.printf("error = %s", errFile).println(); // NOI18N.
            final String logFile = settings.getProperty("log.file"); // NOI18N.
            writer.printf("log = %s", logFile).println(); // NOI18N.
            final String outFile = settings.getProperty("out.file"); // NOI18N.
            writer.printf("output = %s", outFile).println(); // NOI18N.
            final String shouldTransferFiles = settings.getProperty("should.transfer.files"); // NOI18N.
            writer.printf("should_transfer_files = %s", shouldTransferFiles).println(); // NOI18N.
            final String requirements = buildRequirements(host);
            writer.printf("Requirements = %s", requirements).println(); // NOI18N.
            final String whenToTransferOutput = settings.getProperty("when.to.transfer.output"); // NOI18N.
            writer.printf("when_to_transfer_output = %s", whenToTransferOutput).println(); // NOI18N.
            final String priority = settings.getProperty("priority"); // NOI18N.
            writer.printf("priority = %s", priority).println(); // NOI18N.
            final String tranferInputFiles = settings.getProperty("tranfer.input.files");
            writer.printf("TRANSFER_INPUT_FILES = %s", tranferInputFiles).println(); // NOI18N.
            writer.println("queue"); // NOI18N.
        }
        // Recopy exec file.
        final String execFile = settings.getProperty("exec.file"); // NOI18N.
        recopyFromDirectory(templateSourcePath, runWorkPath, execFile); // NOI18N.
        // Recopy input files.
        final String tranferInputFiles = settings.getProperty("tranfer.input.files"); // NOI18N.
        final String[] filesToTransfer = tranferInputFiles.split(",\\s*"); // NOI18N.
        for (final String fileToTransfer : filesToTransfer) {
            recopyFromDirectory(templateSourcePath, runWorkPath, fileToTransfer);
        }
        // Valid directories are added to the run list.
        workDirPathList.add(runWorkPath);
    }

    /**
     * Build run requirements string for given host.
     * @param host The host.
     * @return A {@code String}, never {@code null}.
     */
    private String buildRequirements(final String host) {
        final StringWriter result = new StringWriter();
        boolean isEmpty = true;
        try (final PrintWriter out = new PrintWriter(result)) {
            // Op sys.
            final String opsys = settings.getProperty("requirements.opsys"); // NOI18N.
            if (opsys != null && !opsys.trim().isEmpty()) {
                out.printf("(OpSys == \"%s\")", opsys); // NOI18N.
                isEmpty = false;
            }
            // Arch.
            final String arch = settings.getProperty("requirements.arch"); // NOI18N.
            if (arch != null && !arch.trim().isEmpty()) {
                if (!isEmpty) {
                    out.print(" && "); // NOI18N.
                }
                out.printf("(arch == \"%s\")", arch); // NOI18N.
                isEmpty = false;
            }
            // Min memory.
            final String minMemory = settings.getProperty("requirements.min.memory"); // NOI18N.
            if (minMemory != null && !minMemory.trim().isEmpty()) {
                if (!isEmpty) {
                    out.print(" && "); // NOI18N.
                }
                out.printf("(memory > %s)", minMemory); // NOI18N.
                isEmpty = false;
            }
            // Host
            {
                if (!isEmpty) {
                    out.print(" && "); // NOI18N.
                }
                out.printf("(machine == \"%s\")", host); // NOI18N.
            }
        }
        return result.toString();
    }

    /**
     * Launch job in given work dir.
     * @param hostWorkPath The work dir.
     * @throws IOException In case of IO error.
     * @throws InterruptedException If the report file monitoring was interrupted.
     */
    private void launchJob(final Path hostWorkPath) throws IOException, InterruptedException {
        final String condorBin = settings.getProperty("condor.bin"); // NOI18N.
        final String condorSubmit = settings.getProperty("condor.submit"); // NOI18N.
        final Path condorBinPath = new File(condorBin).toPath();
        if (!Files.exists(condorBinPath)) {
            throw new IOException();
        }
        final String execExtension = System.getProperty("os.name").toLowerCase().contains("windows") ? ".exe" : ""; // NOI18N.
        final String command = String.format("%s/%s", condorBin, condorSubmit, execExtension); // NOI18N.
        final String subFile = settings.getProperty("sub.file"); // NOI18N.
        // Submit job.
        final ProcessBuilder processBuilder = new ProcessBuilder(command, subFile);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        processBuilder.directory(hostWorkPath.toFile());
        final Process process = processBuilder.start();
        final int exitValue = process.waitFor();
        if (exitValue != 0) {
            throw new IOException();
        }
        // Monitor the log file to known when job is finished.
        final String logFile = settings.getProperty("log.file"); // NOI18N.
        final Path logFilePath = new File(hostWorkPath.toFile(), logFile).toPath();
        try (final WatchService watchService = FileSystems.getDefault().newWatchService()) {
            final WatchKey watchKey = hostWorkPath.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            boolean jobEnded = false;
            // This will loop until the job is finished or has been canceled.
            while (!jobEnded) {
                // Blocking method.
                final WatchKey wk = watchService.take();
                for (final WatchEvent<?> event : wk.pollEvents()) {
                    // We only registered "ENTRY_MODIFY" so the context is always a Path.
                    final Path changed = (Path) event.context();
                    if (changed.endsWith(logFile)) {
                        try (final LineNumberReader reader = new LineNumberReader(new FileReader(logFilePath.toFile()))) {
                            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                                if (line.contains("Job terminated.") || line.contains("Job was aborted by the user")) { // NOI18N.
                                    jobEnded = true;
                                    break;
                                }
                            }
                        }
                    }
                }
                // Reset the key
                final boolean valid = wk.reset();
                if (!valid) {
                    message(String.format("%s: key has been unregistered", hostWorkPath.toString())); // NOI18N.
                }
            }
        }
    }

    /**
     * Process result file in given directory.
     * @param hostWorkPath The working directory.
     * @throws IOException In case of IO error.
     */
    private void processResults(final Path hostWorkPath) throws IOException {
        final String reportFile = settings.getProperty("report.file"); // NOI18N.
        final Path reportFilePath = new File(hostWorkPath.toFile(), reportFile).toPath();
        // Canceled jobs do not have a report.
        if (!Files.exists(reportFilePath)) {
            message(String.format("%s: no result.", hostWorkPath.toString())); // NOI18N.
            return;
        }
        try (final LineNumberReader reader = new LineNumberReader(new FileReader(reportFilePath.toFile()))) {
            String line = reader.readLine();
            final String hostname = line;
            line = reader.readLine();
            final long initialSize = Long.parseLong(line.split("\\s+")[0]); // NOI18N.
            line = reader.readLine();
            final String modelRunTime = line.substring(line.indexOf(':') + 1, line.length()).trim(); // NOI18N.
            line = reader.readLine();
            final long finalSize = Long.parseLong(line.split("\\s+")[0]); // NOI18N.
            line = reader.readLine();
            final String execRunTime = line.substring(line.indexOf(':') + 1, line.length()).trim(); // NOI18N.
            message(String.format("%s\t%s\t%s\t%s\t%s", hostname, initialSize, finalSize, modelRunTime, execRunTime)); // NOI18N.
        } catch (IndexOutOfBoundsException | NumberFormatException ex) {
            final String message = String.format("Error while parsing \"%s\".", reportFilePath.toString()); // NOI18N.
            final IOException ioex = new IOException(message);
            ioex.addSuppressed(ex);
            throw ioex;
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    /**
     * Program entry point.
     * @param args The command line arguments
     */
    public static void main(String[] args) {
        try {
            final Main main = new Main();
            System.exit(0);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    /**
     * Clear given directory structure.
     * @param directory The directory to clear.
     * @throws IOException In case of IO error.
     */
    private static void clearDirectory(final Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            
            
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Recopy a file from a source directory to a destination directory.
     * @param sourcePath Path to the source directory.
     * @param destinationPath Path to the target directory.
     * @param file The name of the file.
     * @throws IOException In case of IO error.
     */
    private static void recopyFromDirectory(final Path sourcePath, final Path destinationPath, final String file) throws IOException {
        if (!Files.exists(sourcePath) || !Files.isReadable(sourcePath)) {
            throw new IOException();
        }
        if (Files.exists(destinationPath) && !Files.isWritable(destinationPath)) {
            throw new IOException();
        }
        final Path sourceFilePath = new File(sourcePath.toFile(), file).toPath();
        final Path destinationFilePath = new File(destinationPath.toFile(), file).toPath();
        Files.copy(sourceFilePath, destinationFilePath);
    }

    /**
     * Synchronized output method.
     * <br/>As job processing is done in parallel, we need to synchronize console output.
     * @param message The message to print to the output.
     */
    private static synchronized void message(final String message) {
        System.out.println(message);
    }
}
