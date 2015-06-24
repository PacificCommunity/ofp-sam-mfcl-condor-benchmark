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

/**
 *
 * @author Fabrice Bouyé (fabriceb@spc.int)
 */
public final class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        message("Reading settings.");
        final Path settingsPath = new File("settings.properties").toPath(); // NOI18N.
        final Properties settings = new Properties();
        try (final InputStream input = Files.newInputStream(settingsPath)) {
            settings.load(input);
        }
        // Work space.
        final String wordDir = settings.getProperty("work.dir"); // NOI18N.
        final Path workPath = new File(wordDir).toPath();
        // Delete workdir if exists.
        if (Files.exists(workPath)) {
            message("Cleaning previous workspace.");
            Files.walkFileTree(workPath, new SimpleFileVisitor<Path>() {
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
        // Create workdir.
        message("Initializint workspace.");
        Files.createDirectory(workPath);
        // Prepare host work spaces.
        final String hostsValue = settings.getProperty("hosts"); // NOI18N.
        final String[] hosts = hostsValue.split(",\\s*"); // NOI18N.
        final List<Exception> exceptions = new ArrayList<>();
        Arrays.stream(hosts)
                .parallel()
                .forEach(host -> {
                    try {
                        message(String.format("Setting up workspace for host \"%s\".", host));
                        setupWorkDirForHost(settings, host);
                    } catch (Exception ex) {
                        exceptions.add(ex);
                    }
                });
        // Launch in parallel.
        Arrays.stream(hosts)
                .parallel()
                .forEach(host -> {
                    try {
                        message(String.format("Launching job for host \"%s\".", host));
                        launchJobForHost(settings, host);
                        message(String.format("Job for host \"%s\" finished.", host));
                    } catch (Exception ex) {
                        exceptions.add(ex);
                    }
                });
        // Extract results.
        Arrays.stream(hosts)
                .forEach(host -> {
                    try {
                        message(String.format("Getting result for host \"%s\".", host));
                        processResultForHost(settings, host);
                    } catch (Exception ex) {
                        exceptions.add(ex);
                    }
                });
        ////////////////////////////////////////////////////////////////////////
        if (!exceptions.isEmpty()) {
            final IOException ex = new IOException();
            exceptions.stream()
                    .forEach(suppressed -> ex.addSuppressed(suppressed));
            throw ex;
        }
    }
    
    private static synchronized void message(final String message){
        System.out.println(message);
    }

    private static void setupWorkDirForHost(final Properties settings, final String host) throws IOException {
        final String templateSource = settings.getProperty("template.source");
        final Path templateSourcePath = new File(templateSource).toPath();
        if (!Files.exists(templateSourcePath)) {
            throw new IOException();
        }
        //
        final String wordDir = settings.getProperty("work.dir"); // NOI18N.
        final Path workPath = new File(wordDir).toPath();
        final Path hostWorkPath = new File(workPath.toFile(), host).toPath();
        Files.createDirectory(hostWorkPath);
        final String subFile = settings.getProperty("sub.file"); // NOI18N.
        final Path subFilePath = new File(hostWorkPath.toFile(), subFile).toPath();
        Files.createFile(subFilePath);
        // Sub file.
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
            final String requirements = buildRequirements(settings, host);
            writer.printf("Requirements = %s", requirements).println(); // NOI18N.
            final String whenToTransferOutput = settings.getProperty("when.to.transfer.output"); // NOI18N.
            writer.printf("when_to_transfer_output = %s", whenToTransferOutput).println(); // NOI18N.
            final String priority = settings.getProperty("priority"); // NOI18N.
            writer.printf("priority = %s", priority).println(); // NOI18N.
            final String tranferInputFiles = settings.getProperty("tranfer.input.files");
            writer.printf("TRANSFER_INPUT_FILES = %s", tranferInputFiles).println(); // NOI18N.
            writer.println("queue"); // NOI18N.
        }
        final String execFile = settings.getProperty("exec.file"); // NOI18N.
        recopyFromTemplate(templateSourcePath, hostWorkPath, execFile); // NOI18N.
        final String tranferInputFiles = settings.getProperty("tranfer.input.files"); // NOI18N.
        final String[] filesToTransfer = tranferInputFiles.split(",\\s*"); // NOI18N.
        for (final String fileToTransfer : filesToTransfer) {
            recopyFromTemplate(templateSourcePath, hostWorkPath, fileToTransfer);
        }
    }

    private static void recopyFromTemplate(final Path sourcePath, final Path destinationPath, final String file) throws IOException {
        final Path sourceFilePath = new File(sourcePath.toFile(), file).toPath();
        final Path destinationFilePath = new File(destinationPath.toFile(), file).toPath();
        Files.copy(sourceFilePath, destinationFilePath);
    }

    private static String buildRequirements(final Properties settings, final String host) {
        final StringWriter result = new StringWriter();
        boolean isEmpty = true;
        try (final PrintWriter out = new PrintWriter(result)) {
            // Op sys.
            final String opsys = settings.getProperty("requirements.opsys"); // NOI18N.
            if (opsys != null && !opsys.trim().isEmpty()) {
                out.printf("(OpSys == \"%s\")", opsys);
                isEmpty = false;
            }
            // Arch.
            final String arch = settings.getProperty("requirements.arch"); // NOI18N.
            if (arch != null && !arch.trim().isEmpty()) {
                if (!isEmpty) {
                    out.print(" && ");
                }
                out.printf("(arch == \"%s\")", arch);
                isEmpty = false;
            }
            // Min memory.
            final String minMemory = settings.getProperty("requirements.min.memory"); // NOI18N.
            if (minMemory != null && !minMemory.trim().isEmpty()) {
                if (!isEmpty) {
                    out.print(" && ");
                }
                out.printf("(memory > %s)", minMemory);
                isEmpty = false;
            }
            // Host
            {
                if (!isEmpty) {
                    out.print(" && ");
                }
                out.printf("(machine == \"%s\")", host);
            }
        }
        return result.toString();
    }

    private static void launchJobForHost(final Properties settings, final String host) throws IOException, InterruptedException {
        final String wordDir = settings.getProperty("work.dir"); // NOI18N.
        final Path workPath = new File(wordDir).toPath();
        final Path hostWorkPath = new File(workPath.toFile(), host).toPath();
        final String condorBin = settings.getProperty("condor.bin"); // NOI18N.
        final String condorSubmit = settings.getProperty("condor.submit"); // NOI18N.
        final Path condorBinPath = new File(condorBin).toPath();
        if (!Files.exists(condorBinPath)) {
            throw new IOException();
        }
        final String execExtension = System.getProperty("os.name").toLowerCase().contains("windows") ? ".exe" : ""; // NOI18N.
        final String command = String.format("%s/%s", condorBin, condorSubmit, execExtension); // NOI18N.
        final String subFile = settings.getProperty("sub.file"); // NOI18N.
        final ProcessBuilder processBuilder = new ProcessBuilder(command, subFile);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        processBuilder.directory(hostWorkPath.toFile());
        final Process process = processBuilder.start();
        final int exitValue = process.waitFor();
        if (exitValue != 0) {
            throw new IOException();
        }
        final String logFile = settings.getProperty("log.file"); // NOI18N.
        final Path logFilePath = new File(hostWorkPath.toFile(), logFile).toPath();
        try (final WatchService watchService = FileSystems.getDefault().newWatchService()) {
            final WatchKey watchKey = hostWorkPath.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            boolean jobEnded = false;
            while (!jobEnded) {
                final WatchKey wk = watchService.take();
                for (final WatchEvent<?> event : wk.pollEvents()) {
                    // We only registered "ENTRY_MODIFY" so the context is always a Path.
                    final Path changed = (Path) event.context();
                    if (changed.endsWith(logFile)) {
                        try (final LineNumberReader reader = new LineNumberReader(new FileReader(logFilePath.toFile()))) {
                            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                                if (line.contains("Job terminated.") || line.contains("Job was aborted by the user")) {
                                    jobEnded = true;
                                    break;
                                }
                            }
                        }
                    }
                }
                // reset the key
                boolean valid = wk.reset();
                if (!valid) {
                    message(String.format("%s: key has been unregisterede", host));
                }
            }
        }
    }

    private static void processResultForHost(final Properties settings, final String host) throws IOException {
        final String wordDir = settings.getProperty("work.dir"); // NOI18N.
        final Path workPath = new File(wordDir).toPath();
        final Path hostWorkPath = new File(workPath.toFile(), host).toPath();
        final String reportFile = settings.getProperty("report.file"); // NOI18N.
        final Path reportFilePath = new File(hostWorkPath.toFile(), reportFile).toPath();
        // Canceled jobs do not have a report.
        if (!Files.exists(reportFilePath)) {
            message(String.format("%s: no result.", host));
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
            message(String.format("%s: %s -> %s, %s, %s", hostname, initialSize, finalSize, modelRunTime, execRunTime));
        }
    }
}
