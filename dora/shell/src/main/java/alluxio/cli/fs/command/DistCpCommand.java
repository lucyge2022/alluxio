/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.CopyJobPOptions;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.job.CopyJobRequest;
import alluxio.job.JobDescription;
import alluxio.resource.CloseableResource;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Copies a file or a directory in the Alluxio filesystem using job service.
 */
@ThreadSafe
@PublicApi
public final class DistCpCommand extends AbstractFileSystemCommand {
  private static final JobProgressReportFormat DEFAULT_FORMAT = JobProgressReportFormat.TEXT;
  private static final String JOB_TYPE = "copy";

  private static final Option SUBMIT_OPTION =
      Option.builder()
          .longOpt("submit")
          .required(false)
          .hasArg(false)
          .desc("Submit copy job to Alluxio master, update job options if already exists.")
          .build();

  private static final Option STOP_OPTION =
      Option.builder()
          .longOpt("stop")
          .required(false)
          .hasArg(false)
          .desc("Stop a copy job if it's still running.")
          .build();

  private static final Option PROGRESS_OPTION =
      Option.builder()
          .longOpt("progress")
          .required(false)
          .hasArg(false)
          .desc("Get progress report of a copy job.")
          .build();

  private static final Option PARTIAL_LISTING_OPTION =
      Option.builder()
          .longOpt("partial-listing")
          .required(false)
          .hasArg(false)
          .desc("Use partial directory listing. This limits the memory usage "
              + "and starts copy sooner for larger directory. But progress "
              + "report cannot report on the total number of files because the "
              + "whole directory is not listed yet.")
          .build();

  private static final Option PROGRESS_FORMAT =
      Option.builder()
          .longOpt("format")
          .required(false)
          .hasArg(true)
          .desc("Format of the progress report, supports TEXT and JSON. If not "
              + "set, TEXT is used.")
          .build();

  private static final Option PROGRESS_VERBOSE =
      Option.builder()
          .longOpt("verbose")
          .required(false)
          .hasArg(false)
          .desc("Whether to return a verbose progress report with detailed errors")
          .build();

  private static final Option CHECK_CONTENT =
      Option.builder()
          .longOpt("check-content")
          .required(false)
          .hasArg(false)
          .desc("Whether to check content hash when copying files")
          .build();

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public DistCpCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "distCp";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
    int commands = 0;
    if (cl.hasOption(SUBMIT_OPTION.getLongOpt())) {
      commands++;
    }
    if (cl.hasOption(STOP_OPTION.getLongOpt())) {
      commands++;
    }
    if (cl.hasOption(PROGRESS_OPTION.getLongOpt())) {
      commands++;
    }
    if (commands != 1) {
      throw new InvalidArgumentException("Must have one of submit / stop / progress");
    }
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(PARTIAL_LISTING_OPTION)
        .addOption(SUBMIT_OPTION)
        .addOption(STOP_OPTION)
        .addOption(PROGRESS_OPTION)
        .addOption(PROGRESS_FORMAT)
        .addOption(PROGRESS_VERBOSE)
        .addOption(CHECK_CONTENT);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI srcPath = new AlluxioURI(args[0]);
    AlluxioURI dstPath = new AlluxioURI(args[1]);
    if (srcPath.containsWildcard() || dstPath.containsWildcard()) {
      throw new UnsupportedOperationException("Copy does not support wildcard path");
    }

    if (cl.hasOption(SUBMIT_OPTION.getLongOpt())) {
      OptionalLong bandwidth = OptionalLong.empty();
      return submitCopy(srcPath, dstPath, bandwidth,
          cl.hasOption(PARTIAL_LISTING_OPTION.getLongOpt()),
          cl.hasOption(CHECK_CONTENT.getLongOpt()));
    }

    if (cl.hasOption(STOP_OPTION.getLongOpt())) {
      return stopCopy(srcPath, dstPath);
    }
    JobProgressReportFormat format = DEFAULT_FORMAT;
    if (cl.hasOption(PROGRESS_OPTION.getLongOpt())) {
      if (cl.hasOption(PROGRESS_FORMAT.getLongOpt())) {
        format = JobProgressReportFormat.valueOf(cl.getOptionValue(PROGRESS_FORMAT.getLongOpt()));
      }
      return getProgress(srcPath, dstPath, format, cl.hasOption(PROGRESS_VERBOSE.getLongOpt()));
    }
    return 0;
  }

  private int submitCopy(AlluxioURI srcPath, AlluxioURI dstPath, OptionalLong bandwidth,
      boolean usePartialListing, boolean checkContent) {
    CopyJobPOptions.Builder options =
        CopyJobPOptions.newBuilder().setPartialListing(usePartialListing)
                                    .setCheckContent(checkContent);
    if (bandwidth.isPresent()) {
      options.setBandwidth(bandwidth.getAsLong());
    }
    CopyJobRequest job =
        new CopyJobRequest(srcPath.toString(), dstPath.toString(), options.build());
    try (
        CloseableResource<FileSystemMasterClient> client = mFsContext.acquireMasterClientResource())
    {
      Optional<String> jobId = client.get().submitJob(job);
      if (jobId.isPresent()) {
        System.out.printf("Copy from '%s' to '%s' is successfully submitted. JobId: %s%n", srcPath,
            dstPath, jobId.get());
      }
      else {
        System.out.printf("Copy already running from path '%s' to '%s', updated the job with "
                + "new bandwidth: %s", srcPath, dstPath,
            bandwidth.isPresent() ? String.valueOf(bandwidth.getAsLong()) : "unlimited");
      }
      return 0;
    } catch (StatusRuntimeException e) {
      System.out.println(
          "Failed to submit copy job from " + srcPath + " to " + dstPath + ": " + e.getMessage());
      return -1;
    }
  }

  private int stopCopy(AlluxioURI srcPath, AlluxioURI dstPath) {
    try (
        CloseableResource<FileSystemMasterClient> client = mFsContext.acquireMasterClientResource())
    {
      if (client.get().stopJob(
          JobDescription.newBuilder().setPath(srcPath.toString() + ":" + dstPath.toString())
                        .setType(JOB_TYPE).build())) {
        System.out.printf("Copy job from '%s' to '%s' is successfully stopped.%n", srcPath,
            dstPath);
      }
      else {
        System.out.printf("Cannot find copy job from path %s to %s, it might have already been "
            + "stopped or finished%n", srcPath, dstPath);
      }
      return 0;
    } catch (StatusRuntimeException e) {
      System.out.println(
          "Failed to stop copy job from " + srcPath + " to " + dstPath + ": " + e.getMessage());
      return -1;
    }
  }

  private int getProgress(AlluxioURI srcPath, AlluxioURI dstPath, JobProgressReportFormat format,
      boolean verbose) {
    try (
        CloseableResource<FileSystemMasterClient> client = mFsContext.acquireMasterClientResource())
    {
      System.out.println("Progress for copying path '" + srcPath + "' to '" + dstPath + "':");
      System.out.println(client.get().getJobProgress(
          JobDescription.newBuilder().setPath(srcPath.toString() + ":" + dstPath.toString())
                        .setType(JOB_TYPE).build(), format, verbose));
      return 0;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        System.out.println("Copy for path '" + srcPath + "' to '" + dstPath + "' cannot be found.");
        return -2;
      }
      System.out.println(
          "Failed to get progress for copy job from " + srcPath + " to " + dstPath + ": "
              + e.getMessage());
      return -1;
    }
  }

  @Override
  public String getUsage() {
    return "For distributed copy:\n"
        + "\tcp <src> <dst> --submit [--bandwidth N] [--verify] [--partial-listing] [--overwrite]\n"
        + "\tcp <src> <dst> --stop\n"
        + "\tcp <src> <dst> --progress [--format TEXT|JSON] [--verbose]\n";
  }

  @Override
  public String getDescription() {
    return "Copies a file or a directory between UFSs"
        + "The --submit flag is needed to submit a copy job. The --stop flag is needed to "
        + "stop a copy job. The --progress flag is needed to track the progress of a copy job.";
  }
}
