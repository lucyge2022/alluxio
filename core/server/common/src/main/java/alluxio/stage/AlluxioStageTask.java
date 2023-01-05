package alluxio.stage;

import alluxio.util.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AlluxioStageTask {
    private static final Logger LOG = LoggerFactory.getLogger(AlluxioStageTask.class);

    public AlluxioStageTask() {}

    abstract public void execute();
    abstract public void handleStageException(Throwable e);

    public void beforeExecute() {

    }

    public void afterExecute() {

    }

}
