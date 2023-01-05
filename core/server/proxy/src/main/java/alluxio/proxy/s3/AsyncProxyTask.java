package alluxio.proxy.s3;

import alluxio.stage.AlluxioStageTask;

import javax.servlet.AsyncContext;

public class AsyncProxyTask extends AlluxioStageTask {

    private AsyncContext mAsyncContext;
    public AsyncProxyTask() {

    }

    @Override
    public void execute() {

    }

    @Override
    public void handleStageException(Throwable e) {

    }

    public AsyncContext getAsyncContext() {
        return mAsyncContext;
    }
}
