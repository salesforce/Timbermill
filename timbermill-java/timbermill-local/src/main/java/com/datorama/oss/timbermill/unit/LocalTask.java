package com.datorama.oss.timbermill.unit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalTask extends Task{

    private static final Logger LOG = LoggerFactory.getLogger(LocalTask.class);

    public LocalTask(Task task) {
        name = task.getName();
        parentId = task.getParentId();
        primaryId = task.getPrimaryId();
        parentsPath = task.getParentsPath();
        orphan = task.isOrphan();
        ctx.putAll(task.getCtx());
    }

}
