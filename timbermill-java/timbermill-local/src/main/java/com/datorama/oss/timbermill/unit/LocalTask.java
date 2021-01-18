package com.datorama.oss.timbermill.unit;

public class LocalTask extends Task{

    public LocalTask(Task task) {
        name = task.getName();
        parentId = task.getParentId();
        primaryId = task.getPrimaryId();
        parentsPath = task.getParentsPath();
        orphan = task.isOrphan();
        index = task.getIndex();
        ctx.putAll(task.getCtx());
    }

}
