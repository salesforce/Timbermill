package com.datorama.oss.timbermill.unit;

public class LocalTask extends Task{

    public LocalTask(Task task) {
        name = task.getName();
        parentId = task.getParentId();
        primaryId = task.getPrimaryId();
        parentsPath = task.getParentsPath();
        orphan = task.isOrphan();
        ctx.putAll(task.getCtx());
    }

    public int estimatedSize() {
        int nameSize = name == null ? 0 : name.length();
        int parentIdSize = parentId == null ? 0 : parentId.length();
        int primaryIdSize = primaryId == null ? 0 : primaryId.length();
        int parentsPathSize = parentsPath == null || parentsPath.isEmpty() ? 0 : parentsPath.stream().mapToInt(String::length).sum();
        int ctxSize = ctx == null || ctx.isEmpty() ? 0 : ctx.entrySet().stream().mapToInt(entry -> entry.getKey().length() + entry.getValue().length()).sum();
        return nameSize + parentIdSize + primaryIdSize + parentsPathSize + ctxSize;
    }
}
