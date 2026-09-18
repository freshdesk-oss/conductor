package com.netflix.conductor.freshworks.deletion.model;

/** Lifecycle states reported via the {@code ACCOUNT_DELETION_STATUS} event. */
public enum DeletionStatus {
    STARTED,
    SUCCESS,
    FAILURE,
    NOT_FOUND
}
