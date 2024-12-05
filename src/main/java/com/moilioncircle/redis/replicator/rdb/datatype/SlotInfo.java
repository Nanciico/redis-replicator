package com.moilioncircle.redis.replicator.rdb.datatype;

import java.io.Serializable;

public class SlotInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long currentSlot;

    private final long slotKeySize;

    private final long slotExpiresSize;

    public SlotInfo(long currentSlot, long slotKeySize, long slotExpiresSize) {
        this.currentSlot = currentSlot;
        this.slotKeySize = slotKeySize;
        this.slotExpiresSize = slotExpiresSize;
    }

    public long getCurrentSlot() {
        return currentSlot;
    }

    public long getSlotKeySize() {
        return slotKeySize;
    }

    public long getSlotExpiresSize() {
        return slotExpiresSize;
    }
}