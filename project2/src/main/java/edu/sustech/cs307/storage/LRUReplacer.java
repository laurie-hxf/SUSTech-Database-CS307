package edu.sustech.cs307.storage;

import java.util.*;

public class LRUReplacer {

    private final int maxSize;
    private final Set<Integer> pinnedFrames = new HashSet<>();
    private final Set<Integer> LRUHash = new HashSet<>();
    private final LinkedList<Integer> LRUList = new LinkedList<>();

    public LRUReplacer(int numPages) {
        this.maxSize = numPages;
    }

    public int Victim() {
        if (LRUList.isEmpty()) {
            return -1;
        }
        int victim = LRUList.removeFirst();
        LRUHash.remove(victim);
        return victim;
    }

    public void Pin(int frameId) {
        // 如果已在 pinned 中，视为重复 Pin，忽略
        if (pinnedFrames.contains(frameId)) {
            return;
        }
        // 如果当前总数已达最大容量，拒绝
        if (size() >= maxSize) {
            throw new RuntimeException("REPLACER IS FULL");
        }
        // 从 LRU 中移除（如果在的话）
        if (LRUHash.remove(frameId)) {
            LRUList.removeFirstOccurrence(frameId);
        }
        pinnedFrames.add(frameId);
    }


    public void Unpin(int frameId) {
        // 必须先在 pinnedFrames 中，否则非法
        if (!pinnedFrames.contains(frameId)) {
            throw new RuntimeException("UNPIN PAGE NOT FOUND");
        }
        // 从 pinned 中移除
        pinnedFrames.remove(frameId);
        // 如果未超出容量，加入到 LRU 尾部
        if (size() < maxSize) {
            // 避免二次加入
            if (!LRUHash.contains(frameId)) {
                LRUList.addLast(frameId);
                LRUHash.add(frameId);
            }
        }
        // 若 capacity 已满，则此帧不放入 LRU，直到有 Victim 后容量释放
    }


    public int size() {
        return LRUList.size() + pinnedFrames.size();
    }
}