/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.buffer;

final class PoolSubpage<T> implements PoolSubpageMetric {

    // 当前SubPage所属的PoolChunk
    final PoolChunk<T> chunk;
    // 分配的二叉树的节点的id索引
    private final int memoryMapIdx;
    // 当前叶子节点的 在整块大内存上的内存偏移量 8k 16k 24k...
    private final int runOffset;
    // 页内存大小 默认8k
    private final int pageSize;
    // 默认创建数组长度8
    private final long[] bitmap;

    PoolSubpage<T> prev;
    PoolSubpage<T> next;

    // 为true表示当前subpage可用
    boolean doNotDestroy;
    // 表示当前SubPage管理的 合适规格大小
    int elemSize;
    // subpage能个存放合适规格大小elemSize的个数
    private int maxNumElems;
    // 当前合适规格大小elemSize占用情况， bitmap数组的前几个索引
    // 其实就是表示这个 bitmap 的有效数据长度，初始化时将 bitmap 数组的前 bitmapLength 数据置为 0，表示未被占用
    private int bitmapLength;
    // 申请内存时，表示下个可用的位图的下标
    private int nextAvail;
    // 该subpage还剩下几个位置可以用来存放 合适规格大小elemSize，会随着分配慢慢减少
    private int numAvail;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /** Special constructor that creates a linked list head */
    PoolSubpage(int pageSize) {
        chunk = null;
        memoryMapIdx = -1;
        runOffset = -1;
        elemSize = -1;
        this.pageSize = pageSize;
        bitmap = null;
    }


    /**
     * 创建 PoolSubpage
     *
     * @param head arena范围内的pools 符合当前规格的head节点
     * @param chunk 当前PoolChunk对象，因为subpage需要知道它爸爸是谁
     * @param memoryMapIdx id 二叉树节点id
     * @param runOffset runOffset(id) 计算出当前叶子节点管理内存在整个内存的偏移量 eg.0,8k,16k,24k...
     * @param pageSize 页内存
     * @param elemSize 合适规格大小
     */
    PoolSubpage(PoolSubpage<T> head, PoolChunk<T> chunk, int memoryMapIdx, int runOffset, int pageSize, int elemSize) {
        this.chunk = chunk;
        // 叶子节点 id
        this.memoryMapIdx = memoryMapIdx;
        this.runOffset = runOffset;
        this.pageSize = pageSize;
        // 默认创建长度8的bitmap数组
        // 因为tiny最小规格是16b，以最小规格划分的话，需要多少个bit才能表示出这个内存的使用情况
        // 再除64是因为long类型占用8个bit，计算出需要多少个long才能表示整个位图
        bitmap = new long[pageSize >>> 10]; // pageSize / 16 / 64
        init(head, elemSize);
    }

    void init(PoolSubpage<T> head, int elemSize) {
        doNotDestroy = true;
        this.elemSize = elemSize;
        if (elemSize != 0) {
            // pageSize 8k / 合适规格大小 ， eg 1024 则除出来得8
            // maxNumElems表示该subPage最大能存几个 合适规格大小 的内存，不会再改变了
            // numAvail表示该subpage还剩下几个位置可以用来存放 合适规格大小，会随着分配慢慢减少
            maxNumElems = numAvail = pageSize / elemSize;
            nextAvail = 0;
            // maxNumElems除以64 因为long类型是64bit
            // 用于计算占用多少个long位置
            bitmapLength = maxNumElems >>> 6;
            if ((maxNumElems & 63) != 0) {
                // 如果有余数就需要长度再+1
                bitmapLength ++;
            }

            // 初始化为bitmap数组指定位置为0
            for (int i = 0; i < bitmapLength; i ++) {
                bitmap[i] = 0;
            }
        }
        addToPool(head);
    }

    /**
     * Returns the bitmap index of the subpage allocation.
     * 返回子页面分配的位图索引
     */
    long allocate() {
        if (elemSize == 0) {
            return toHandle(0);
        }

        // 条件1：说明当前subpage的内存已经分配完了
        // 条件2：subpage关闭了
        if (numAvail == 0 || !doNotDestroy) {
            return -1;
        }

        // 找到位图中 可用bit位置的 索引
        // 获取 bit 位的下标（不是数组的下标）
        final int bitmapIdx = getNextAvail();
        // 除以64，就是再bitmap[]数组中的索引了
        int q = bitmapIdx >>> 6;
        // 63 -> 0011 1111 就是按64取模运算，计算在一个long中的位置,从当前元素最低位开始的第几个比特位
        int r = bitmapIdx & 63;
        assert (bitmap[q] >>> r & 1) == 0;
        // 给指定位置的 r bit 赋值为1，表示该位已被占用
        bitmap[q] |= 1L << r;

        // numAvail自减，表示用了一块内存
        if (-- numAvail == 0) {
            // 此subpage的小内存分配完了，需要将其从PoolSubpage链表中移除
            removeFromPool();
        }

        return toHandle(bitmapIdx);
    }

    /**
     * @return {@code true} if this subpage is in use.
     *         {@code false} if this subpage is not used by its chunk and thus it's OK to be released.
     */
    boolean free(PoolSubpage<T> head, int bitmapIdx) {
        if (elemSize == 0) {
            return true;
        }
        int q = bitmapIdx >>> 6;
        int r = bitmapIdx & 63;
        assert (bitmap[q] >>> r & 1) != 0;
        bitmap[q] ^= 1L << r;

        setNextAvail(bitmapIdx);

        if (numAvail ++ == 0) {
            addToPool(head);
            return true;
        }

        if (numAvail != maxNumElems) {
            return true;
        } else {
            // Subpage not in use (numAvail == maxNumElems)
            if (prev == next) {
                // Do not remove if this subpage is the only one left in the pool.
                return true;
            }

            // Remove this subpage from the pool if there are other subpages left in the pool.
            doNotDestroy = false;
            removeFromPool();
            return false;
        }
    }

    private void addToPool(PoolSubpage<T> head) {
        // head 类似于哨兵节点，插入到 head 的下一个元素，
        assert prev == null && next == null;
        prev = head;
        next = head.next;
        next.prev = this;
        head.next = this;
    }

    private void removeFromPool() {
        assert prev != null && next != null;
        prev.next = next;
        next.prev = prev;
        next = null;
        prev = null;
    }

    private void setNextAvail(int bitmapIdx) {
        nextAvail = bitmapIdx;
    }

    private int getNextAvail() {
        // nextAvail 初始时是0,
        // 当某个bitmap占用的内存还给subpage时，这个内存占用的bit的索引值，会设置给nextAvil，下次再申请就直接使用nextAvail就可以了
        int nextAvail = this.nextAvail;
        if (nextAvail >= 0) {
            this.nextAvail = -1;
            // 1.初始时 nextAvail = 0
            // 2.其他ByteBuf归还内存时，会设置nextAvail为她占用的那块内存对应的bit索引值
            // 一个 Subpage 被释放之后，会记录当前 Subpage 的 bitmapIdx 的位置，下次分配可以直接通过 bitmapIdx 获取一个 Subpage
            return nextAvail;
        }
        // 走到此处 nextAvail < 0 ，表示没有被释放的子内存
        // 调用下面的方法继续查找下一个可用内存
        return findNextAvail();
    }

    private int findNextAvail() {
        // 位图
        final long[] bitmap = this.bitmap;
        // 位图数组的有效长度
        final int bitmapLength = this.bitmapLength;
        // 假设 当前subpage的规格是 32b 对外分配了 68块小内存，并且68块小内存都没归还给subpage，那么他的位图长下面这样
        // bitmap[0] = 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111
        // bitmap[1] = 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 1111
        // bitmap[2] = 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000
        // bitmap[3] = 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000
        for (int i = 0; i < bitmapLength; i ++) {
            long bits = bitmap[i];
            // bitmap为0表示未被占用
            // 条件不成立说明 bitmap[i]表示的内存全部分配出去了...没办法在bitmap[i]的这个bitmap上分配了
            // 0000 0001 没用有占用完 取反就不是0，就去这long的8bit中找一个出来占用
            // 1111 1111 全部占用 取反就是 0了，就去找一个位置
            if (~bits != 0) {
                // 进入此处，说明bitmap[i]还有空余空间 可以给当前这次allocate去支撑
                // findNextAvail0去查找一个 空闲内存的bitmap索引
                return findNextAvail0(i, bits);
            }
        }
        return -1;
    }

    /**
     *
     * @param i 当前bit位在 bitmap的位置
     * @param bits 当前bitmap[i]表示的这一小块bitmap
     * @return
     */
    private int findNextAvail0(int i, long bits) {
        // subpage能个存放合适规格大小elemSize的个数
        // eg. 规格是1024，那么maxNumElems就是8了 一页内存只能放8个
        final int maxNumElems = this.maxNumElems;
        // i乘以64 eg. i=0 -> baseVal=0
        // i乘以64 eg. i=1 -> baseVal=64
        // 乘以64, 代表当前long 的第一个下标
        // 假设 当前subpage的规格是 32b 对外分配了 68块小内存，并且68块小内存都没归还给subpage，那么他的位图长下面这样
        // bitmap[0] = 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111 1111
        // bitmap[1] = 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 1111
        // bitmap[2] = 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000
        // bitmap[3] = 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000  0000
        final int baseVal = i << 6;

        // 循环64 次(指代当前的下标)
        for (int j = 0; j < 64; j ++) {
            // 第一位为0(如果是2 的倍数, 则第一位就是0)
            // 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 1111
            // 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0001
            if ((bits & 1) == 0) {
                // 这里相当于加, 将i*64 之后加上j, 获取绝对下标
                int val = baseVal | j;
                // 小于块数(不能越界
                if (val < maxNumElems) {
                    return val;
                } else {
                    break;
                }
            }
            // 当前下标不为0 右移一位
            // 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0111 j=0
            // 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0011 j=1
            // 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 j=3
            bits >>>= 1;
        }
        return -1;
    }

    /**
     * (long) bitmapIdx << 32 是将bitmapIdx 右移32 位, 而32 位正好是一个int 的长度,
     * 这样, 通过(long) bitmapIdx <<32 | memoryMapIdx 计算,
     * 就可以将memoryMapIdx, 也就是page 所属的下标的二进制数保存在(long) bitmapIdx<< 32 的低32 位中
     * 0x4000000000000000L 是一个最高位是1 并且所有低位都是0 的二进制数,
     * 这样通过按位或的方式可以将(long) bitmapIdx << 32 | memoryMapIdx
     * 计算出来的结果保存在0x4000000000000000L 的所有低位中,这样, 返回对的数字就可以指向chunk 中唯一的一块内存，
     *
     *
     * 假设 bitmapIdx = 68
     * 68 => 68 << 32 => 0000 0000 0000 0000 0000 0000 0100 0100 0000 0000 0000 0000 0000 0000 0000 0000
     * memoryMapIdx 是当前subpage占用的叶子节点的序号
     * @param bitmapIdx
     * @return
     */
    private long toHandle(int bitmapIdx) {
        // memoryMapIdx 二叉树节点id
        return 0x4000000000000000L | (long) bitmapIdx << 32 | memoryMapIdx;
    }

    @Override
    public String toString() {
        final boolean doNotDestroy;
        final int maxNumElems;
        final int numAvail;
        final int elemSize;
        if (chunk == null) {
            // This is the head so there is no need to synchronize at all as these never change.
            doNotDestroy = true;
            maxNumElems = 0;
            numAvail = 0;
            elemSize = -1;
        } else {
            synchronized (chunk.arena) {
                if (!this.doNotDestroy) {
                    doNotDestroy = false;
                    // Not used for creating the String.
                    maxNumElems = numAvail = elemSize = -1;
                } else {
                    doNotDestroy = true;
                    maxNumElems = this.maxNumElems;
                    numAvail = this.numAvail;
                    elemSize = this.elemSize;
                }
            }
        }

        if (!doNotDestroy) {
            return "(" + memoryMapIdx + ": not in use)";
        }

        return "(" + memoryMapIdx + ": " + (maxNumElems - numAvail) + '/' + maxNumElems +
                ", offset: " + runOffset + ", length: " + pageSize + ", elemSize: " + elemSize + ')';
    }

    @Override
    public int maxNumElements() {
        if (chunk == null) {
            // It's the head.
            return 0;
        }

        synchronized (chunk.arena) {
            return maxNumElems;
        }
    }

    @Override
    public int numAvailable() {
        if (chunk == null) {
            // It's the head.
            return 0;
        }

        synchronized (chunk.arena) {
            return numAvail;
        }
    }

    @Override
    public int elementSize() {
        if (chunk == null) {
            // It's the head.
            return -1;
        }

        synchronized (chunk.arena) {
            return elemSize;
        }
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    void destroy() {
        if (chunk != null) {
            chunk.destroy();
        }
    }
}
