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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Description of algorithm for PageRun/PoolSubpage allocation from PoolChunk
 * 从 PoolChunk 分配 PageRun/PoolSubpage 的算法说明
 *
 * Notation: The following terms are important to understand the code
 * > page  - a page is the smallest unit of memory chunk that can be allocated
 *           page页是可以分配的最小内存块单位
 * > chunk - a chunk is a collection of pages
 *           chunk块是页面的集合
 * > in this code chunkSize = 2^{maxOrder} * pageSize
 *                poolChunk管理的内存大小 = 2^{二叉树最大深度} * 一页内存大小
 *
 * To begin we allocate a byte array of size = chunkSize
 * Whenever a ByteBuf of given size needs to be created we search for the first position
 * in the byte array that has enough empty space to accommodate the requested size and
 * return a (long) handle that encodes this offset information, (this memory segment is then
 * marked as reserved so it is always used by exactly one ByteBuf and no more)
 * 首先需要分配一个chunkSize大小的数组，
 * 每当需要创建给定大小的 ByteBuf 时，会在字节数组中搜索第一个满足条件的位置，
 * 该位置有足够的空间来容纳请求的大小，并返回一个（long）句柄来表示偏移信息
 * 然后将该内存段标记为保留，因此它始终只被一个 ByteBuf 使用
 *
 * For simplicity all sizes are normalized according to PoolArena#normalizeCapacity method
 * This ensures that when we request for memory segments of size >= pageSize the normalizedCapacity
 * equals the next nearest power of 2
 * 为简单起见，所有大小都根据 PoolArena 的 normalizeCapacity 方法进行了标准化
 * 这确保当我们请求大小 >= pageSize 的内存段时， normalizedCapacity 等于下一个最接近的 2 的幂
 *
 *
 * To search for the first offset in chunk that has at least requested size available we construct a
 * complete balanced binary tree and store it in an array (just like heaps) - memoryMap
 * 为了在块中搜索至少具有可用大小的第一个偏移量，
 * 我们构建了一个完整的平衡二叉树并将其存储在一个数组中（就像堆一样） - memoryMap
 *
 * The tree looks like this (the size of each node being mentioned in the parenthesis)
 *
 * depth=0        1 node (chunkSize)
 * depth=1        2 nodes (chunkSize/2)
 * ..
 * ..
 * depth=d        2^d nodes (chunkSize/2^d)
 * ..
 * depth=maxOrder 2^maxOrder nodes (chunkSize/2^{maxOrder} = pageSize)
 *
 * depth=maxOrder is the last level and the leafs consist of pages
 *
 * With this tree available searching in chunkArray translates like this:
 * To allocate a memory segment of size chunkSize/2^k we search for the first node (from left) at height k
 * which is unused
 * 深度优先
 *
 * Algorithm:
 * ----------
 * 使用符号对 memoryMap 中的树进行编码
 * Encode the tree in memoryMap with the notation
 *   memoryMap[id] = x => in the subtree rooted at id, the first node that is free to be allocated
 *   is at depth x (counted from depth=0) i.e., at depths [depth_of_id, x), there is no node that is free
 *
 *  As we allocate & free nodes, we update values stored in memoryMap so that the property is maintained
 *  当我们分配和释放节点时，我们更新存储在 memoryMap 中的值，以便维护该属性
 *
 * Initialization - 开始时memoryMap[id]的值是 二叉树节点的深度值
 *   In the beginning we construct the memoryMap array by storing the depth of a node at each node
 *     i.e., memoryMap[id] = depth_of_id
 *
 * Observations:
 * -------------
 * 1) memoryMap[id] = depth_of_id  => it is free / unallocated
 *                                    当二叉树的节点的分配能力和该节点的深度相等，则说明该阶段还未分配内存
 * 2) memoryMap[id] > depth_of_id  => at least one of its child nodes is allocated, so we cannot allocate it, but
 *                                    some of its children can still be allocated based on their availability
 *                                    至少有一个子节点的内存已经分配出去了，所以这个节点就不能分配内存，但是他的一些子节点还可以继续分配内存
 * 3) memoryMap[id] = maxOrder + 1 => the node is fully allocated & thus none of its children can be allocated, it
 *                                    is thus marked as unusable
 *                                    当某个节点的分配能力等于maxOrder + 1,则表示当前节点的内存已经完全分配出去了
 *
 * Algorithm: [allocateNode(d) => we want to find the first node (from left) at height h that can be allocated]
 * ----------
 * 1) start at root (i.e., depth = 0 or id = 1) 从根节点开始找
 * 2) if memoryMap[1] > d => cannot be allocated from this chunk 假如memoryMap[1] > d, 表示不能从当前chunk分配内存了
 * 3) if left node value <= h; we can allocate from left subtree so move to left and repeat until found
 *      假如左节点的值小于深度值  我们可以从左子树分配，所以向左移动并重复直到找到
 * 4) else try in right subtree 尝试查找右子树
 *
 * Algorithm: [allocateRun(size)]
 * ----------
 * 1) Compute d = log_2(chunkSize/size)  计算需要的深度值
 * 2) Return allocateNode(d)
 *
 * Algorithm: [allocateSubpage(size)]
 * ----------
 * 1) use allocateNode(maxOrder) to find an empty (i.e., unused) leaf (i.e., page)
 * 2) use this handle to construct the PoolSubpage object or if it already exists just call init(normCapacity)
 *    note that this PoolSubpage object is added to subpagesPool in the PoolArena when we init() it
 *
 * Note:
 * -----
 * In the implementation for improving cache coherence,
 * we store 2 pieces of information depth_of_id and x as two byte values in memoryMap and depthMap respectively
 *
 * memoryMap[id]= depth_of_id  is defined above
 * depthMap[id]= x  indicates that the first node which is free to be allocated is at depth x (from root)
 */
final class PoolChunk<T> implements PoolChunkMetric {

    private static final int INTEGER_SIZE_MINUS_ONE = Integer.SIZE - 1;

    // 当前创建的PoolChunk对象归属的PoolArena
    final PoolArena<T> arena;
    // 分配的内存 ByteBuffer
    final T memory;
    final boolean unpooled;
    final int offset;
    // 二叉树节点内存分配能力数组
    private final byte[] memoryMap;
    // 二叉树节点深度数组
    private final byte[] depthMap;
    // PoolSubpage数组, 默认2048
    private final PoolSubpage<T>[] subpages;
    /** Used to determine if the requested capacity is equal to or greater than pageSize. */
    // 掩码 用于和 pageSize的比较大小
    private final int subpageOverflowMask;
    // 页内存大小 默认8k
    private final int pageSize;
    // 默认13
    private final int pageShifts;
    // 默认11
    private final int maxOrder;
    // 默认16mb
    private final int chunkSize;
    private final int log2ChunkSize;
    // 最多可申请多少个subpage 默认2048
    private final int maxSubpageAllocs;
    /** Used to mark memory as unusable */
    // 标记节点已经没有能力分配指定大小的内存 默认12
    private final byte unusable;

    // Use as cache for ByteBuffer created from the memory. These are just duplicates and so are only a container
    // around the memory itself. These are often needed for operations within the Pooled*ByteBuf and so
    // may produce extra GC, which can be greatly reduced by caching the duplicates.
    //
    // This may be null if the PoolChunk is unpooled as pooling the ByteBuffer instances does not make any sense here.
    private final Deque<ByteBuffer> cachedNioBuffers;

    // 能够分配的数据大小
    private int freeBytes;

    // PoolChunk 所属的 PoolChunkList
    PoolChunkList<T> parent;
    // 前驱
    PoolChunk<T> prev;
    // 后驱
    PoolChunk<T> next;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /**
     * 创建 PoolChunk 对象
     *
     * @param arena 当前创建的PoolChunk对象归属的PoolArena
     * @param memory 分配的内存 ByteBuffer
     * @param pageSize pageSize 默认8k
     * @param maxOrder maxOrder 默认11
     * @param pageShifts pageShifts 默认13
     * @param chunkSize chunkSize 默认16mb
     * @param offset offset 内存偏移量
     */
    PoolChunk(PoolArena<T> arena, T memory, int pageSize, int maxOrder, int pageShifts, int chunkSize, int offset) {
        unpooled = false;
        this.arena = arena;
        // 让 chunk 直接持有 ByteBuffer 的引用
        this.memory = memory;
        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.maxOrder = maxOrder;
        this.chunkSize = chunkSize;
        this.offset = offset;
        // 表示某个节点的内存被分配出去后，它的可分配深度值就需要改成 unusable，标记节点已经没有能力分配指定大小的内存 默认12
        // 二叉树每个节点有三个维度的数据，（深度，ID，可分配深度值 初始值与深度一致）
        unusable = (byte) (maxOrder + 1);
        // 24 干嘛使的？
        // chunkSize 16mb = 1 0000 0000 0000 0000 0000 0000
        log2ChunkSize = log2(chunkSize);
        // 用于和pageSize比较大小
        // pageSize - 1 = 0000 0000 0000 0000 0000 0001 1111 1111
        // ~              1111 1111 1111 1111 1111 1110 0000 0000
        subpageOverflowMask = ~(pageSize - 1);
        // 能够分配的内存大小，默认初始时是 16mb
        freeBytes = chunkSize;

        assert maxOrder < 30 : "maxOrder should be < 30, but is: " + maxOrder;
        // 最多可申请多少个subpage 默认2048
        maxSubpageAllocs = 1 << maxOrder;

        // 构建二叉树 i的左子节点 2i i的右子节点2i+1
        // mermoryMap 数组长度默认 4096，存放的数据为 标识当前i位置的节点的可分配内存能力值 [0,0,1,1,2,2,2,2,3,3,3,3,3,3,3,3...] 注意这个值会改变
        // depthMap 数组长度默认 4096，存放的数据为节点的深度 [0,0,1,1,2,2,2,2,3,3,3,3,3,3,3,3...] 注意这个值就是深度，不会再变了
        memoryMap = new byte[maxSubpageAllocs << 1];
        depthMap = new byte[memoryMap.length];
        int memoryMapIndex = 1;
        for (int d = 0; d <= maxOrder; ++ d) { // move down the tree one level at a time
            int depth = 1 << d;
            for (int p = 0; p < depth; ++ p) {
                // in each level traverse left to right and set value to the depth of subtree
                memoryMap[memoryMapIndex] = (byte) d;
                depthMap[memoryMapIndex] = (byte) d;
                memoryMapIndex ++;
            }
        }

        // 创建PoolSubpage数组 maxSubpageAllocs默认2048
        subpages = newSubpageArray(maxSubpageAllocs);
        cachedNioBuffers = new ArrayDeque<ByteBuffer>(8);
    }

    /** Creates a special chunk that is not pooled. */
    PoolChunk(PoolArena<T> arena, T memory, int size, int offset) {
        unpooled = true;
        this.arena = arena;
        this.memory = memory;
        this.offset = offset;
        memoryMap = null;
        depthMap = null;
        subpages = null;
        subpageOverflowMask = 0;
        pageSize = 0;
        pageShifts = 0;
        maxOrder = 0;
        unusable = (byte) (maxOrder + 1);
        chunkSize = size;
        log2ChunkSize = log2(chunkSize);
        maxSubpageAllocs = 0;
        cachedNioBuffers = null;
    }

    @SuppressWarnings("unchecked")
    private PoolSubpage<T>[] newSubpageArray(int size) {
        return new PoolSubpage[size];
    }

    @Override
    public int usage() {
        final int freeBytes;
        synchronized (arena) {
            freeBytes = this.freeBytes;
        }
        return usage(freeBytes);
    }

    private int usage(int freeBytes) {
        if (freeBytes == 0) {
            return 100;
        }

        int freePercentage = (int) (freeBytes * 100L / chunkSize);
        if (freePercentage == 0) {
            return 99;
        }
        return 100 - freePercentage;
    }

    boolean allocate(PooledByteBuf<T> buf, int reqCapacity, int normCapacity) {
        final long handle;
        if ((normCapacity & subpageOverflowMask) != 0) { // >= pageSize
            // 条件成立：说明当前规格的大小 大于一页内存的大小 pageSize
            // 返回的是分配的内存占用的二叉树的树节点的 id 值，假如返回 -1 表示分配失败
            // 通过这个返回的下标可以定位到一块内存
            handle =  allocateRun(normCapacity);
        } else {
            // 分配小于一页内存 pageSize 8k 的内存
            handle = allocateSubpage(normCapacity);
        }

        if (handle < 0) {
            // 小于0表示分配内存失败了
            return false;
        }
        ByteBuffer nioBuffer = cachedNioBuffers != null ? cachedNioBuffers.pollLast() : null;
        initBuf(buf, nioBuffer, handle, reqCapacity);
        return true;
    }

    /**
     * Update method used by allocate
     * This is triggered only when a successor is allocated and all its predecessors
     * need to update their state
     * The minimal depth at which subtree rooted at id has some free space
     *
     * @param id id
     */
    private void updateParentsAlloc(int id) {
        // eg. 2048 会影响 1024 512 256 128 64 32 16 8 4 2 1
        while (id > 1) {
            int parentId = id >>> 1; // 除2
            // 获取当前节点的分配能力值
            byte val1 = value(id);
            // 获取当前节点的兄弟节点的分配能力值
            byte val2 = value(id ^ 1);
            // 取val1和val2较小的一个
            byte val = val1 < val2 ? val1 : val2;
            // 设置父节点
            setValue(parentId, val);
            // 更新id为父节点，继续更新深度值
            id = parentId;
        }
    }

    /**
     * Update method used by free
     * This needs to handle the special case when both children are completely free
     * in which case parent be directly allocated on request of size = child-size * 2
     *
     * @param id id
     */
    private void updateParentsFree(int id) {
        int logChild = depth(id) + 1;
        while (id > 1) {
            int parentId = id >>> 1;
            byte val1 = value(id);
            byte val2 = value(id ^ 1);
            logChild -= 1; // in first iteration equals log, subsequently reduce 1 from logChild as we traverse up

            if (val1 == logChild && val2 == logChild) {
                setValue(parentId, (byte) (logChild - 1));
            } else {
                byte val = val1 < val2 ? val1 : val2;
                setValue(parentId, val);
            }

            id = parentId;
        }
    }

    /**
     * Algorithm to allocate an index in memoryMap when we query for a free node
     * at depth d
     *
     * @param d depth 合适规格大小 对应的满二叉树的深度值
     * @return index in memoryMap
     */
    private int allocateNode(int d) {
        // id = 1 索引为1的是满二叉树的根节点， 方法是从根节点向下查找一个合适的节点，完成内存分配
        int id = 1;
        // 这个表达式的作用是 最后的 d 位置为0，其他的位置为1
        // 假如申请 8k，d = 11    1 << 11 = 2048
        // -2048的二进制如下
        // 1111 1111 1111 1111 1111 1000 0000 0000
        int initial = - (1 << d); // has last d bits = 0 and rest all = 1
        // 获取二叉树根节点的可分配深度能力值
        byte val = value(id);
        if (val > d) { // unusable
            // 进入到此处，val > d,说明根节点已经没有能力分配了，返回-1表示申请失败
            return -1;
        }
        // 深度优先
        // val < d 表示val能够分配深度d表示的内存大小，继续向下搜查子节点是否有能力分配
        // 条件1：val < d，val 表示二叉树中某个节点的可分配深度能力值，条件成立则说明当前节点管理的内存足够大，需要继续往下查找子节点是否具有分配能力
        // 条件2：(id & initial) 假如值为0，则说明子节点已经有分配过，当前节点的深度值其实是比正常的小的
        // 当前下标对应的节点值如果小于层级，或者当前下标小于层级的初始小标。
        while (val < d || (id & initial) == 0) { // id & initial == 1 << d for all ids at depth d, for < d it is 0
            // 索引乘以 2，得到左子节点的位置
            id <<= 1;
            // 获取左移之后的 二叉树节点的可分配能力值
            val = value(id);
            // val > d 说明左子节点的分配能力不够了，需要看看右子节点能否分配
            if (val > d) {
                // 伙伴算法，其实就是获取兄弟节点的id
                // id 为偶数 +1，为奇数则 -1
                // eg. 2048 1000 0000 0000
                //   ^ 1    0000 0000 0001
                id ^= 1;
                // 获取兄弟节点的分配能力值
                val = value(id);
            }
        }
        // 获取合适分配节点的深度能力值
        byte value = value(id);
        assert value == d && (id & initial) == 1 << d : String.format("val = %d, id & initial = %d, d = %d",
                value, id & initial, d);
        // 占用节点，就是将该节点的分配能力值改为unusable
        setValue(id, unusable); // mark as unusable
        // 更新父节点的分配能力值
        updateParentsAlloc(id);
        return id;
    }

    /**
     * Allocate a run of pages (>=1)
     *
     * @param normCapacity normalized capacity 合适规格的大小 8K 16k...
     * @return index in memoryMap
     */
    private long allocateRun(int normCapacity) {
        // eg. normCapacity = 8k,  11 - (13 - 13) = 11
        // eg. normCapacity = 16k, 11 - (14 - 13) = 10
        // 表示需要去深度为d的树节点上分配内存
        int d = maxOrder - (log2(normCapacity) - pageShifts);
        int id = allocateNode(d);
        if (id < 0) {
            return id;
        }
        // 减少可分配内存的大小
        freeBytes -= runLength(id);
        return id;
    }

    /**
     * Create / initialize a new PoolSubpage of normCapacity
     * Any PoolSubpage created / initialized here is added to subpage pool in the PoolArena that owns this PoolChunk
     *
     * 创建/初始化一个新的normCapacity的PoolSubpage
     * 在这里创建/初始化的任何PoolSubpage都会被添加到拥有这个PoolChunk的PoolArena中的subpage pool
     *
     * @param normCapacity normalized capacity 合适的规格大小
     * @return index in memoryMap
     */
    private long allocateSubpage(int normCapacity) {
        // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and synchronize on it.
        // This is need as we may add it back and so alter the linked-list structure.
        // 获取 arena 在指定位置的链表头结点
        PoolSubpage<T> head = arena.findSubpagePoolHead(normCapacity);
        // 深度赋值为11，因为接下来要从chunk上申请一页内存，也就是 8k 内存
        int d = maxOrder; // subpages are only be allocated from pages i.e., leaves
        synchronized (head) {
            // 申请一个 8k 内存，返回叶子节点的 id
            int id = allocateNode(d);
            if (id < 0) {
                // 说明申请失败了
                return id;
            }

            // 获取当前 Chunk 的 subpages 数组的引用
            final PoolSubpage<T>[] subpages = this.subpages;
            final int pageSize = this.pageSize;

            // 可用内存大小减少一页内存 8k
            freeBytes -= pageSize;

            // 获得分得到的内存在 subpages 数组上的索引
            int subpageIdx = subpageIdx(id);
            // 获取数组当前位置的subpage
            PoolSubpage<T> subpage = subpages[subpageIdx];
            if (subpage == null) {
                // 第一次创建就是 null
                // 参数1：head arena 范围内的 pools 符合当前规格的head节点
                // 参数2：当前PoolChunk对象，因为subpage需要知道它爸爸是谁
                // 参数3：id 二叉树节点id
                // 参数4：runOffset(id) 计算出当前叶子节点管理内存在整个内存的偏移量 eg.0,8k,16k,24k...
                // 参数5：页内存
                // 参数6：合适规格大小 是 tiny 和 small 规格的
                subpage = new PoolSubpage<T>(head, this, id, runOffset(id), pageSize, normCapacity);
                subpages[subpageIdx] = subpage;
            } else {
                subpage.init(head, normCapacity);
            }
            return subpage.allocate();
        }
    }

    /**
     * Free a subpage or a run of pages
     * When a subpage is freed from PoolSubpage, it might be added back to subpage pool of the owning PoolArena
     * If the subpage pool in PoolArena has at least one other PoolSubpage of given elemSize, we can
     * completely free the owning Page so it is available for subsequent allocations
     *
     * @param handle handle to free
     */
    void free(long handle, ByteBuffer nioBuffer) {
        // 二叉树节点的id
        int memoryMapIdx = memoryMapIdx(handle);
        // bitmap的索引位置 bitmapIdx != 0 说明释放的是tiny或small的内存
        int bitmapIdx = bitmapIdx(handle);

        if (bitmapIdx != 0) { // free a subpage
            PoolSubpage<T> subpage = subpages[subpageIdx(memoryMapIdx)];
            assert subpage != null && subpage.doNotDestroy;

            // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and synchronize on it.
            // This is need as we may add it back and so alter the linked-list structure.
            // 找到指定小规格的 头节点的
            PoolSubpage<T> head = arena.findSubpagePoolHead(subpage.elemSize);
            synchronized (head) {
                // bitmapIdx & 0x3FFFFFFF 去掉最高位的 标志位
                if (subpage.free(head, bitmapIdx & 0x3FFFFFFF)) {
                    return;
                }
            }
        }
        // 可用内存大小恢复
        freeBytes += runLength(memoryMapIdx);
        // 设置分配能力
        setValue(memoryMapIdx, depth(memoryMapIdx));
        // 更新父节点的分配能力
        updateParentsFree(memoryMapIdx);

        if (nioBuffer != null && cachedNioBuffers != null &&
                cachedNioBuffers.size() < PooledByteBufAllocator.DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK) {
            cachedNioBuffers.offer(nioBuffer);
        }
    }

    void initBuf(PooledByteBuf<T> buf, ByteBuffer nioBuffer, long handle, int reqCapacity) {
        // 获取低32位的handle值 都是表示二叉树节点的id值
        int memoryMapIdx = memoryMapIdx(handle);
        // 获取handler高32位的值   位图索引  allocateRun的高32位是0 allocateSubpage的高32位不是0
        int bitmapIdx = bitmapIdx(handle);
        if (bitmapIdx == 0) { // bitmapIdx等于0说明是normal 规格的
            // 获取可分配能力值
            byte val = value(memoryMapIdx);
            assert val == unusable : String.valueOf(val);
            // 参数1：chunk 当前所属的chunk 创建byteBuf分配的chunk对象，真实的内存是有chunk持有的
            // 参数2：认为是个null吧
            // 参数3：handle 分配内存的位置的 long值，后面1释放内存还要使用handle
            // 参数4：runOffset(memoryMapIdx) + offset ，表示分配给缓冲区的这块内存相对于 chunk 中申请的内存的首地址偏移了多少
            // 参数5：reqCapacity 业务需要的内存大小
            // 参数6：runLength(memoryMapIdx) 获取节点能够分配管理内存的大小
            // 参数7：本地线程缓存
            buf.init(this, nioBuffer, handle, runOffset(memoryMapIdx) + offset,
                    reqCapacity, runLength(memoryMapIdx), arena.parent.threadCache());
        } else { // bitmapIdx != 0,说明是小一点规格的内存
            // 参数1：buf 返回给业务使用的ByteBuf对象，他包装内存
            // 参数2：
            // 参数3：handle 分配内存的位置的 long值，后面1释放内存还要使用handle
            // 参数4：bitmapIdx 高位的第二位是标记位 是 1
            // 参数5：reqCapacity 业务需要的内存大小
            initBufWithSubpage(buf, nioBuffer, handle, bitmapIdx, reqCapacity);
        }
    }

    void initBufWithSubpage(PooledByteBuf<T> buf, ByteBuffer nioBuffer, long handle, int reqCapacity) {
        initBufWithSubpage(buf, nioBuffer, handle, bitmapIdx(handle), reqCapacity);
    }

    private void initBufWithSubpage(PooledByteBuf<T> buf, ByteBuffer nioBuffer,
                                    long handle, int bitmapIdx, int reqCapacity) {
        assert bitmapIdx != 0;

        // 叶子节点ID
        int memoryMapIdx = memoryMapIdx(handle);
        // 获取给buf分配内存的subpage对象
        PoolSubpage<T> subpage = subpages[subpageIdx(memoryMapIdx)];
        assert subpage.doNotDestroy;
        assert reqCapacity <= subpage.elemSize;

        // (bitmapIdx & 0x3FFFFFFF) 是为了去除 64位long值 第二位的1的表示是subpage的标志位

        // runOffset(memoryMapIdx) + (bitmapIdx & 0x3FFFFFFF) * subpage.elemSize + offset
        // 原 Page 的偏移量 + 子块的偏移量
        // bitmapIdx & 0x3FFFFFFF 当前分配的 SubPage 属于第几个 SubPage
        // (bitmapIdx & 0x3FFFFFFF) * subpage.elemSize 表示在当前Page的偏移量

        buf.init(
            this, nioBuffer, handle,
            runOffset(memoryMapIdx) + (bitmapIdx & 0x3FFFFFFF) * subpage.elemSize + offset,
                reqCapacity, subpage.elemSize, arena.parent.threadCache());

    }

    private byte value(int id) {
        return memoryMap[id];
    }

    private void setValue(int id, byte val) {
        memoryMap[id] = val;
    }

    private byte depth(int id) {
        return depthMap[id];
    }

    private static int log2(int val) {
        // compute the (0-based, with lsb = 0) position of highest set bit i.e, log2
        return INTEGER_SIZE_MINUS_ONE - Integer.numberOfLeadingZeros(val);
    }

    private int runLength(int id) {
        // represents the size in #bytes supported by node 'id' in the tree
        // 24 - 11 = 13
        // 1 << 13 = 8k
        return 1 << log2ChunkSize - depth(id);
    }

    private int runOffset(int id) {
        // represents the 0-based offset in #bytes from start of the byte-array chunk
        // eg depth(id)=11
        // 1 << 11 = 1000 0000 0000
        // id 和1000 0000 0000异或
        // id 2048 ->shift= 0
        // id 2049 ->shift= 1
        // id 2050 ->shift= 2
        int shift = id ^ 1 << depth(id);
        // runLength(id)返回 1<<13
        // 2048 -> 偏移量就是 0
        // 2049 -> 偏移量就是 8k
        // 2050 -> 偏移量就是 16k
        return shift * runLength(id);
    }

    /**
     * 取模算法
     *
     * @param memoryMapIdx
     * @return
     */
    private int subpageIdx(int memoryMapIdx) {
        // maxSubpageAllocs 最多可申请多少个 subpage 默认2048
        // 异或 1000 0000 0000
        // 删除最高设置位，以获得偏移量
        return memoryMapIdx ^ maxSubpageAllocs; // remove highest set bit, to get offset
    }

    private static int memoryMapIdx(long handle) {
        return (int) handle;
    }

    /**
     * 右移32位得到位图的索引
     * @param handle
     * @return
     */
    private static int bitmapIdx(long handle) {
        return (int) (handle >>> Integer.SIZE);
    }

    @Override
    public int chunkSize() {
        return chunkSize;
    }

    @Override
    public int freeBytes() {
        synchronized (arena) {
            return freeBytes;
        }
    }

    @Override
    public String toString() {
        final int freeBytes;
        synchronized (arena) {
            freeBytes = this.freeBytes;
        }

        return new StringBuilder()
                .append("Chunk(")
                .append(Integer.toHexString(System.identityHashCode(this)))
                .append(": ")
                .append(usage(freeBytes))
                .append("%, ")
                .append(chunkSize - freeBytes)
                .append('/')
                .append(chunkSize)
                .append(')')
                .toString();
    }

    void destroy() {
        arena.destroyChunk(this);
    }
}
