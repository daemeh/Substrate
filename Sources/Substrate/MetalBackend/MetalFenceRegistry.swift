//
//  MetalFenceRegistry.swift
//  MetalRenderer
//
//  Created by Thomas Roughton on 20/07/19.
//

#if canImport(Metal)

import Metal
import SubstrateUtilities

struct MetalFenceHandle : Equatable {
    public var index : UInt32
    
    init(index: UInt32) {
        self.index = index
    }
    
    init(label: String, queue: Queue, commandBufferIndex: UInt64) {
        self = MetalFenceRegistry.instance.allocate(queue: queue, commandBufferIndex: commandBufferIndex)
        self.fence.label = label
    }
    
    var isValid : Bool {
        return self.index != .max
    }
    
    var fence : MTLFence {
        assert(self.isValid)
        return MetalFenceRegistry.instance.fences[Int(self.index)].takeUnretainedValue()
    }
    
    var commandBufferIndex : UInt64 {
        assert(self.isValid)
        return MetalFenceRegistry.instance.commandBufferIndices[Int(self.index)].1
    }
    
    public static let invalid = MetalFenceHandle(index: .max)
}


final class MetalFenceRegistry {
    public static let instance = MetalFenceRegistry()
    
    public let allocator : ResizingAllocator
    public var activeIndices = [UInt32]()
    public var freeIndices = RingBuffer<UInt32>()
    public var maxIndex : UInt32 = 0
    
    public var device : MTLDevice! = nil
    
    public var fences : UnsafeMutablePointer<Unmanaged<MTLFence>>
    public var commandBufferIndices : UnsafeMutablePointer<(Queue, UInt64)> // On the queue
    
    public init() {
        self.allocator = ResizingAllocator(allocator: .system)
        (self.fences, self.commandBufferIndices) = allocator.reallocate(capacity: 256, initializedCount: 0)
    }
    
    deinit {
        self.fences.deinitialize(count: Int(self.maxIndex))
    }
    
    public func allocate(queue: Queue, commandBufferIndex: UInt64) -> MetalFenceHandle {
        let index : UInt32
        if let reusedIndex = self.freeIndices.popFirst() {
            index = reusedIndex
        } else {
            index = self.maxIndex
            self.ensureCapacity(Int(self.maxIndex + 1))
            self.maxIndex += 1
            
            self.fences.advanced(by: Int(index)).initialize(to: Unmanaged.passRetained(self.device.makeFence()!))
        }

        self.commandBufferIndices[Int(index)] = (queue, commandBufferIndex)
        self.activeIndices.append(index)
        
        return MetalFenceHandle(index: index)
    }
    
    func delete(at index: UInt32) {
        self.freeIndices.append(index)
    }

    func clearCompletedFences() {
        var i = 0
        while i < self.activeIndices.count {
            let index = self.activeIndices[i]
            if self.commandBufferIndices[Int(index)].1 <= self.commandBufferIndices[Int(index)].0.lastCompletedCommand {
                self.delete(at: index)
                self.activeIndices.remove(at: i, preservingOrder: false)
            } else {
                i += 1
            }
        }
    }
    
    @inlinable
    public func ensureCapacity(_ capacity: Int) {
        if self.allocator.capacity < capacity {
            let oldCapacity = self.allocator.capacity
            let newCapacity = max(2 * oldCapacity, capacity)
            (self.fences, self.commandBufferIndices) = allocator.reallocate(capacity: newCapacity, initializedCount: Int(self.maxIndex))
        }
    }
}

#endif // canImport(Metal)
