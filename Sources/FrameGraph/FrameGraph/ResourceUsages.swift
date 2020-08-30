//
//  ResourceUsages.swift
//  FrameGraph
//
//  Created by Joseph Bennett on 20/12/17.
//

import FrameGraphUtilities
import FrameGraphCExtras

// This 'PerformOrder' type is necessary to make sure the command goes to the right Command Encoder
public enum PerformOrder : Comparable {
    case before // Before the command index
    case after // After the command index
    
    @inlinable
    public static func <(lhs: PerformOrder, rhs: PerformOrder) -> Bool {
        return lhs == .before && rhs == .after
    }
}

@usableFromInline typealias ResourceUsagePointer = UnsafeMutablePointer<ResourceUsage>

extension ChunkArray where Element == ResourceUsage {
    @inlinable
    var firstActiveUsage : ResourceUsage? {
        for usage in self {
            if usage.renderPassRecord.isActive, usage.type != .unusedRenderTarget, usage.type != .unusedArgumentBuffer {
                return usage
            }
        }
        return nil
    }
    
    @inlinable
    mutating func mergeOrAppendUsage(_ usage: ResourceUsage, allocator: TagAllocator.ThreadView) {
        if self.isEmpty || !self.last.mergeWithUsage(usage) {
            self.append(usage, allocator: .tagThreadView(allocator))
        }
    }
}

extension ResourceUsageType {
    @inlinable
    public var isRead : Bool {
        switch self {
        case .read, .readWrite, .blitSource, .blitSynchronisation, .mipGeneration,
             .vertexBuffer, .indexBuffer, .indirectBuffer, .readWriteRenderTarget,
             .inputAttachment, .inputAttachmentRenderTarget, .constantBuffer:
            return true
        default:
            return false
        }
    }
    
    @inlinable
    public var isWrite : Bool {
        switch self {
        case .write, .readWrite, .readWriteRenderTarget, .writeOnlyRenderTarget, .inputAttachmentRenderTarget, .blitDestination, .blitSynchronisation, .mipGeneration:
            return true
        default:
            return false
        }
    }
    
    @inlinable
    public var isUAVReadWrite : Bool {
        switch self {
        case .readWrite:
            return true
        default:
            return false
        }
    }
    
    @inlinable
    static func areMergeable(_ typeA: ResourceUsageType, _ typeB: ResourceUsageType) -> Bool {
        // We can only merge resources of the same type, and we can only merge writes if they're contained within a render target.
        return typeA == typeB &&
            (!typeA.isWrite || typeA.isRenderTarget)
    }
}

// Note: must be a value type.
public struct ResourceUsage {
    public var type : ResourceUsageType
    public var stages : RenderStages
    public var inArgumentBuffer : Bool
    @usableFromInline
    unowned(unsafe) var renderPassRecord : RenderPassRecord
    public var commandRange : Range<Int> // References the range in the pass before and during FrameGraph compilation, and the range in the full commands array aftre.
    public var activeRange: ActiveResourceRange = .fullResource
    
    @inlinable
    init(type: ResourceUsageType, stages: RenderStages, activeRange: ActiveResourceRange, inArgumentBuffer: Bool, firstCommandOffset: Int, renderPass: RenderPassRecord) {
        self.type = type
        self.stages = stages
        self.activeRange = activeRange
        self.renderPassRecord = renderPass
        self.commandRange = Range(firstCommandOffset...firstCommandOffset)
        self.inArgumentBuffer = inArgumentBuffer
    }
    
    @inlinable
    public var isRead : Bool {
        return self.type.isRead
    }
    
    @inlinable
    public var isWrite : Bool {
        return self.type.isWrite
    }
    
    @inlinable
    public var affectsGPUBarriers : Bool {
        return self.renderPassRecord.isActive && self.stages != .cpuBeforeRender && self.type != .unusedRenderTarget && self.renderPassRecord.pass.passType != .external
    }
    
    /// - returns: Whether the usages could be merged.
    @inlinable
    mutating func mergeWithUsage(_ nextUsage: ResourceUsage) -> Bool {
        if self.renderPassRecord !== nextUsage.renderPassRecord {
            return false
        }
        
        if type == .inputAttachment && self.type.isRenderTarget { // Transform a resource read within a render target into a readWriteRenderTarget.
            self.type = .inputAttachmentRenderTarget
            self.stages.formUnion(stages)
            self.inArgumentBuffer = self.inArgumentBuffer || inArgumentBuffer
            self.commandRange = Range(uncheckedBounds: (min(self.commandRange.lowerBound, nextUsage.commandRange.lowerBound), max(self.commandRange.upperBound, nextUsage.commandRange.upperBound)))
            return true
        }
        
        readWriteMergeCheck: if self.commandRange.contains(nextUsage.commandRange.lowerBound), stages == self.stages, self.type != type {
            assert(self.inArgumentBuffer == inArgumentBuffer)
            
            switch (type, self.type) {
            case (.read, .readWrite), (.write, .write), (.write, .readWrite):
                break
            case (.read, .write), (.readWrite, .read), (.write, .read):
                self.type = .readWrite
            case (.writeOnlyRenderTarget, .readWriteRenderTarget), (.readWriteRenderTarget, .writeOnlyRenderTarget):
                self.type = .readWriteRenderTarget
            case (_, _) where !type.isWrite && !self.type.isWrite:
                // If neither are writes, then it's fine to have conflicting uses.
                // This might occur e.g. when reading from a buffer while simultaneously using it as an indirect buffer.
                break readWriteMergeCheck
            default:
                assertionFailure("Resource simulaneously bound for conflicting uses.")
            }
            
            return true
        }
        
        if ResourceUsageType.areMergeable(self.type, type) &&
            self.inArgumentBuffer == inArgumentBuffer {
            self.stages.formUnion(stages)
            self.commandRange = Range(uncheckedBounds: (min(self.commandRange.lowerBound, nextUsage.commandRange.lowerBound), max(self.commandRange.upperBound, nextUsage.commandRange.upperBound)))
            return true
        }
        return false
        
    }
}


extension ResourceUsage : CustomStringConvertible {
    public var description: String {
        return "ResourceUsage(type: \(self.type), stages: \(self.stages), inArgumentBuffer: \(self.inArgumentBuffer), pass: \(self.renderPassRecord.pass.name), commandRange: \(self.commandRange))"
    }
}

fileprivate extension Array {
    var mutableLast : Element {
        get {
            return self[self.count - 1]
        }
        set {
            self[self.count - 1] = newValue
        }
    }
}

extension Unmanaged : Hashable, Equatable where Instance : Hashable {
    public func hash(into hasher: inout Hasher) {
        self.takeUnretainedValue().hash(into: &hasher)
    }
    
    public static func ==(lhs: Unmanaged, rhs: Unmanaged) -> Bool {
        return lhs.takeUnretainedValue() == rhs.takeUnretainedValue()
    }
}

extension UInt64 : CustomHashable {
    public var customHashValue : Int {
        return Int(truncatingIfNeeded: self)
    }
}
